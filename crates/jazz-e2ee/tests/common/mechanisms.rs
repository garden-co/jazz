//! Custom mechanisms for protocol tests: the built-in HPKE key sealing and
//! signatures under other IDs, as a mechanism migration's new mechanism
//! would be, and a sealer, a key wrapper and a signer that can be switched
//! to fail as a host-provided mechanism might.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use jazz_e2ee::agility::{
    KeyPair, KeySealer, KeyWrapper, MechanismConfig, MechanismImpl, PublicKey, Signer,
};
use jazz_e2ee::envelope::{self, Mechanism};
use jazz_e2ee::mechanisms::{key_envelope, key_seal, signature};
use jazz_e2ee::registry::Registry;
use jazz_e2ee::{Error, RandomSource, Result, Zeroizing};

/// Re-labels a built-in envelope's payload under `to`.
fn relabel(from: Mechanism<'_>, to: Mechanism<'_>, bytes: &[u8]) -> Result<Vec<u8>> {
    envelope::encode(to, envelope::decode(from, bytes)?)
}

/// The built-in HPKE key sealing under another ID, as a mechanism
/// migration's new mechanism would be. With `fails` set, opening is a
/// provider failure.
pub struct RelabelledSealer {
    pub mechanism: Mechanism<'static>,
    pub fails: Arc<AtomicBool>,
}

pub const OTHER_SEAL: Mechanism<'static> = Mechanism::new("com.example.other-seal", 1);
pub const FLAKY_SEAL: Mechanism<'static> = Mechanism::new("com.example.flaky-seal", 1);
pub const OTHER_SIGN: Mechanism<'static> = Mechanism::new("com.example.other-sign", 1);
pub const FLAKY_WRAP: Mechanism<'static> = Mechanism::new("com.example.flaky-wrap", 1);
pub const FLAKY_SIGN: Mechanism<'static> = Mechanism::new("com.example.flaky-sign", 1);
/// A mechanism no registry here has: envelopes relabelled to it are not
/// routed.
pub const UNROUTED_SEAL: Mechanism<'static> = Mechanism::new("com.example.unrouted-seal", 1);
pub const UNROUTED_WRAP: Mechanism<'static> = Mechanism::new("com.example.unrouted-wrap", 1);

/// Re-labels an envelope of the built-in key sealing or key wrapping under
/// `to`, as an envelope of a mechanism the reader may not have.
pub fn relabelled(from: Mechanism<'_>, to: Mechanism<'_>, bytes: &[u8]) -> Vec<u8> {
    relabel(from, to, bytes).expect("a well-formed envelope")
}

impl RelabelledSealer {
    pub fn new(mechanism: Mechanism<'static>) -> (Arc<Self>, Arc<AtomicBool>) {
        let fails = Arc::new(AtomicBool::new(false));
        (
            Arc::new(Self {
                mechanism,
                fails: fails.clone(),
            }),
            fails,
        )
    }

    fn builtin(public: &PublicKey) -> PublicKey {
        PublicKey {
            mechanism: key_seal::MECHANISM.into(),
            bytes: public.bytes.clone(),
        }
    }
}

impl MechanismImpl for RelabelledSealer {
    fn mechanism(&self) -> Mechanism<'static> {
        self.mechanism
    }
}

impl KeySealer for RelabelledSealer {
    fn derive_key_pair(&self, ikm: &[u8]) -> Result<KeyPair> {
        let mut pair = key_seal::Builtin.derive_key_pair(ikm)?;
        pair.public.mechanism = self.mechanism.into();
        Ok(pair)
    }

    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        if recipient.mechanism != self.mechanism {
            return Err(Error::RecipientUnsupported);
        }
        let sealed = key_seal::Builtin.seal(&Self::builtin(recipient), context, key, rng)?;
        relabel(key_seal::MECHANISM, self.mechanism, &sealed)
    }

    fn open(
        &self,
        recipient: &KeyPair,
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        if self.fails.load(Ordering::SeqCst) {
            return Err(Error::CryptoFailure);
        }
        let builtin = KeyPair {
            public: Self::builtin(&recipient.public),
            secret: recipient.secret.clone(),
        };
        key_seal::Builtin.open(
            &builtin,
            context,
            &relabel(self.mechanism, key_seal::MECHANISM, envelope)?,
        )
    }
}

/// The built-in signature mechanism under another ID.
pub struct RelabelledSigner;

impl MechanismImpl for RelabelledSigner {
    fn mechanism(&self) -> Mechanism<'static> {
        OTHER_SIGN
    }
}

fn builtin_signing(public: &PublicKey) -> PublicKey {
    PublicKey {
        mechanism: signature::MECHANISM.into(),
        bytes: public.bytes.clone(),
    }
}

impl Signer for RelabelledSigner {
    fn derive_key_pair(&self, seed: &[u8]) -> Result<KeyPair> {
        let mut pair = signature::Builtin.derive_key_pair(seed)?;
        pair.public.mechanism = OTHER_SIGN.into();
        Ok(pair)
    }

    fn sign(&self, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>> {
        let builtin = KeyPair {
            public: builtin_signing(&key.public),
            secret: key.secret.clone(),
        };
        relabel(
            signature::MECHANISM,
            OTHER_SIGN,
            &signature::Builtin.sign(&builtin, record)?,
        )
    }

    fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        let Ok(builtin) = relabel(OTHER_SIGN, signature::MECHANISM, signature) else {
            return Ok(false);
        };
        signature::Builtin.verify(&builtin_signing(public), record, &builtin)
    }
}

/// The built-in key wrapping under [`FLAKY_WRAP`]. With `fails` set,
/// unwrapping is a provider failure.
pub struct FlakyWrapper {
    pub fails: Arc<AtomicBool>,
}

impl MechanismImpl for FlakyWrapper {
    fn mechanism(&self) -> Mechanism<'static> {
        FLAKY_WRAP
    }
}

impl KeyWrapper for FlakyWrapper {
    fn wrap(
        &self,
        wrapping_key: &[u8; 32],
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> Result<Vec<u8>> {
        let wrapped = key_envelope::Builtin.wrap(wrapping_key, context, key, rng)?;
        relabel(key_envelope::MECHANISM, FLAKY_WRAP, &wrapped)
    }

    fn unwrap(
        &self,
        wrapping_key: &[u8; 32],
        context: &[u8],
        envelope: &[u8],
    ) -> Result<Zeroizing<[u8; 32]>> {
        if self.fails.load(Ordering::SeqCst) {
            return Err(Error::CryptoFailure);
        }
        key_envelope::Builtin.unwrap(
            wrapping_key,
            context,
            &relabel(FLAKY_WRAP, key_envelope::MECHANISM, envelope)?,
        )
    }
}

/// The built-in signatures under [`FLAKY_SIGN`]. With `fails` set,
/// verifying a record containing the selected marker is a provider failure (all records for None).
pub struct FlakySigner {
    pub fails: Arc<AtomicBool>,
    pub record_marker: Option<&'static [u8]>,
}

impl MechanismImpl for FlakySigner {
    fn mechanism(&self) -> Mechanism<'static> {
        FLAKY_SIGN
    }
}

fn flaky_signing(public: &PublicKey) -> PublicKey {
    PublicKey {
        mechanism: FLAKY_SIGN.into(),
        bytes: public.bytes.clone(),
    }
}

impl Signer for FlakySigner {
    fn derive_key_pair(&self, seed: &[u8]) -> Result<KeyPair> {
        let mut pair = signature::Builtin.derive_key_pair(seed)?;
        pair.public = flaky_signing(&pair.public);
        Ok(pair)
    }

    fn sign(&self, key: &KeyPair, record: &[u8]) -> Result<Vec<u8>> {
        let builtin = KeyPair {
            public: builtin_signing(&key.public),
            secret: key.secret.clone(),
        };
        relabel(
            signature::MECHANISM,
            FLAKY_SIGN,
            &signature::Builtin.sign(&builtin, record)?,
        )
    }

    fn verify(&self, public: &PublicKey, record: &[u8], signature: &[u8]) -> Result<bool> {
        if self.fails.load(Ordering::SeqCst)
            && self
                .record_marker
                .is_none_or(|marker| record.windows(marker.len()).any(|window| window == marker))
        {
            return Err(Error::CryptoFailure);
        }
        let Ok(builtin) = relabel(FLAKY_SIGN, signature::MECHANISM, signature) else {
            return Ok(false);
        };
        signature::Builtin.verify(&builtin_signing(public), record, &builtin)
    }
}

/// A registry with the built-ins, the flaky key wrapper and the flaky
/// signer, and the flags that make each fail.
pub fn flaky_registry() -> (Registry, Arc<AtomicBool>, Arc<AtomicBool>) {
    flaky_registry_with_scope(Some(b"jazz.e2ee.space.v1"))
}

pub fn flaky_registry_with_all_record_signer() -> (Registry, Arc<AtomicBool>, Arc<AtomicBool>) {
    flaky_registry_with_scope(None)
}

fn flaky_registry_with_scope(
    record_marker: Option<&'static [u8]>,
) -> (Registry, Arc<AtomicBool>, Arc<AtomicBool>) {
    let mut registry = Registry::builtin();
    let wrap_fails = Arc::new(AtomicBool::new(false));
    let sign_fails = Arc::new(AtomicBool::new(false));
    registry
        .register_key_wrapper(Arc::new(FlakyWrapper {
            fails: wrap_fails.clone(),
        }))
        .unwrap();
    registry
        .register_signer(Arc::new(FlakySigner {
            fails: sign_fails.clone(),
            record_marker,
        }))
        .unwrap();
    (registry, wrap_fails, sign_fails)
}

/// A registry with the built-ins, a relabelled sealer of `sealer`'s ID and
/// the relabelled signer, and the flag that makes the sealer fail.
pub fn custom_registry(sealer: Mechanism<'static>) -> (Registry, Arc<AtomicBool>) {
    let mut registry = Registry::builtin();
    let (relabelled, fails) = RelabelledSealer::new(sealer);
    registry.register_key_sealer(relabelled).unwrap();
    registry
        .register_signer(Arc::new(RelabelledSigner))
        .unwrap();
    (registry, fails)
}

/// A configuration that seals with `mechanism`.
pub fn sealing_with(mechanism: Mechanism<'static>) -> MechanismConfig {
    MechanismConfig {
        key_seal: mechanism.into(),
        ..MechanismConfig::default()
    }
}
