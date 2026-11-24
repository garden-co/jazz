//! Held-key documents of the private store (`E2EE_PROTOCOL.md` §6.1,
//! §6.2): sealing a held space into its document, and the checks a
//! document passes on load.
//!
//! A key is sealed to the device: `envelope = seal(device.pub, B(format,
//! body), K)`, then `signature = sign(device.signingKey, B(format, body +
//! envelope))`; the lineage record is signed over `B(format, record without
//! signature)`. Fresh output is opened or verified again, and a failure to
//! read it back is a provider failure.
//!
//! On load, a record that fails a check is skipped and reported, never
//! fatal (§4.2); a lineage record that fails skips the whole document, and
//! the space is acquired again. A provider failure fails the load. A
//! `refused` key is kept for good (§3.2, §6.4): a rewrite of the document
//! keeps every held key and carries over unchanged every key record the
//! load skipped as unsupported (one only a newer client can open may be
//! such a key), and a later record of the same epoch never lifts a
//! refusal. A rewrite drops a record that failed any other check.

use std::collections::{BTreeMap, BTreeSet};

use zeroize::Zeroizing;

use crate::error::Error;
use crate::random::RandomSource;
use crate::registry::Registry;

use super::super::account::build::LocalDevice;
use super::super::domain::{AccountDomain, AppDomain};
use super::super::error::{ProtocolError, ProtocolResult};
use super::super::space::keys::{Outcome, marker};
use super::super::{key_crypto, signing};
use super::held::{HeldEntry, HeldSpace, KeyMark};
pub use super::stored::{
    Binding, DecodedDocument, HeldDocument, HeldKeyRecord, LineageRecord, UnrecognisedDocument,
    decode_document, device_hash, prefix_hash,
};

/// Why a stored record was skipped on load (§6.2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Rejection {
    /// The binding is not this device's: another application domain,
    /// account, device or device keys.
    Binding,
    /// The device's signature does not verify.
    Signature,
    /// The record does not decode, or does not satisfy its own format: a
    /// root or successor row that does not validate, both marks, or an
    /// empty transaction.
    Malformed,
    /// `successors` is not a chain from the root's epoch.
    NotAChain,
    /// `rootId` is not the lineage's root.
    RootMismatch,
    /// `depth` exceeds the lineage.
    Depth,
    /// `prefix` is not the hash of the lineage prefix of that depth.
    Prefix,
    /// `epochId` is not the epoch at that depth.
    Epoch,
    /// The envelope does not open for this device.
    Envelope,
    /// The opened key fails the marker of its epoch.
    Marker,
    /// A signature, envelope or marker of a mechanism this reader does not
    /// route: unsupported, never invalid.
    Unsupported,
}

/// Which record of a document was skipped.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RejectedRecord {
    /// The lineage record: the whole document is skipped.
    Lineage,
    /// The held-key record at this position in the document, with its
    /// epoch when it decoded.
    Key {
        index: usize,
        epoch_id: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Rejected {
    pub record: RejectedRecord,
    pub reason: Rejection,
}

/// The checked lineage and keys of a loaded document, and its skipped records.
#[derive(Debug)]
pub struct Loaded {
    pub revision: u64,
    /// The checked lineage, even when no key loads. A rewrite uses it to
    /// preserve the depths of carried records. Only a space with keys is
    /// held for reads.
    pub space: Option<HeldSpace>,
    pub rejected: Vec<Rejected>,
    /// The key records skipped as [`Rejection::Unsupported`], as stored:
    /// the next rewrite of this document carries them over unchanged
    /// ([`StoreOwner::seal_document`]). Empty when the lineage was skipped.
    pub unsupported: Vec<HeldKeyRecord>,
}

/// The device whose store this is: its application, account and keys.
#[derive(Clone, Copy)]
pub struct StoreOwner<'a> {
    pub registry: &'a Registry,
    pub domain: &'a AppDomain,
    /// The device's account: the store's scope is `Dₐ(account)`.
    pub account_id: &'a str,
    pub device: &'a LocalDevice,
}

/// A check's outcome that is not a provider failure.
pub(super) type Checked<T> = Result<T, Rejection>;

impl StoreOwner<'_> {
    fn scope(&self) -> ProtocolResult<AccountDomain> {
        self.domain.account(self.account_id)
    }

    /// This device's binding: `{ application: Dₐ(self), device: deviceId,
    /// keys: <device hash> }`.
    pub fn binding(&self) -> ProtocolResult<Binding> {
        Ok(Binding {
            application: self.scope()?.as_str().to_owned(),
            device: self.device.id.clone(),
            keys: device_hash(&self.device.sealing.public, &self.device.signing.public)?,
        })
    }

    /// Signs with the device's signing key and verifies the signature
    /// again; a failure to read it back is a provider failure.
    pub(super) fn sign(&self, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
        key_crypto::sign(self.registry, &self.device.signing, bytes)
    }

    pub(super) fn verify(&self, bytes: &[u8], signature: &[u8]) -> ProtocolResult<Checked<()>> {
        match signing::verify(self.registry, &self.device.signing.public, bytes, signature) {
            Ok(true) => Ok(Ok(())),
            Ok(false) => Ok(Err(Rejection::Signature)),
            Err(ProtocolError::Unsupported(_))
            | Err(ProtocolError::Crypto(Error::UnsupportedFormat)) => {
                Ok(Err(Rejection::Unsupported))
            }
            Err(ProtocolError::ProviderFailure) => Err(ProtocolError::ProviderFailure),
            Err(_) => Ok(Err(Rejection::Signature)),
        }
    }

    /// The lineage record of `space`, signed by the device.
    pub fn seal_lineage(&self, space: &HeldSpace) -> ProtocolResult<LineageRecord> {
        let mut record = LineageRecord {
            binding: self.binding()?,
            root: space.root.clone(),
            successors: space.successors.clone(),
            signature: Vec::new(),
        };
        record.signature = self.sign(&record.signed_bytes()?)?;
        Ok(record)
    }

    /// The held-key record of the key at `depth` of `space`, sealed with
    /// the device's own sealing mechanism and opened again, then signed.
    pub fn seal_key(
        &self,
        space: &HeldSpace,
        depth: usize,
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<HeldKeyRecord> {
        let entry = space
            .keys
            .get(&depth)
            .ok_or(ProtocolError::InvalidRecord("no key held at that depth"))?;
        let (unpublished, refused) = match &entry.mark {
            KeyMark::Published => (None, false),
            KeyMark::Unpublished { transaction } => (Some(transaction.clone()), false),
            KeyMark::Refused => (None, true),
        };
        let mut record = HeldKeyRecord {
            binding: self.binding()?,
            root_id: space.root.id.clone(),
            epoch_id: space
                .epoch_at(depth)
                .expect("a held depth is in the lineage")
                .to_owned(),
            depth: depth as u64,
            prefix: prefix_hash(&space.root, &space.successors[..depth])?,
            unpublished,
            refused,
            envelope: Vec::new(),
            signature: Vec::new(),
        };
        let body = record.body_bytes()?;
        let sealing = &self.device.sealing;
        let sealer = self
            .registry
            .key_sealer(sealing.public.mechanism.as_mechanism())?;
        let envelope = sealer.seal(&sealing.public, &body, &entry.key, rng)?;
        key_crypto::check_sealed(self.registry, sealing, &body, &envelope, &entry.key)?;
        record.envelope = envelope;
        record.signature = self.sign(&record.signed_bytes()?)?;
        Ok(record)
    }

    /// The whole document of `space`: its lineage, the records of
    /// `carried`, unchanged, and one record per held key. Storing a key
    /// rewrites only its root's document (§6.1). `carried` are the records
    /// the last load of this document skipped as unsupported
    /// ([`Loaded::unsupported`]), so that a key this device cannot open is
    /// not lost; it is empty only for a new document: passing `&[]` when
    /// rewriting an existing document drops them. A carried record of
    /// another root, or one deeper than `space`'s lineage reaches, is an
    /// invalid input: extend the lineage first, or the next load would
    /// drop the record.
    pub fn seal_document(
        &self,
        space: &HeldSpace,
        carried: &[HeldKeyRecord],
        revision: u64,
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<HeldDocument> {
        if carried.iter().any(|record| record.root_id != space.root.id) {
            return Err(ProtocolError::InvalidRecord(
                "a carried record of another root",
            ));
        }
        // A carried record must stay loadable: the lineage written now has
        // to reach its depth, or the next load would drop it (§6.2).
        let reach = space.successors.len() as u64;
        if carried.iter().any(|record| record.depth > reach) {
            return Err(ProtocolError::InvalidRecord(
                "a carried record deeper than the lineage",
            ));
        }
        let lineage = self.seal_lineage(space)?;
        // Carried records first: a reader that opens both lets this
        // device's own record of an epoch replace one carried, unless the
        // carried one is `refused`.
        let keys = carried
            .iter()
            .cloned()
            .map(Ok)
            .chain(
                space
                    .keys
                    .keys()
                    .map(|depth| self.seal_key(space, *depth, rng)),
            )
            .collect::<ProtocolResult<_>>()?;
        Ok(HeldDocument {
            revision,
            lineage,
            keys,
        })
    }

    pub(super) fn check_binding(&self, binding: &Binding) -> ProtocolResult<Checked<()>> {
        Ok(if *binding == self.binding()? {
            Ok(())
        } else {
            Err(Rejection::Binding)
        })
    }

    /// The lineage checks of §6.2: binding, signature, the root decodes and
    /// is a root of its address, and `successors` is a chain from the
    /// root's epoch with no epoch repeated.
    fn check_lineage(&self, record: &LineageRecord) -> ProtocolResult<Checked<()>> {
        if let Err(reason) = self.check_binding(&record.binding)? {
            return Ok(Err(reason));
        }
        if let Err(reason) = self.verify(&record.signed_bytes()?, &record.signature)? {
            return Ok(Err(reason));
        }
        let Ok(scope) = record.root.validate().and_then(|()| record.root.scope()) else {
            return Ok(Err(Rejection::Malformed));
        };
        let mut previous = record.root.epoch_id.as_str();
        let mut used = BTreeSet::from([previous]);
        for successor in &record.successors {
            if successor.validate(&scope).is_err() {
                return Ok(Err(Rejection::Malformed));
            }
            if successor.predecessor != previous || !used.insert(successor.epoch_id.as_str()) {
                return Ok(Err(Rejection::NotAChain));
            }
            previous = &successor.epoch_id;
        }
        Ok(Ok(()))
    }

    /// The held-key record checks of §6.2, against an accepted lineage;
    /// returns the depth, the opened key and its mark.
    fn check_key(
        &self,
        space: &HeldSpace,
        record: &HeldKeyRecord,
    ) -> ProtocolResult<Checked<(usize, HeldEntry)>> {
        if let Err(reason) = self.check_binding(&record.binding)? {
            return Ok(Err(reason));
        }
        if let Err(reason) = self.verify(&record.signed_bytes()?, &record.signature)? {
            return Ok(Err(reason));
        }
        let mark = match (&record.unpublished, record.refused) {
            (None, false) => KeyMark::Published,
            (Some(transaction), false) if !transaction.is_empty() => KeyMark::Unpublished {
                transaction: transaction.clone(),
            },
            (None, true) => KeyMark::Refused,
            _ => return Ok(Err(Rejection::Malformed)),
        };
        if record.root_id != space.root.id {
            return Ok(Err(Rejection::RootMismatch));
        }
        let depth = match usize::try_from(record.depth) {
            Ok(depth) if depth <= space.successors.len() => depth,
            _ => return Ok(Err(Rejection::Depth)),
        };
        if prefix_hash(&space.root, &space.successors[..depth])? != record.prefix {
            return Ok(Err(Rejection::Prefix));
        }
        if space.epoch_at(depth) != Some(record.epoch_id.as_str()) {
            return Ok(Err(Rejection::Epoch));
        }
        let key = match self.registry.open_sealed(
            core::slice::from_ref(&self.device.sealing),
            &record.body_bytes()?,
            &record.envelope,
        ) {
            Ok(key) => key,
            Err(Error::UnsupportedFormat) => return Ok(Err(Rejection::Unsupported)),
            Err(Error::CryptoFailure) => return Err(ProtocolError::ProviderFailure),
            Err(_) => return Ok(Err(Rejection::Envelope)),
        };
        let successor = depth.checked_sub(1).map(|at| &space.successors[at]);
        match marker(
            self.registry,
            &key,
            &space.application,
            &space.scope,
            &space.root,
            successor,
        )? {
            Outcome::Ok(()) => {}
            Outcome::Invalid => return Ok(Err(Rejection::Marker)),
            Outcome::Unsupported => return Ok(Err(Rejection::Unsupported)),
        }
        Ok(Ok((
            depth,
            HeldEntry {
                key: Zeroizing::new(*key),
                mark,
            },
        )))
    }

    /// Loads one decoded document with the checks of §6.2. Records that
    /// fail are skipped and reported; a lineage that fails skips the
    /// document. Two records of one epoch: the later replaces the earlier,
    /// except that a `refused` mark is final. A provider failure fails the
    /// load.
    pub fn load(&self, document: &DecodedDocument) -> ProtocolResult<Loaded> {
        let lineage = match &document.lineage {
            Some(lineage) => match self.check_lineage(lineage)? {
                Ok(()) => lineage,
                Err(reason) => return Ok(Self::skipped(document, reason)),
            },
            None => return Ok(Self::skipped(document, Rejection::Malformed)),
        };
        let root = &lineage.root;
        let mut space = HeldSpace {
            application: self.domain.account(&root.creator_account_id)?,
            scope: root.scope()?,
            root: root.clone(),
            successors: lineage.successors.clone(),
            keys: BTreeMap::new(),
        };
        let mut rejected = Vec::new();
        let mut unsupported = Vec::new();
        for (index, record) in document.keys.iter().enumerate() {
            let outcome = match record {
                Some(record) => self.check_key(&space, record)?,
                None => Err(Rejection::Malformed),
            };
            match outcome {
                Ok((depth, entry)) => space.keep_entry(depth, entry),
                Err(reason) => {
                    if let (Rejection::Unsupported, Some(record)) = (reason, record) {
                        unsupported.push(record.clone());
                    }
                    rejected.push(Rejected {
                        record: RejectedRecord::Key {
                            index,
                            epoch_id: record.as_ref().map(|record| record.epoch_id.clone()),
                        },
                        reason,
                    });
                }
            }
        }
        Ok(Loaded {
            revision: document.revision,
            space: Some(space),
            rejected,
            unsupported,
        })
    }

    fn skipped(document: &DecodedDocument, reason: Rejection) -> Loaded {
        Loaded {
            revision: document.revision,
            space: None,
            rejected: vec![Rejected {
                record: RejectedRecord::Lineage,
                reason,
            }],
            unsupported: Vec::new(),
        }
    }
}
