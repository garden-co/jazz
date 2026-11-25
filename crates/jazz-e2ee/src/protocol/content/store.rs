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

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::agility::PublicKey;
use crate::error::Error;
use crate::mechanisms::signature;
use crate::primitives::{hash, sign};
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
use super::stored::{decode_stored_document, encode_verified_document, prefix_hashes};

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
    /// The same document with an advanced verification depth, if any.
    /// The store must save it only against the revision that was loaded.
    pub updated_document: Option<Vec<u8>>,
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
        verify_stored_signature(self.registry, &self.device.signing.public, bytes, signature)
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
        let prefix = prefix_hash(&space.root, &space.successors[..depth])?;
        self.seal_key_with_prefix(space, depth, entry, prefix, rng)
    }

    fn seal_key_with_prefix(
        &self,
        space: &HeldSpace,
        depth: usize,
        entry: &HeldEntry,
        prefix: String,
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<HeldKeyRecord> {
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
            prefix,
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
        let prefixes = prefix_hashes(&space.root, &space.successors, space.keys.keys().copied())?;
        let mut keys = Vec::with_capacity(carried.len() + space.keys.len());
        keys.extend_from_slice(carried);
        for ((&depth, entry), prefix) in space.keys.iter().zip(prefixes) {
            keys.push(self.seal_key_with_prefix(space, depth, entry, prefix, rng)?);
        }
        Ok(HeldDocument {
            revision,
            lineage,
            keys,
        })
    }

    /// Encodes the held space for the private store, authenticating the
    /// records, removal counter and previously checked depth together.
    pub fn seal_stored_document(
        &self,
        space: &HeldSpace,
        carried: &[HeldKeyRecord],
        revision: u64,
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<Vec<u8>> {
        let bytes = self
            .seal_document(space, carried, revision, rng)?
            .encode()?;
        let verified = space
            .verified_through
            .filter(|&verified| carried.iter().all(|record| record.depth > verified as u64));
        match (verified, self.document_key()) {
            (Some(depth), Some(key)) => {
                Self::authenticate_document(&bytes[..bytes.len() - 1], depth, &key)
            }
            _ => Ok(bytes),
        }
    }

    fn document_key(&self) -> Option<Zeroizing<[u8; 32]>> {
        // Record authentication already depends on the signing key.
        // The built-in signer stores its seed; custom signers may hold
        // an opaque handle instead of key material.
        let signing = &self.device.signing;
        if signing.public.mechanism != signature::MECHANISM {
            return None;
        }
        let secret: &[u8; sign::SECRET_KEY_BYTES] = signing.secret.as_bytes().try_into().ok()?;
        let seed = secret.first_chunk::<{ sign::SEED_BYTES }>()?;
        Some(hash::keyed_hash(
            seed,
            b"jazz.e2ee.held-document-authentication.v1\0",
        ))
    }

    fn authenticate_document(body: &[u8], depth: usize, key: &[u8; 32]) -> ProtocolResult<Vec<u8>> {
        let depth = depth as u64;
        let tag = hash::keyed_hash_parts(key, &[&depth.to_be_bytes(), body]);
        encode_verified_document(body, depth, &tag)
    }

    /// Removes the verification hint without changing any of the records.
    pub fn clear_document_verification(bytes: &[u8]) -> Option<Vec<u8>> {
        let (_, body, _) = decode_stored_document(bytes).ok()?;
        if body.len() + 1 == bytes.len() {
            return None;
        }
        let mut cleared = body.to_vec();
        cleared.push(b'}');
        Some(cleared)
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
        prefixes: &[String],
        record: &HeldKeyRecord,
        verified_through: Option<usize>,
    ) -> ProtocolResult<Checked<(usize, HeldEntry)>> {
        let depth = usize::try_from(record.depth).unwrap_or(usize::MAX);
        let verified = verified_through.is_some_and(|verified| depth <= verified);
        if !verified {
            #[cfg(test)]
            KEY_RECORDS_VERIFIED.set(KEY_RECORDS_VERIFIED.get() + 1);
            if let Err(reason) = self.check_binding(&record.binding)? {
                return Ok(Err(reason));
            }
            if let Err(reason) = self.verify(&record.signed_bytes()?, &record.signature)? {
                return Ok(Err(reason));
            }
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
        if depth > space.successors.len() {
            return Ok(Err(Rejection::Depth));
        }
        if !verified && prefixes[depth] != record.prefix {
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
        if verified {
            let verification = successor.map_or(&space.root.verification, |row| &row.verification);
            if self.registry.key_wrapper_for(verification).is_err() {
                return Ok(Err(Rejection::Unsupported));
            }
        } else {
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
        self.load_with_verification(document, None)
    }

    /// Reopens the stored bytes. A missing or invalid authenticator uses
    /// the same per-record checks as an unmarked document.
    pub fn load_stored_document(&self, bytes: &[u8]) -> ProtocolResult<Option<Loaded>> {
        let Ok((document, body, verification)) = decode_stored_document(bytes) else {
            return Ok(None);
        };
        let key = self.document_key();
        let verified = verification
            .zip(key.as_ref())
            .and_then(|((depth, tag), key)| {
                let expected = hash::keyed_hash_parts(key, &[&depth.to_be_bytes(), body]);
                bool::from(expected.ct_eq(&tag)).then_some(usize::try_from(depth).ok()?)
            })
            .filter(|&depth| {
                document
                    .lineage
                    .as_ref()
                    .is_some_and(|lineage| depth <= lineage.successors.len())
            });
        let mut loaded = self.load_with_verification(&document, verified)?;
        if let (Some(space), Some(key)) = (&mut loaded.space, key)
            && loaded.rejected.is_empty()
        {
            space.verified_through = space
                .held()
                .enumerate()
                .take_while(|(expected, (depth, mark))| {
                    *expected == *depth && **mark == KeyMark::Published
                })
                .last()
                .map(|(_, (depth, _))| depth);
            if let Some(depth) = space
                .verified_through
                .filter(|&depth| Some(depth) > verified)
            {
                loaded.updated_document = Some(Self::authenticate_document(body, depth, &key)?);
            }
        }
        Ok(Some(loaded))
    }

    fn load_with_verification(
        &self,
        document: &DecodedDocument,
        verified_through: Option<usize>,
    ) -> ProtocolResult<Loaded> {
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
            verified_through: None,
        };
        // Hash the lineage once, then check keys in document order so
        // duplicate and refused records keep their existing precedence.
        let prefix_count = document
            .keys
            .iter()
            .flatten()
            .filter_map(|record| usize::try_from(record.depth).ok())
            .filter(|&depth| depth <= space.successors.len())
            .filter(|&depth| verified_through.is_none_or(|verified| depth > verified))
            .max()
            .map_or(0, |depth| depth + 1);
        let prefixes = prefix_hashes(&space.root, &space.successors, 0..prefix_count)?;
        let mut rejected = Vec::new();
        let mut unsupported = Vec::new();
        for (index, record) in document.keys.iter().enumerate() {
            let outcome = match record {
                Some(record) => self.check_key(&space, &prefixes, record, verified_through)?,
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
            updated_document: None,
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
            updated_document: None,
        }
    }
}

pub(super) fn verify_stored_signature(
    registry: &Registry,
    key: &PublicKey,
    bytes: &[u8],
    signature: &[u8],
) -> ProtocolResult<Checked<()>> {
    match signing::verify(registry, key, bytes, signature) {
        Ok(true) => Ok(Ok(())),
        Ok(false) => Ok(Err(Rejection::Signature)),
        Err(ProtocolError::Unsupported(_))
        | Err(ProtocolError::Crypto(Error::UnsupportedFormat)) => Ok(Err(Rejection::Unsupported)),
        Err(ProtocolError::ProviderFailure) => Err(ProtocolError::ProviderFailure),
        Err(_) => Ok(Err(Rejection::Signature)),
    }
}

#[cfg(test)]
std::thread_local! {
    static KEY_RECORDS_VERIFIED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::super::stored::PREFIX_ROWS_ENCODED;
    use super::*;
    use crate::agility::MechanismConfig;
    use crate::conformance::FixedRandom;
    use crate::protocol::records::space::{SpaceRoot, SpaceSuccessor};

    /// Alice reopens each held version of her rotated space. The prefix work
    /// is counted here because the store's public API exposes only the keys.
    #[test]
    fn loading_rotations_encodes_each_prefix_row_once() {
        let id = |number: usize| format!("00000000-0000-4000-8000-{number:012x}");
        let alice = "a11ce000-0000-5000-8000-00000000a11c";
        let domain = AppDomain::new(&id(1), Some("dev")).unwrap();
        let application = domain.account(alice).unwrap();
        let registry = Registry::builtin();
        let suite = registry.select(&MechanismConfig::default()).unwrap();
        let mechanism = suite.key_wrap.mechanism();
        let mut rng = FixedRandom((0..=255).cycle().take(65_536).collect());
        let device = LocalDevice::generate(&suite, &registry, &application, &mut rng).unwrap();
        let owner = StoreOwner {
            registry: &registry,
            domain: &domain,
            account_id: alice,
            device: &device,
        };
        let mut root = SpaceRoot {
            id: id(2),
            scope_id: id(3),
            identifier: id(4),
            creator_account_id: alice.into(),
            device_id: device.id.clone(),
            account_epoch_id: id(5),
            epoch_id: id(1000),
            mechanism: mechanism.id.into(),
            version: mechanism.version,
            verification: Vec::new(),
            account_envelope: vec![1],
            signature: vec![1],
        };
        let root_key = [0; 32];
        root.verification = suite
            .key_wrap
            .wrap(
                &root_key,
                &root.marker_context(&application).unwrap(),
                &[0; 32],
                &mut rng,
            )
            .unwrap();
        let mut space = HeldSpace {
            application,
            scope: root.scope().unwrap(),
            root,
            successors: Vec::new(),
            keys: BTreeMap::from([(
                0,
                HeldEntry {
                    key: Zeroizing::new(root_key),
                    mark: KeyMark::Published,
                },
            )]),
            verified_through: None,
        };
        let mut encoded = Vec::new();
        for depth in 1..=100 {
            let predecessor = space.epoch_at(depth - 1).unwrap().to_owned();
            let mut successor = SpaceSuccessor {
                id: id(100 + depth),
                scope_id: space.root.scope_id.clone(),
                identifier: space.root.identifier.clone(),
                predecessor: predecessor.clone(),
                history_epoch_id: predecessor,
                epoch_id: id(1000 + depth),
                author_account_id: alice.into(),
                author_device_id: device.id.clone(),
                author_epoch_id: space.root.account_epoch_id.clone(),
                verification: Vec::new(),
                history: vec![1],
                account_envelope: vec![1],
                signature: vec![1],
            };
            let key = Zeroizing::new([depth as u8; 32]);
            let context = successor
                .marker_context(&space.application, &space.scope)
                .unwrap();
            successor.verification = suite
                .key_wrap
                .wrap(&key, &context, &[0; 32], &mut rng)
                .unwrap();
            space.successors.push(successor);
            space.keep_entry(
                depth,
                HeldEntry {
                    key,
                    mark: KeyMark::Published,
                },
            );

            if depth == 10 || depth == 100 {
                let mut document = owner.seal_document(&space, &[], 0, &mut rng).unwrap();
                document.keys.reverse();
                let document = decode_document(&document.encode().unwrap()).unwrap();
                PREFIX_ROWS_ENCODED.set(0);
                let loaded = owner.load(&document).unwrap();
                assert!(loaded.rejected.is_empty());
                assert_eq!(loaded.space.unwrap().held().count(), depth + 1);
                encoded.push(PREFIX_ROWS_ENCODED.get());

                assert_verified_history(&owner, &space, depth, &mut rng);
            }
        }
        assert_eq!(encoded, [11, 101]);
    }

    // The crypto count is internal. Exercise the same bytes, merge and
    // reopen paths as the client, without requiring its database fixture.
    fn assert_verified_history(
        owner: &StoreOwner<'_>,
        space: &HeldSpace,
        depth: usize,
        rng: &mut dyn RandomSource,
    ) {
        let mut earlier = space.clone();
        earlier.keys.remove(&depth);
        earlier.successors.pop();
        let bytes = owner.seal_stored_document(&earlier, &[], 0, rng).unwrap();
        KEY_RECORDS_VERIFIED.set(0);
        let reopened = owner.load_stored_document(&bytes).unwrap().unwrap();
        assert_eq!(KEY_RECORDS_VERIFIED.get(), depth);
        let mut earlier = reopened.space.unwrap();
        assert!(reopened.updated_document.is_some());

        earlier.keep(space.clone()).unwrap();
        let bytes = owner.seal_stored_document(&earlier, &[], 0, rng).unwrap();
        KEY_RECORDS_VERIFIED.set(0);
        let reopened = owner.load_stored_document(&bytes).unwrap().unwrap();
        assert_eq!(KEY_RECORDS_VERIFIED.get(), 1, "only the new key");
        assert_eq!(reopened.space.unwrap().held().count(), depth + 1);
        let verified = reopened.updated_document.unwrap();

        KEY_RECORDS_VERIFIED.set(0);
        let reopened = owner.load_stored_document(&verified).unwrap().unwrap();
        assert_eq!(KEY_RECORDS_VERIFIED.get(), 0);
        assert!(reopened.updated_document.is_none());

        let (_, body, authentication) = decode_stored_document(&verified).unwrap();
        let (_, tag) = authentication.unwrap();
        let tampered = encode_verified_document(body, depth as u64 - 1, &tag).unwrap();
        KEY_RECORDS_VERIFIED.set(0);
        let reopened = owner.load_stored_document(&tampered).unwrap().unwrap();
        assert_eq!(KEY_RECORDS_VERIFIED.get(), depth + 1);
        assert!(reopened.rejected.is_empty());
        assert_eq!(reopened.updated_document.unwrap(), verified);

        // A damaged key fails the whole-document check. The individual
        // record check still identifies the damaged depth and keeps the rest.
        let mut damaged = body.to_vec();
        let signature = b"\"signature\":[";
        let position = damaged
            .windows(signature.len())
            .enumerate()
            .filter(|(_, value)| *value == signature)
            .nth(depth)
            .unwrap()
            .0
            + signature.len();
        let end = position
            + damaged[position..]
                .iter()
                .position(|&byte| byte == b']')
                .unwrap();
        let start = position
            + damaged[position..end]
                .iter()
                .rposition(|&byte| byte == b',')
                .unwrap()
            + 1;
        let replacement = if &damaged[start..end] == b"0" {
            b'1'
        } else {
            b'0'
        };
        damaged.splice(start..end, [replacement]);
        damaged.extend_from_slice(&verified[body.len()..]);
        KEY_RECORDS_VERIFIED.set(0);
        let reopened = owner.load_stored_document(&damaged).unwrap().unwrap();
        assert_eq!(KEY_RECORDS_VERIFIED.get(), depth + 1);
        assert_eq!(
            reopened.rejected,
            [Rejected {
                record: RejectedRecord::Key {
                    index: depth,
                    epoch_id: space.epoch_at(depth).map(str::to_owned),
                },
                reason: Rejection::Signature,
            }]
        );
        assert_eq!(reopened.space.unwrap().held().count(), depth);
        assert!(reopened.updated_document.is_none());

        let mut sparse = space.clone();
        sparse.keys.remove(&(depth - 1));
        let mut base = sparse.clone();
        base.keys.remove(&depth);
        let bytes = owner.seal_stored_document(&base, &[], 0, rng).unwrap();
        let mut base = owner
            .load_stored_document(&bytes)
            .unwrap()
            .unwrap()
            .space
            .unwrap();
        assert_eq!(base.verified_through, Some(depth - 2));
        base.keep(sparse).unwrap();
        let bytes = owner.seal_stored_document(&base, &[], 0, rng).unwrap();
        KEY_RECORDS_VERIFIED.set(0);
        let reopened = owner.load_stored_document(&bytes).unwrap().unwrap();
        assert_eq!(
            KEY_RECORDS_VERIFIED.get(),
            depth,
            "the gap clears the marker"
        );
        assert_eq!(reopened.space.unwrap().verified_through, Some(depth - 2));

        if depth == 10 {
            // Refused and lost creations retain their key for reads but
            // cannot advance the marker, even after repeated reopens.
            for (fate, winning_root) in [
                (super::super::held::CreationFate::NotAccepted, None),
                (
                    super::super::held::CreationFate::Accepted,
                    Some("another root"),
                ),
            ] {
                let mut creation = space.clone();
                creation.successors.clear();
                creation.keys.retain(|&depth, _| depth == 0);
                creation.keys.get_mut(&0).unwrap().mark = KeyMark::Unpublished {
                    transaction: "alice's pending creation".into(),
                };
                let bytes = owner.seal_stored_document(&creation, &[], 0, rng).unwrap();
                let opened = owner.load_stored_document(&bytes).unwrap().unwrap();
                assert!(opened.updated_document.is_none());
                let mut creation = opened.space.unwrap();
                creation.settle(fate, winning_root);
                let bytes = owner.seal_stored_document(&creation, &[], 0, rng).unwrap();
                KEY_RECORDS_VERIFIED.set(0);
                let opened = owner.load_stored_document(&bytes).unwrap().unwrap();
                assert_eq!(KEY_RECORDS_VERIFIED.get(), 1);
                assert_eq!(opened.space.unwrap().mark(0), Some(&KeyMark::Refused));
                assert!(opened.updated_document.is_none());
            }
        }
    }

    /// The byte fixture pins the optional field, domain, depth framing and
    /// tag without needing a full lineage or access to real device secrets.
    #[test]
    fn verified_document_bytes_are_pinned() {
        let domain = AppDomain::new("00000000-0000-4000-8000-000000000001", None).unwrap();
        let alice = "a11ce000-0000-5000-8000-00000000a11c";
        let registry = Registry::builtin();
        let suite = registry.select(&MechanismConfig::default()).unwrap();
        let mut rng = FixedRandom(vec![19; 512]);
        let mut device =
            LocalDevice::generate(&suite, &registry, &domain.account(alice).unwrap(), &mut rng)
                .unwrap();
        device.signing = suite.signature.derive_key_pair(&[7; 32]).unwrap();
        let owner = StoreOwner {
            registry: &registry,
            domain: &domain,
            account_id: alice,
            device: &device,
        };
        let body = br#"{"format":"jazz-e2ee-held-space-v1","keys":[],"lineage":null,"revision":0"#;
        assert_eq!(
            StoreOwner::authenticate_document(body, 0, &owner.document_key().unwrap()).unwrap(),
            concat!(
                r#"{"format":"jazz-e2ee-held-space-v1","keys":[],"lineage":null,"revision":0,"verification":["#,
                "0,[193,177,106,141,188,19,163,212,90,200,125,173,69,7,199,145,248,24,30,14,",
                "228,30,126,194,72,236,214,26,184,242,190,55]]}"
            )
            .as_bytes()
        );
    }
}
