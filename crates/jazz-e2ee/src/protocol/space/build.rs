//! The records each space publication writes (`E2EE_PROTOCOL.md` §3.1,
//! §3.2, §3.4, §3.5) and the preconditions that refuse it, as pure
//! functions.
//!
//! Every context's `application` is the domain of the space root's
//! creator, whoever writes. Writers seal, wrap and sign with the configured
//! [`Suite`]. Every marker and history link is unwrapped again, every
//! account envelope the writer can open is opened again, and every
//! signature is verified before records are returned; a failure to read
//! back fresh output is [`ProtocolError::ProviderFailure`]. Writers that
//! need an account key take the authenticated [`AccountKey`], and writers
//! that need a space key take it from the authenticated [`SpaceKeys`] of
//! that space's chain, never raw key bytes.

use zeroize::Zeroizing;

use crate::agility::PublicKey;
use crate::error::Error;
use crate::random::RandomSource;
use crate::registry::{Registry, Suite};

use super::super::account::AccountKey;
use super::super::account::AccountState;
use super::super::account::build::LocalDevice;
use super::super::derive;
use super::super::domain::AppDomain;
use super::super::error::{ProtocolError, ProtocolResult, RefusalCode};
use super::super::ids;
use super::super::records::space::{SpaceAddress, SpaceDelivery, SpaceRoot, SpaceSuccessor};
use super::super::signing;
use super::chain::SpaceChain;
use super::keys::{HeldVersion, Outcome, SpaceKeys, open_account_envelope, unwrap};
use super::ledger::Recipient;
use super::members::DELIVERY_BATCH;

const Z32: [u8; 32] = [0; 32];

fn refuse<T>(code: RefusalCode) -> ProtocolResult<T> {
    Err(ProtocolError::Refused(code))
}

fn provider_check(ok: bool) -> ProtocolResult<()> {
    if ok {
        Ok(())
    } else {
        Err(ProtocolError::ProviderFailure)
    }
}

fn random_key(rng: &mut dyn RandomSource) -> ProtocolResult<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new([0u8; 32]);
    rng.fill(&mut key[..])?;
    Ok(key)
}

/// What a writer of space records needs: the configured mechanisms, the
/// registry to read back with, the application, and the writing account.
#[derive(Clone, Copy)]
pub struct SpaceWriter<'a> {
    pub suite: Suite<'a>,
    pub registry: &'a Registry,
    pub domain: &'a AppDomain,
    pub account_id: &'a str,
}

/// The writing device's own account at the cut: its current state, the
/// authenticated current account key, and the device, which must be active.
#[derive(Clone, Copy)]
pub struct Author<'a> {
    pub state: &'a AccountState,
    pub account_key: &'a AccountKey,
    pub device: &'a LocalDevice,
}

impl SpaceWriter<'_> {
    /// Fails unless the author is this writer's account, its device is
    /// active, and its key is of the current account epoch.
    fn check_author(&self, author: &Author<'_>) -> ProtocolResult<()> {
        if author.state.account_id != self.account_id {
            return Err(ProtocolError::InconsistentEvidence);
        }
        if author.state.is_removed(&author.device.id) {
            return refuse(RefusalCode::DeviceRemoved);
        }
        if !author.state.is_active(&author.device.id) {
            return refuse(RefusalCode::DeviceNotActive);
        }
        if author.account_key.epoch_id() != author.state.epoch_id() {
            return Err(ProtocolError::AccountKeyUnavailable);
        }
        Ok(())
    }

    /// `wrap(K, C, p)` with the configured key wrapper, read back.
    fn wrap(
        &self,
        key: &[u8; 32],
        context: &[u8],
        payload: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<Vec<u8>> {
        let wrapped = self.suite.key_wrap.wrap(key, context, payload, rng)?;
        match unwrap(self.registry, key, context, &wrapped)? {
            Outcome::Ok(value) => provider_check(*value == *payload)?,
            _ => return Err(ProtocolError::ProviderFailure),
        }
        Ok(wrapped)
    }

    /// Seals `key` to `recipient`; a key of another mechanism than the
    /// selected one is "recipient unsupported".
    fn seal(
        &self,
        recipient: &PublicKey,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<Vec<u8>> {
        match self
            .suite
            .seal_to(core::slice::from_ref(recipient), context, key, rng)
        {
            Err(Error::RecipientUnsupported) => refuse(RefusalCode::UnsupportedRecipient),
            other => Ok(other?),
        }
    }

    /// Seals `key` to the author's own account sealing key of its current
    /// epoch, derived from the account key (§2.12), and opens it again.
    fn seal_to_self(
        &self,
        author: &Author<'_>,
        context: &[u8],
        key: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<Vec<u8>> {
        let pair = derive::account_sealing_key_pair(self.suite.key_seal, author.account_key.key())?;
        let envelope = self.seal(&pair.public, context, key, rng)?;
        match open_account_envelope(self.registry, author.account_key.key(), context, &envelope)? {
            Outcome::Ok(opened) => provider_check(*opened == *key)?,
            _ => return Err(ProtocolError::ProviderFailure),
        }
        Ok(envelope)
    }

    /// Signs with the device's signing key and verifies the signature
    /// again.
    fn sign(&self, device: &LocalDevice, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
        let signer = self
            .registry
            .signer(device.signing.public.mechanism.as_mechanism())?;
        let signature = signing::sign(signer, &device.signing, bytes)?;
        let verified = signing::verify(self.registry, &device.signing.public, bytes, &signature)
            .map_err(|_| ProtocolError::ProviderFailure)?;
        provider_check(verified)?;
        Ok(signature)
    }
}

/// A new space root (§3.2) and its key `K₀`, which the creating device
/// keeps as an `unpublished` held key until the root is accepted as the
/// space's root (§5.4). The key is never printed.
pub struct RootRecords {
    pub root: SpaceRoot,
    pub key: Zeroizing<[u8; 32]>,
}

impl core::fmt::Debug for RootRecords {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RootRecords")
            .field("root", &self.root)
            .field("key", &"..")
            .finish()
    }
}

/// A root of the space at `address` (§3.1, §3.2), by an active device of
/// the writer's account: a fresh root ID, epoch and key `K₀`; the marker
/// `wrap(K₀, X("verification", "", epochId), Z32)` under the selected key
/// wrapping mechanism, which the root's `mechanism` and `version` name; and
/// the creator's own copy, `accountEnvelope = seal(A(creator,
/// accountEpochId), X("account-envelope", json([creator, accountEpochId]),
/// epochId), K₀)`, sealed to the account's sealing key of its current epoch.
/// Whether the address already has a valid root is the caller's to read
/// (§3.2); the authority decides whether the account may create it (R15).
pub fn space_root(
    writer: &SpaceWriter<'_>,
    address: &SpaceAddress,
    author: &Author<'_>,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RootRecords> {
    writer.check_author(author)?;
    let application = writer.domain.account(writer.account_id)?;
    let key = random_key(rng)?;
    let wrap_mechanism = writer.suite.key_wrap.mechanism();
    let mut root = SpaceRoot {
        id: ids::random_v4(rng)?,
        scope_id: address.scope_id.clone(),
        identifier: address.identifier.clone(),
        creator_account_id: writer.account_id.to_owned(),
        device_id: author.device.id.clone(),
        account_epoch_id: author.state.epoch_id().to_owned(),
        epoch_id: ids::random_v4(rng)?,
        mechanism: wrap_mechanism.id.to_owned(),
        version: wrap_mechanism.version,
        verification: Vec::new(),
        account_envelope: Vec::new(),
        signature: Vec::new(),
    };
    root.verification = writer.wrap(&key, &root.marker_context(&application)?, &Z32, rng)?;
    root.account_envelope = writer.seal_to_self(
        author,
        &root.account_envelope_context(&application)?,
        &key,
        rng,
    )?;
    root.signature = writer.sign(author.device, &root.signed_bytes(&application)?)?;
    Ok(RootRecords { root, key })
}

/// A rotation (§3.5): the successor and the deliveries of its new key, in
/// one transaction. The new key is never printed.
pub struct RotationRecords {
    pub successor: SpaceSuccessor,
    /// One delivery of the new version to each recipient given: at most
    /// [`DELIVERY_BATCH`].
    pub deliveries: Vec<SpaceDelivery>,
    pub key: Zeroizing<[u8; 32]>,
}

impl core::fmt::Debug for RotationRecords {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RotationRecords")
            .field("successor", &self.successor)
            .field("deliveries", &self.deliveries)
            .field("key", &"..")
            .finish()
    }
}

/// A rotation (§3.5) by an active device of the writer's account that holds
/// the authenticated key of the current version: a successor whose
/// `predecessor` and `historyEpochId` are both the current version, and one
/// delivery of the new key to each of `recipients`, the rotation's own
/// batch ([`first`](super::members::RotationRecipients::first)). The
/// remaining members ([`rest`](super::members::RotationRecipients::rest))
/// are delivered to with [`deliver`] in later transactions of up to
/// [`DELIVERY_BATCH`], once the rotation is accepted. Without the current
/// version's key it is `rotation-needs-key`; more than [`DELIVERY_BATCH`]
/// recipients is an invalid input, refused before anything is generated.
pub fn rotate(
    writer: &SpaceWriter<'_>,
    chain: &SpaceChain,
    keys: &SpaceKeys,
    author: &Author<'_>,
    recipients: &[Recipient],
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecords> {
    let current = chain.current()?;
    keys.check_chain(chain)?;
    let Some(history) = keys.version(&current.epoch_id) else {
        return refuse(RefusalCode::RotationNeedsKey);
    };
    successor(writer, chain, &history, author, recipients, rng)
}

/// A recovery rotation (§3.5), which the application calls explicitly: a
/// successor whose `predecessor` is the current version and whose `history`
/// wraps the newest version the rotator holds, so the versions after that
/// one, up to and including the predecessor, are skipped. `recipients` is
/// the rotation's own batch, as for [`rotate`]. With no held version of the
/// space it is `rotation-needs-key`.
pub fn recover_rotation(
    writer: &SpaceWriter<'_>,
    chain: &SpaceChain,
    keys: &SpaceKeys,
    author: &Author<'_>,
    recipients: &[Recipient],
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecords> {
    chain.current()?;
    keys.check_chain(chain)?;
    let Some(history) = keys.newest() else {
        return refuse(RefusalCode::RotationNeedsKey);
    };
    successor(writer, chain, &history, author, recipients, rng)
}

fn successor(
    writer: &SpaceWriter<'_>,
    chain: &SpaceChain,
    history: &HeldVersion<'_>,
    author: &Author<'_>,
    recipients: &[Recipient],
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecords> {
    if recipients.len() > DELIVERY_BATCH {
        return Err(ProtocolError::InvalidRecord(
            "more deliveries than a rotation's own transaction holds",
        ));
    }
    writer.check_author(author)?;
    let current = chain.current()?;
    let application = &chain.application;
    let scope = &chain.scope;
    let key = random_key(rng)?;
    let mut successor = SpaceSuccessor {
        id: ids::random_v4(rng)?,
        scope_id: scope.address.scope_id.clone(),
        identifier: scope.address.identifier.clone(),
        predecessor: current.epoch_id.clone(),
        history_epoch_id: history.epoch_id.to_owned(),
        epoch_id: ids::random_v4(rng)?,
        author_account_id: writer.account_id.to_owned(),
        author_device_id: author.device.id.clone(),
        author_epoch_id: author.state.epoch_id().to_owned(),
        verification: Vec::new(),
        history: Vec::new(),
        account_envelope: Vec::new(),
        signature: Vec::new(),
    };
    successor.verification = writer.wrap(
        &key,
        &successor.marker_context(application, scope)?,
        &Z32,
        rng,
    )?;
    successor.history = writer.wrap(
        &key,
        &successor.history_context(application, scope)?,
        history.key,
        rng,
    )?;
    successor.account_envelope = writer.seal_to_self(
        author,
        &successor.account_envelope_context(application, scope)?,
        &key,
        rng,
    )?;
    successor.signature =
        writer.sign(author.device, &successor.signed_bytes(application, scope)?)?;
    let new_version = HeldVersion {
        application,
        scope,
        epoch_id: &successor.epoch_id,
        depth: chain.versions.len(),
        key: &key,
    };
    let mut deliveries = Vec::with_capacity(recipients.len());
    for recipient in recipients {
        if recipient.account_id == writer.account_id {
            continue;
        }
        deliveries.push(deliver(writer, &new_version, recipient, rng)?);
    }
    Ok(RotationRecords {
        successor,
        deliveries,
        key,
    })
}

/// A delivery of a held `version` to `recipient` (§3.1, §3.4): a fresh ID,
/// and `envelope = seal(A(recipient, recipientAccountEpochId),
/// X(json(["delivery", id, senderAccountId]), json([recipient,
/// recipientAccountEpochId]), keyEpochId), K)`, sealed to the recipient's
/// first valid sealing key record of its current epoch (§2.12). A record of
/// another mechanism than the selected one is "recipient unsupported".
pub fn deliver(
    writer: &SpaceWriter<'_>,
    version: &HeldVersion<'_>,
    recipient: &Recipient,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<SpaceDelivery> {
    let record = &recipient.sealing_key.row;
    if record.account_id != recipient.account_id || record.epoch_id != recipient.account_epoch_id {
        return Err(ProtocolError::InconsistentEvidence);
    }
    let mut delivery = SpaceDelivery {
        id: ids::random_v4(rng)?,
        scope_id: version.scope.address.scope_id.clone(),
        identifier: version.scope.address.identifier.clone(),
        key_epoch_id: version.epoch_id.to_owned(),
        sender_account_id: writer.account_id.to_owned(),
        recipient_account_id: recipient.account_id.clone(),
        recipient_account_epoch_id: recipient.account_epoch_id.clone(),
        envelope: Vec::new(),
    };
    let context = delivery.seal_context(version.application, version.scope)?;
    delivery.envelope = writer.seal(&record.public(), &context, version.key, rng)?;
    Ok(delivery)
}
