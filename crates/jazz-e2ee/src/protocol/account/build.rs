//! The records each account publication writes (`E2EE_PROTOCOL.md` §2.1–§2.11)
//! and the preconditions that refuse it, as pure functions.
//!
//! Writers seal, wrap and sign with the configured [`Suite`]. Every marker
//! is unwrapped again and every signature verified before records are
//! returned; any failure to read back fresh output, a malformed envelope
//! included, is [`ProtocolError::ProviderFailure`], never a refusal or
//! invalid ciphertext. Writers that need the account key take the
//! [`AccountKey`] key authentication returned, never raw key bytes.

use zeroize::Zeroizing;

use crate::agility::{KeyPair, PublicKey};
use crate::error::Error;
use crate::random::RandomSource;
use crate::registry::{Registry, Suite};

use super::super::derive::{self, RecoveryKeys};
use super::super::device_account::DeviceAccountIdentity;
use super::super::domain::{AccountDomain, AppDomain};
use super::super::error::{
    IntrinsicContradiction, ProtocolError, ProtocolResult, RecoveryFailure, RefusalCode,
};
use super::super::ids;
use super::super::json;
use super::super::records::KeyColumns;
use super::super::records::account::{
    AccountAction, AccountIdentity, AccountKind, AccountRoot, AccountSealingKey, AccountSuccessor,
    ChallengeCoordinates, DeviceApproval, DeviceChallenge, DeviceDelivery, DeviceKeysRow,
    DeviceProof, DeviceRemovalRequest, DeviceRequest, LEDGER_VERSION, PublicAccountSuccessor,
    PublicDeviceApproval, RecoveryDelivery, RecoveryRoot, approval_context, approval_signed_bytes,
    challenge_signed_bytes, delivery_marker_context, identity_context, nonce_commitment,
    proof_signed_bytes, recovery_delivery_context, successor_context,
};
use super::super::signing;
use super::key::AccountKey;
use super::ledger::{PrivateAccountLedger, PublicAccountLedger};
use super::replay::AccountState;

const Z32: [u8; 32] = [0; 32];

fn refuse<T>(code: RefusalCode) -> ProtocolResult<T> {
    Err(ProtocolError::Refused(code))
}

/// A self-check that failed: the provider produced something it cannot
/// read back.
fn provider_check(ok: bool) -> ProtocolResult<()> {
    if ok {
        Ok(())
    } else {
        Err(ProtocolError::ProviderFailure)
    }
}

/// Reading back output this writer just produced: whatever fails there is
/// the provider's failure, not a property of any input.
fn read_back<T, E>(result: Result<T, E>) -> ProtocolResult<T> {
    result.map_err(|_| ProtocolError::ProviderFailure)
}

fn random_key(rng: &mut dyn RandomSource) -> ProtocolResult<Zeroizing<[u8; 32]>> {
    let mut key = Zeroizing::new([0u8; 32]);
    rng.fill(&mut key[..])?;
    Ok(key)
}

/// A device's approval nonce `n_R` (§2.5): committed in its request,
/// revealed in its one approval proof. Cleared on drop and never printed.
#[derive(Clone)]
pub struct ApprovalNonce(pub Zeroizing<[u8; 32]>);

impl core::fmt::Debug for ApprovalNonce {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str("ApprovalNonce(..)")
    }
}

/// This install's encryption identity in one account domain (§2.1). Its
/// approval nonce answers one challenge (§2.2, §2.5): [`respond`] and
/// [`recovery_approve`] take it, and refuse a further challenge. A device
/// whose attempt failed or was cancelled after that starts again as a new
/// device, with a new ID and request. The runtime persists the nonce's
/// spent state with the device; a copy of a device does not make it usable
/// again.
#[derive(Debug, Clone)]
pub struct LocalDevice {
    pub id: String,
    pub sealing: KeyPair,
    pub signing: KeyPair,
    /// The commitment to the approval nonce, which the request carries.
    pub approval_commitment: [u8; 32],
    /// The approval nonce until a challenge was answered with it.
    pub approval_nonce: Option<ApprovalNonce>,
}

impl LocalDevice {
    /// A new device with the configured mechanisms, checked over `scope`
    /// (the account domain) before it is returned.
    pub fn generate(
        suite: &Suite<'_>,
        registry: &Registry,
        scope: &AccountDomain,
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<Self> {
        let id = ids::random_v4(rng)?;
        let nonce = random_key(rng)?;
        let device = Self {
            approval_commitment: nonce_commitment(&id, &nonce[..])?,
            approval_nonce: Some(ApprovalNonce(nonce)),
            id,
            sealing: suite.key_seal.generate_key_pair(rng)?,
            signing: suite.signature.generate_key_pair(rng)?,
        };
        device.check(suite, registry, scope, rng)?;
        Ok(device)
    }

    /// The local check (§2.1): a seal/open and a sign/verify over
    /// `UTF8("jazz.e2ee.local-device-check.v1\0" ‖ scope)`.
    pub fn check(
        &self,
        suite: &Suite<'_>,
        registry: &Registry,
        scope: &AccountDomain,
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<()> {
        let bytes = derive::local_device_check(scope.as_str());
        let probe = random_key(rng)?;
        let sealed = suite.seal_to(
            core::slice::from_ref(&self.sealing.public),
            &bytes,
            &probe,
            rng,
        )?;
        let opened =
            read_back(registry.open_sealed(core::slice::from_ref(&self.sealing), &bytes, &sealed))?;
        provider_check(*opened == *probe)?;
        let signature = registry
            .signer(self.signing.public.mechanism.as_mechanism())?
            .sign(&self.signing, &bytes)?;
        provider_check(read_back(signing::verify(
            registry,
            &self.signing.public,
            &bytes,
            &signature,
        ))?)
    }

    pub fn keys(&self) -> KeyColumns {
        KeyColumns::new(&self.sealing.public, &self.signing.public)
    }

    /// The request row (§2.2): row ID = device ID, with the commitment to
    /// the device's approval nonce.
    pub fn request(&self) -> DeviceRequest {
        DeviceRequest {
            id: self.id.clone(),
            keys: self.keys(),
            nonce_commitment: self.approval_commitment.to_vec(),
        }
    }

    /// The public projection of the request's keys.
    pub fn keys_row(&self, rng: &mut dyn RandomSource) -> ProtocolResult<DeviceKeysRow> {
        Ok(DeviceKeysRow {
            id: ids::random_v4(rng)?,
            device_id: self.id.clone(),
            keys: self.keys(),
        })
    }

    /// Whether `request` is this device's request with its keys and nonce
    /// commitment.
    pub fn matches(&self, request: &DeviceRequest) -> bool {
        *request == self.request()
    }
}

/// What a writer for one account needs: the configured mechanisms, the
/// registry to read back with, and the account's domain.
#[derive(Clone, Copy)]
pub struct AccountWriter<'a> {
    pub suite: Suite<'a>,
    pub registry: &'a Registry,
    pub domain: &'a AppDomain,
    pub account_id: &'a str,
}

impl AccountWriter<'_> {
    fn application(&self) -> ProtocolResult<AccountDomain> {
        self.domain.account(self.account_id)
    }

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

    /// `wrap(K, C, p)` with the configured key wrapper, read back.
    fn wrap(
        &self,
        key: &[u8; 32],
        context: &[u8],
        payload: &[u8; 32],
        rng: &mut dyn RandomSource,
    ) -> ProtocolResult<Vec<u8>> {
        let wrapped = self.suite.key_wrap.wrap(key, context, payload, rng)?;
        let wrapper = read_back(self.registry.key_wrapper_for(&wrapped))?;
        let unwrapped = read_back(wrapper.unwrap(key, context, &wrapped))?;
        provider_check(*unwrapped == *payload)?;
        Ok(wrapped)
    }

    /// Signs with `key` (whose mechanism must be registered) and verifies
    /// the signature again.
    fn sign(&self, key: &KeyPair, bytes: &[u8]) -> ProtocolResult<Vec<u8>> {
        let signer = self.registry.signer(key.public.mechanism.as_mechanism())?;
        let signature = signing::sign(signer, key, bytes)?;
        provider_check(read_back(signing::verify(
            self.registry,
            &key.public,
            bytes,
            &signature,
        ))?)?;
        Ok(signature)
    }

    fn check_device_request(
        &self,
        device: &LocalDevice,
        public: &PublicAccountLedger,
        private: &PrivateAccountLedger,
    ) -> ProtocolResult<()> {
        let mut requests = private
            .requests
            .iter()
            .filter(|request| request.row.id == device.id);
        let (Some(request), None) = (requests.next(), requests.next()) else {
            return refuse(RefusalCode::NoDeviceRequest);
        };
        let mut projections = public
            .keys
            .iter()
            .filter(|key| key.row.device_id == device.id)
            .peekable();
        if !device.matches(&request.row)
            || projections.peek().is_none()
            || !projections.all(|key| key.row.matches(&request.row))
        {
            return refuse(RefusalCode::DeviceKeysMismatch);
        }
        Ok(())
    }
}

/// A new account root: its private identity, its public root, the sealing
/// key record of its epoch (§2.12) and the epoch key. The key is never
/// printed.
pub struct RootRecords {
    pub identity: AccountIdentity,
    pub root: AccountRoot,
    pub account_key: AccountSealingKey,
    pub key: Zeroizing<[u8; 32]>,
}

impl core::fmt::Debug for RootRecords {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RootRecords")
            .field("identity", &self.identity)
            .field("root", &self.root)
            .field("account_key", &self.account_key)
            .field("key", &"..")
            .finish()
    }
}

/// Permission to write a root of `generation`, granted only by
/// [`check_bootstrap`] (generation 0) or [`check_reset`] (`g + 1`) for one
/// device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootPermit {
    device_id: String,
    generation: u64,
}

impl RootPermit {
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Bootstrap preconditions (§2.3): no account evidence of any kind, and
/// exactly one request of this device with matching keys and at least one
/// matching key projection.
pub fn check_bootstrap(
    writer: &AccountWriter<'_>,
    device: &LocalDevice,
    public: &PublicAccountLedger,
    private: &PrivateAccountLedger,
) -> ProtocolResult<RootPermit> {
    if !public.is_empty() || private.has_history() {
        return refuse(RefusalCode::AccountHasHistory);
    }
    writer.check_device_request(device, public, private)?;
    Ok(RootPermit {
        device_id: device.id.clone(),
        generation: 0,
    })
}

/// How the account signs in, and whether the application allows resets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResetPolicy {
    pub device_account: bool,
    pub allow_account_reset: bool,
}

/// Reset preconditions (§2.11): an app-authenticated session, reset
/// allowed, a canonical `app` root (`state`; an account whose root is
/// `device-account` never resets, whoever signs in), this device's request
/// matching its keys, and this device not active in the current generation.
pub fn check_reset(
    writer: &AccountWriter<'_>,
    policy: ResetPolicy,
    state: &AccountState,
    device: &LocalDevice,
    public: &PublicAccountLedger,
    private: &PrivateAccountLedger,
) -> ProtocolResult<RootPermit> {
    if policy.device_account
        || !policy.allow_account_reset
        || state.root.row.kind != AccountKind::App
    {
        return refuse(RefusalCode::AccountResetNotAllowed);
    }
    writer.check_device_request(device, public, private)?;
    if state.is_active(&device.id) {
        return refuse(RefusalCode::ResetDeviceActive);
    }
    let generation = state
        .generation
        .checked_add(1)
        .ok_or(ProtocolError::InvalidRecord("generation overflow"))?;
    Ok(RootPermit {
        device_id: device.id.clone(),
        generation,
    })
}

/// What authenticates a new account root (§2.3).
#[derive(Debug, Clone, Copy)]
pub enum RootAuthority<'a> {
    /// An account of the application's own sign-in: the root is
    /// authenticated only by its author.
    App,
    /// A device account: the root is signed by the identity, whose key
    /// must derive the account ID.
    DeviceAccount(&'a DeviceAccountIdentity),
}

/// The identity, public root and sealing key record `permit` allows for
/// `device` (§2.3; at `g + 1` the reset of §2.11, whose identity has a
/// random ID), of the kind `authority` gives. Both envelopes are opened
/// again and every signature is verified before the records are returned.
pub fn account_root(
    writer: &AccountWriter<'_>,
    device: &LocalDevice,
    permit: &RootPermit,
    authority: RootAuthority<'_>,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RootRecords> {
    if permit.device_id != device.id {
        return refuse(RefusalCode::NoDeviceRequest);
    }
    if let RootAuthority::DeviceAccount(identity) = authority
        && identity.account_id(writer.domain).as_deref() != Some(writer.account_id)
    {
        return refuse(RefusalCode::DeviceAccountIdentityMismatch);
    }
    let generation = permit.generation;
    let application = writer.application()?;
    let identity_id = if generation == 0 {
        writer.account_id.to_owned()
    } else {
        ids::random_v4(rng)?
    };
    let epoch_id = ids::random_v4(rng)?;
    let key = random_key(rng)?;
    let envelope_context = identity_context(
        &application,
        writer.account_id,
        &identity_id,
        &epoch_id,
        "envelope",
        &device.id,
    )?;
    let envelope = writer.seal(&device.sealing.public, &envelope_context, &key, rng)?;
    let opened = read_back(writer.registry.open_sealed(
        core::slice::from_ref(&device.sealing),
        &envelope_context,
        &envelope,
    ))?;
    provider_check(*opened == *key)?;
    let marker_context = identity_context(
        &application,
        writer.account_id,
        &identity_id,
        &epoch_id,
        "verification",
        "",
    )?;
    let verification = writer.wrap(&key, &marker_context, &Z32, rng)?;
    let mut root = AccountRoot {
        id: ids::random_v4(rng)?,
        ledger_version: LEDGER_VERSION,
        generation,
        account_id: writer.account_id.to_owned(),
        device_id: device.id.clone(),
        epoch_id: epoch_id.clone(),
        kind: AccountKind::App,
        device_account_public_key: None,
        device_account_signature: None,
    };
    if let RootAuthority::DeviceAccount(identity) = authority {
        root.kind = AccountKind::DeviceAccount;
        root.device_account_public_key = Some(identity.public_key().to_vec());
        root.device_account_signature = Some(Vec::new());
        let bytes = root.signed_bytes(&application, &device.keys())?;
        let signature = identity.sign(&bytes)?;
        provider_check(read_back(super::super::device_account::verify(
            writer.registry,
            identity.public_key(),
            &bytes,
            &signature,
        ))?)?;
        root.device_account_signature = Some(signature);
    }
    let account_key = sealing_key_record(writer, device, &epoch_id, &key, rng)?;
    Ok(RootRecords {
        identity: AccountIdentity {
            id: identity_id,
            ledger_version: LEDGER_VERSION,
            generation,
            device_id: device.id.clone(),
            epoch_id: epoch_id.clone(),
            envelope,
            verification,
        },
        root,
        account_key,
        key,
    })
}

/// The sealing key record of account epoch `epoch_id` with key `key`, for
/// the selected key sealing mechanism (§2.12), signed by `signer`.
fn sealing_key_record(
    writer: &AccountWriter<'_>,
    signer: &LocalDevice,
    epoch_id: &str,
    key: &[u8; 32],
    rng: &mut dyn RandomSource,
) -> ProtocolResult<AccountSealingKey> {
    let pair = derive::account_sealing_key_pair(writer.suite.key_seal, key)?;
    let mut record = AccountSealingKey {
        id: ids::random_v4(rng)?,
        account_id: writer.account_id.to_owned(),
        epoch_id: epoch_id.to_owned(),
        signer_id: signer.id.clone(),
        mechanism: pair.public.mechanism.id.clone(),
        version: pair.public.mechanism.version,
        public_key: pair.public.bytes.clone(),
        signature: Vec::new(),
    };
    let bytes = record.signed_bytes(&writer.application()?)?;
    record.signature = writer.sign(&signer.signing, &bytes)?;
    Ok(record)
}

/// A later publication of the current epoch's sealing key record for the
/// selected mechanism (§2.12, stage `account-key`), by an active device
/// holding the current account key: the case of a mechanism migration,
/// since the epoch's creation wrote the record of the mechanism selected
/// then. It is for a selected mechanism the current epoch has no valid
/// record of; the caller decides that, as replay does not yet judge
/// sealing key records.
pub fn account_sealing_key(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    signer: &LocalDevice,
    account_key: &AccountKey,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<AccountSealingKey> {
    if !state.is_active(&signer.id) {
        return refuse(RefusalCode::DeviceNotActive);
    }
    let key = current_key(state, account_key)?;
    sealing_key_record(writer, signer, state.epoch_id(), key, rng)
}

/// A challenge and the secret it seals, kept only in memory and never
/// printed.
pub struct Challenge {
    pub challenge: DeviceChallenge,
    pub secret: Zeroizing<[u8; 32]>,
    /// The request the challenge was signed over; [`approve`] completes
    /// the challenge for this request only.
    pub request: DeviceRequest,
}

impl core::fmt::Debug for Challenge {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("Challenge")
            .field("challenge", &self.challenge)
            .field("secret", &"..")
            .finish()
    }
}

/// Fails unless `key` authenticated the current epoch of `state`, against a
/// state with the same accepted approvals: a key authenticated at an earlier
/// read, before an approval this state includes, is refused and must be
/// authenticated again (a rotation from it would brick the account).
fn current_key<'k>(state: &AccountState, key: &'k AccountKey) -> ProtocolResult<&'k [u8; 32]> {
    if key.epoch_id() == state.epoch_id()
        && key.authenticated().public_approval_ids == state.current_approval_ids()
    {
        Ok(key.key())
    } else {
        Err(ProtocolError::AccountKeyUnavailable)
    }
}

/// Challenge (§2.5 step 1), by an active approver to a requester that is
/// not removed and whose request matches its published keys.
pub fn challenge(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    approver: &LocalDevice,
    requester: &DeviceRequest,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<Challenge> {
    if !state.is_active(&approver.id) {
        return refuse(RefusalCode::DeviceNotActive);
    }
    if state.is_removed(&requester.id) {
        return refuse(RefusalCode::DeviceRemoved);
    }
    if state.keys.get(&requester.id) != Some(&requester.keys) {
        return refuse(RefusalCode::DeviceKeysMismatch);
    }
    let application = writer.application()?;
    let secret = random_key(rng)?;
    let id = ids::random_v4(rng)?;
    let coordinates = ChallengeCoordinates {
        id: &id,
        device_id: &requester.id,
        epoch_id: state.epoch_id(),
    };
    let context = approval_context(&application, writer.account_id, coordinates, "challenge")?;
    let envelope = writer.seal(&requester.keys.sealing_key(), &context, &secret, rng)?;
    let nonce = random_key(rng)?;
    let challenge = signed_challenge(
        writer,
        &application,
        &id,
        approver,
        requester,
        state.epoch_id(),
        envelope,
        &nonce[..],
    )?;
    Ok(Challenge {
        challenge,
        secret,
        request: requester.clone(),
    })
}

/// A challenge row signed by `approver` over the requester keys and nonce
/// commitment it was shown, with its nonce `n_A` (§2.5).
#[allow(clippy::too_many_arguments)]
fn signed_challenge(
    writer: &AccountWriter<'_>,
    application: &super::super::domain::AccountDomain,
    id: &str,
    approver: &LocalDevice,
    requester: &DeviceRequest,
    epoch_id: &str,
    envelope: Vec<u8>,
    nonce: &[u8],
) -> ProtocolResult<DeviceChallenge> {
    let coordinates = ChallengeCoordinates {
        id,
        device_id: &requester.id,
        epoch_id,
    };
    let bytes = challenge_signed_bytes(
        application,
        writer.account_id,
        coordinates,
        &approver.id,
        requester,
        &envelope,
        nonce,
    )?;
    Ok(DeviceChallenge {
        id: id.to_owned(),
        device_id: requester.id.clone(),
        epoch_id: epoch_id.to_owned(),
        signer_id: approver.id.clone(),
        signature: writer.sign(&approver.signing, &bytes)?,
        envelope,
        nonce: nonce.to_vec(),
    })
}

/// Response (§2.5 step 2): the requester checks that an active device of
/// the account signed the challenge over the requester's own keys
/// (`invalid-challenge` otherwise: the approver was shown other keys), opens
/// the secret and proves possession of both its keys.
pub fn respond(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    device: &mut LocalDevice,
    challenge: &DeviceChallenge,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<DeviceProof> {
    if challenge.device_id != device.id || challenge.epoch_id != state.epoch_id() {
        return refuse(RefusalCode::StaleChallenge);
    }
    if challenge.validate().is_err() || !state.is_active(&challenge.signer_id) {
        return refuse(RefusalCode::InvalidChallenge);
    }
    let signer_keys = state
        .keys
        .get(&challenge.signer_id)
        .ok_or(ProtocolError::Intrinsic(
            IntrinsicContradiction::MissingSignerKey,
        ))?;
    let bytes = challenge_signed_bytes(
        &writer.application()?,
        writer.account_id,
        challenge.coordinates(),
        &challenge.signer_id,
        &device.request(),
        &challenge.envelope,
        &challenge.nonce,
    )?;
    if !signing::verify(
        writer.registry,
        &signer_keys.signing_key(),
        &bytes,
        &challenge.signature,
    )? {
        return refuse(RefusalCode::InvalidChallenge);
    }
    // The nonce answers one challenge only: a second would let whoever
    // sent the first search for a challenge nonce matching another code.
    let Some(nonce) = device.approval_nonce.take() else {
        return refuse(RefusalCode::InvalidChallenge);
    };
    prove(writer, device, challenge, &nonce, rng)
}

/// Opens the challenge's secret and proves possession of both keys. The
/// caller has checked the challenge: [`respond`], or a recovery, where the
/// device wrote its own challenge in this process.
fn prove(
    writer: &AccountWriter<'_>,
    device: &LocalDevice,
    challenge: &DeviceChallenge,
    nonce: &ApprovalNonce,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<DeviceProof> {
    let application = writer.application()?;
    let coordinates = challenge.coordinates();
    let context = approval_context(&application, writer.account_id, coordinates, "challenge")?;
    let secret = writer.registry.open_sealed(
        core::slice::from_ref(&device.sealing),
        &context,
        &challenge.envelope,
    )?;
    let proof_context = approval_context(&application, writer.account_id, coordinates, "proof")?;
    let proof = writer.wrap(&secret, &proof_context, &Z32, rng)?;
    let nonce = nonce.0.to_vec();
    let bytes = proof_signed_bytes(&application, writer.account_id, coordinates, &proof, &nonce)?;
    Ok(DeviceProof {
        id: challenge.id.clone(),
        challenge_id: challenge.id.clone(),
        signature: writer.sign(&device.signing, &bytes)?,
        proof,
        nonce,
    })
}

/// The possession check (§2.5 step 3): the proof's nonce opens the
/// request's nonce commitment, the proof unwraps to `Z32` under the secret
/// and its signature verifies under the request's signing key.
/// Anything provably wrong is `invalid-possession-proof`; a provider failure
/// stays one.
pub fn verify_possession(
    writer: &AccountWriter<'_>,
    challenge: &DeviceChallenge,
    proof: &DeviceProof,
    requester: &DeviceRequest,
    secret: &[u8; 32],
) -> ProtocolResult<()> {
    if proof.id != challenge.id
        || proof.challenge_id != challenge.id
        || challenge.device_id != requester.id
        || nonce_commitment(&requester.id, &proof.nonce)?[..] != requester.nonce_commitment[..]
    {
        return refuse(RefusalCode::InvalidPossessionProof);
    }
    let application = writer.application()?;
    let coordinates = challenge.coordinates();
    let context = approval_context(&application, writer.account_id, coordinates, "proof")?;
    let unwrapped = match writer.registry.key_wrapper_for(&proof.proof) {
        Ok(wrapper) => wrapper
            .unwrap(secret, &context, &proof.proof)
            .map_err(Error::after_routing),
        Err(error) => Err(error),
    };
    match unwrapped {
        Ok(value) if *value == Z32 => {}
        Ok(_) | Err(Error::InvalidCiphertext) => {
            return refuse(RefusalCode::InvalidPossessionProof);
        }
        Err(error) => return Err(error.into()),
    }
    let bytes = proof_signed_bytes(
        &application,
        writer.account_id,
        coordinates,
        &proof.proof,
        &proof.nonce,
    )?;
    if !signing::verify(
        writer.registry,
        &requester.keys.signing_key(),
        &bytes,
        &proof.signature,
    )? {
        return refuse(RefusalCode::InvalidPossessionProof);
    }
    Ok(())
}

/// The three rows of an approval (§2.5 step 3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApprovalRecords {
    pub approval: DeviceApproval,
    pub public: PublicDeviceApproval,
    pub delivery: DeviceDelivery,
}

#[allow(clippy::too_many_arguments)]
fn approval_records(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    approver: &LocalDevice,
    challenge: &DeviceChallenge,
    requester: &DeviceRequest,
    account_key: &AccountKey,
    recovery: Option<&RecoveryKeys>,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<ApprovalRecords> {
    let account_key = current_key(state, account_key)?;
    let application = writer.application()?;
    let coordinates = challenge.coordinates();
    let marker_context =
        approval_context(&application, writer.account_id, coordinates, "approval")?;
    let verification = writer.wrap(account_key, &marker_context, &Z32, rng)?;
    let approval_bytes = approval_signed_bytes(
        &application,
        writer.account_id,
        coordinates,
        &approver.id,
        &verification,
    )?;
    let approval = DeviceApproval {
        id: challenge.id.clone(),
        challenge_id: challenge.id.clone(),
        signer_id: approver.id.clone(),
        signature: writer.sign(&approver.signing, &approval_bytes)?,
        verification,
    };
    // The public approval shares the challenge's ID, so private
    // verification pairs the two by ID (§2.4).
    let mut public = PublicDeviceApproval {
        id: challenge.id.clone(),
        account_id: writer.account_id.to_owned(),
        epoch_id: state.epoch_id().to_owned(),
        device_id: requester.id.clone(),
        signer_id: approver.id.clone(),
        signature: Vec::new(),
        recovery_root_id: recovery.map(|keys| keys.root_id.clone()),
        recovery_signature: None,
    };
    let public_bytes = public.signed_bytes(&application, &requester.keys)?;
    public.signature = writer.sign(&approver.signing, &public_bytes)?;
    if let Some(recovery) = recovery {
        public.recovery_signature = Some(writer.sign(&recovery.signing, &public_bytes)?);
    }
    let delivery_context =
        approval_context(&application, writer.account_id, coordinates, "delivery")?;
    let envelope = writer.seal(
        &requester.keys.sealing_key(),
        &delivery_context,
        account_key,
        rng,
    )?;
    let delivery_marker =
        delivery_marker_context(&application, writer.account_id, coordinates, &envelope)?;
    let delivery = DeviceDelivery {
        id: challenge.id.clone(),
        challenge_id: challenge.id.clone(),
        verification: writer.wrap(account_key, &delivery_marker, &Z32, rng)?,
        envelope,
    };
    Ok(ApprovalRecords {
        approval,
        public,
        delivery,
    })
}

/// Completion (§2.5 step 3) by an active approver holding the current
/// account key, after the possession check.
#[allow(clippy::too_many_arguments)]
pub fn approve(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    approver: &LocalDevice,
    challenge: &Challenge,
    proof: &DeviceProof,
    requester: &DeviceRequest,
    account_key: &AccountKey,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<ApprovalRecords> {
    if challenge.challenge.epoch_id != state.epoch_id()
        || challenge.challenge.device_id != requester.id
        || *requester != challenge.request
    {
        return refuse(RefusalCode::StaleChallenge);
    }
    if !state.is_active(&approver.id) {
        return refuse(RefusalCode::DeviceNotActive);
    }
    if state.is_removed(&requester.id) {
        return refuse(RefusalCode::DeviceRemoved);
    }
    verify_possession(
        writer,
        &challenge.challenge,
        proof,
        requester,
        &challenge.secret,
    )?;
    approval_records(
        writer,
        state,
        approver,
        &challenge.challenge,
        requester,
        account_key,
        None,
        rng,
    )
}

/// Every row a recovery restore writes (§2.9): the device challenges,
/// answers and approves itself, and the public approval carries both its
/// own and the recovery root's signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryApprovalRecords {
    pub challenge: DeviceChallenge,
    pub proof: DeviceProof,
    pub records: ApprovalRecords,
}

/// A device's self-approval with recovery keys, given the account key it
/// obtained from a recovery delivery. `None` when the device is already
/// active: nothing is written.
pub fn recovery_approve(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    device: &mut LocalDevice,
    recovery: &RecoveryKeys,
    account_key: &AccountKey,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<Option<RecoveryApprovalRecords>> {
    let root_matches = state.recovery_root(&recovery.root_id).is_some_and(|root| {
        root.row.keys.sealing_key() == recovery.sealing.public
            && root.row.keys.signing_key() == recovery.signing.public
    });
    if !root_matches {
        return Err(ProtocolError::Recovery(RecoveryFailure::RootMismatch));
    }
    if state.is_removed(&device.id) {
        return refuse(RefusalCode::DeviceRemoved);
    }
    if state.is_active(&device.id) {
        return Ok(None);
    }
    let request = device.request();
    if state.keys.get(&device.id) != Some(&request.keys) {
        return refuse(RefusalCode::DeviceKeysMismatch);
    }
    let application = writer.application()?;
    let id = ids::random_v4(rng)?;
    let coordinates = ChallengeCoordinates {
        id: &id,
        device_id: &device.id,
        epoch_id: state.epoch_id(),
    };
    let secret = random_key(rng)?;
    let context = approval_context(&application, writer.account_id, coordinates, "challenge")?;
    let envelope = writer.seal(&device.sealing.public, &context, &secret, rng)?;
    // A recovery is approved by the recovery root, not an active device: the
    // device signs its own challenge, which it never receives from anyone.
    let nonce = random_key(rng)?;
    let challenge = signed_challenge(
        writer,
        &application,
        &id,
        device,
        &request,
        state.epoch_id(),
        envelope,
        &nonce[..],
    )?;
    let Some(approval_nonce) = device.approval_nonce.take() else {
        return refuse(RefusalCode::InvalidChallenge);
    };
    let proof = prove(writer, device, &challenge, &approval_nonce, rng)?;
    verify_possession(writer, &challenge, &proof, &request, &secret)?;
    let records = approval_records(
        writer,
        state,
        device,
        &challenge,
        &request,
        account_key,
        Some(recovery),
        rng,
    )?;
    Ok(Some(RecoveryApprovalRecords {
        challenge,
        proof,
        records,
    }))
}

/// What an account rotation removes or retires (§2.8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RotationTarget {
    RemoveDevice(String),
    RetireRecoveryRoot(String),
}

/// The rows of an account rotation, the sealing key record of its new
/// epoch (§2.12) and its new key, which is never printed.
pub struct RotationRecords {
    pub private: AccountSuccessor,
    pub public: PublicAccountSuccessor,
    pub recovery_deliveries: Vec<RecoveryDelivery>,
    pub account_key: AccountSealingKey,
    pub key: Zeroizing<[u8; 32]>,
}

impl core::fmt::Debug for RotationRecords {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("RotationRecords")
            .field("private", &self.private)
            .field("public", &self.public)
            .field("recovery_deliveries", &self.recovery_deliveries)
            .field("account_key", &self.account_key)
            .field("key", &"..")
            .finish()
    }
}

/// An account rotation (§2.8) by an active author holding the current
/// `account_key`. Members are the active devices minus a removed target; the
/// private revision lists the private approvals of the current epoch that
/// authenticated with the key, the public one the public approvals replay
/// accepted in it. Recovery roots whose key is not of the selected sealing
/// mechanism get no delivery (§0, mechanism agility) and do not block the
/// rotation. The new epoch's sealing key record is signed by the author. A
/// device never removes itself (`self-removal`): it generated `K'`, so it
/// would keep the new epoch's key.
pub fn rotate(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    author: &LocalDevice,
    account_key: &AccountKey,
    target: &RotationTarget,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RotationRecords> {
    if !state.is_active(&author.id) {
        return refuse(RefusalCode::DeviceNotActive);
    }
    let authenticated = account_key.authenticated();
    let account_key = current_key(state, account_key)?;
    let mut members = state.active.clone();
    let (action, removed, retired) = match target {
        RotationTarget::RemoveDevice(device) => {
            // The author generates K'; a device removing itself would keep
            // it, and every space key later delivered to the account.
            if *device == author.id {
                return refuse(RefusalCode::SelfRemoval);
            }
            if !members.remove(device) {
                return refuse(RefusalCode::TargetNotActive);
            }
            (
                AccountAction::RemoveDevice,
                Some(device.clone()),
                held_recovery_roots(state, device),
            )
        }
        RotationTarget::RetireRecoveryRoot(root) => {
            if state.recovery_root(root).is_none() {
                return refuse(RefusalCode::TargetNotActive);
            }
            (AccountAction::RetireRecoveryRoot, None, vec![root.clone()])
        }
    };
    let retired_ids = json::encode_root_id_list(&retired)?;
    let application = writer.application()?;
    let account_id = writer.account_id;
    let id = ids::random_v4(rng)?;
    let epoch_id = ids::random_v4(rng)?;
    let key = random_key(rng)?;
    let context = |column: &str, recipient: &str| {
        successor_context(&application, account_id, &id, &epoch_id, column, recipient)
    };
    let mut deliveries = Vec::with_capacity(members.len());
    for member in &members {
        let keys = state
            .keys
            .get(member)
            .ok_or(ProtocolError::Refused(RefusalCode::UnsupportedRecipient))?;
        let envelope = writer.seal(
            &keys.sealing_key(),
            &context("delivery", member)?,
            &key,
            rng,
        )?;
        deliveries.push((member.clone(), envelope));
    }
    let membership = json::encode_id_list(&members)?;
    let private_revision = json::encode_id_list(&authenticated.approval_ids)?;
    let public_revision = json::encode_revision(
        state
            .approvals
            .iter()
            .filter(|approval| approval.row.epoch_id == state.epoch_id())
            .map(|approval| &approval.row.id),
    )?;
    let verification = writer.wrap(&key, &context("verification", "")?, &Z32, rng)?;
    let history = writer.wrap(&key, &context("history", "")?, account_key, rng)?;
    let mut private = AccountSuccessor {
        id: id.clone(),
        account_id: account_id.to_owned(),
        predecessor: state.epoch_id().to_owned(),
        epoch_id: epoch_id.clone(),
        signer_id: author.id.clone(),
        action,
        removed_device_id: removed.clone(),
        retired_recovery_root_ids: retired_ids.clone(),
        membership: membership.clone(),
        revision: private_revision,
        verification,
        history,
        deliveries: json::encode_delivery_map(deliveries)?,
        signature: Vec::new(),
    };
    private.signature = writer.sign(&author.signing, &private.signed_bytes(&application)?)?;
    let mut public = PublicAccountSuccessor {
        id,
        account_id: account_id.to_owned(),
        predecessor: state.epoch_id().to_owned(),
        epoch_id: epoch_id.clone(),
        signer_id: author.id.clone(),
        action,
        removed_device_id: removed,
        retired_recovery_root_ids: retired_ids,
        membership,
        revision: public_revision,
        signature: Vec::new(),
    };
    public.signature = writer.sign(&author.signing, &public.signed_bytes(&application)?)?;
    let mut recovery_deliveries = Vec::new();
    for root in &state.recovery_roots {
        if retired.contains(&root.row.id)
            || root.row.keys.sealing_key().mechanism != writer.suite.key_seal.mechanism()
        {
            continue;
        }
        recovery_deliveries.push(recovery_delivery_for(
            writer, &root.row, &epoch_id, &key, rng,
        )?);
    }
    let account_key = sealing_key_record(writer, author, &epoch_id, &key, rng)?;
    Ok(RotationRecords {
        private,
        public,
        recovery_deliveries,
        account_key,
        key,
    })
}

/// The recovery roots a removal of `device` must also retire (§2.8): in
/// an app account, every active root whose key pair the device held, the
/// roots it registered and the roots of all its recovery approvals in this
/// generation, by position then ID. The SDK then asks for a new recovery
/// secret backup.
pub fn held_recovery_roots(state: &AccountState, device: &str) -> Vec<String> {
    if state.root.row.kind != AccountKind::App {
        return Vec::new();
    }
    state
        .recovery_roots
        .iter()
        .filter(|root| {
            root.row.signer_id == device
                || state.approvals.iter().any(|approval| {
                    approval.row.device_id == device
                        && approval.row.recovery_root_id.as_deref() == Some(root.row.id.as_str())
                })
        })
        .map(|root| root.row.id.clone())
        .collect()
}

/// A removal request for `device` itself (§2.8, stage `account-rotation`):
/// how a device leaves its account, since it never removes itself. Refused
/// unless it is active and the current epoch has another active device to
/// complete the request (`no-other-device`). The device stays an active
/// member, and keeps its keys, until another device removes it.
pub fn removal_request(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    device: &LocalDevice,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<DeviceRemovalRequest> {
    if !state.is_active(&device.id) {
        return refuse(RefusalCode::DeviceNotActive);
    }
    if !state.active.iter().any(|other| *other != device.id) {
        return refuse(RefusalCode::NoOtherDevice);
    }
    let mut request = DeviceRemovalRequest {
        id: ids::random_v4(rng)?,
        account_id: writer.account_id.to_owned(),
        epoch_id: state.epoch_id().to_owned(),
        device_id: device.id.clone(),
        signature: Vec::new(),
    };
    request.signature = writer.sign(
        &device.signing,
        &request.signed_bytes(&writer.application()?)?,
    )?;
    Ok(request)
}

/// A recovery root registration (§2.9, stage `recovery-root`), signed by an
/// active device at the current epoch.
pub fn recovery_root(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    signer: &LocalDevice,
    recovery: &RecoveryKeys,
) -> ProtocolResult<RecoveryRoot> {
    if !state.is_active(&signer.id) {
        return refuse(RefusalCode::DeviceNotActive);
    }
    let mut root = RecoveryRoot {
        id: recovery.root_id.clone(),
        account_id: writer.account_id.to_owned(),
        signer_id: signer.id.clone(),
        epoch_id: state.epoch_id().to_owned(),
        keys: KeyColumns::new(&recovery.sealing.public, &recovery.signing.public),
        signature: Vec::new(),
    };
    root.signature = writer.sign(&signer.signing, &root.signed_bytes(&writer.application()?)?)?;
    Ok(root)
}

/// A recovery delivery of the current account key to `root` (§2.9, stage
/// `recovery-delivery`).
pub fn recovery_delivery(
    writer: &AccountWriter<'_>,
    state: &AccountState,
    root: &RecoveryRoot,
    account_key: &AccountKey,
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RecoveryDelivery> {
    let account_key = current_key(state, account_key)?;
    recovery_delivery_for(writer, root, state.epoch_id(), account_key, rng)
}

fn recovery_delivery_for(
    writer: &AccountWriter<'_>,
    root: &RecoveryRoot,
    epoch_id: &str,
    key: &[u8; 32],
    rng: &mut dyn RandomSource,
) -> ProtocolResult<RecoveryDelivery> {
    let mut delivery = RecoveryDelivery {
        id: ids::random_v4(rng)?,
        root_id: root.id.clone(),
        epoch_id: epoch_id.to_owned(),
        envelope: Vec::new(),
    };
    let context = recovery_delivery_context(&writer.application()?, writer.account_id, &delivery)?;
    delivery.envelope = writer.seal(&root.keys.sealing_key(), &context, key, rng)?;
    Ok(delivery)
}
