//! Account and device tables (`E2EE_PROTOCOL.md` §1.2) and the contexts and
//! signed bytes of §2.3–§2.9. Every context's `application` is the subject
//! account's domain `Dₐ(account)`.

use crate::context::CryptoContext;

use super::super::domain::AccountDomain;

use super::super::error::ProtocolResult;
use super::super::ids::{self, IdForm};
use super::super::json;
use super::{KeyColumns, append_framed, context, ensure, ensure_account, ensure_id, frame, row_id};

/// The only ledger version this client supports; any other is an
/// unsupported capability.
pub const LEDGER_VERSION: u32 = 1;

const IDENTITY_POLICY: &str = "jazz.e2ee.account-identity.v1";
const APPROVAL_POLICY: &str = "jazz.e2ee.device-approval.v1";
const PUBLIC_APPROVAL_POLICY: &str = "jazz.e2ee.public-device-approval.v1";
const RECOVERY_APPROVAL_POLICY: &str = "jazz.e2ee.recovery-device-approval.v1";
const SUCCESSOR_POLICY: &str = "jazz.e2ee.account-successor.v1";
const PUBLIC_SUCCESSOR_POLICY: &str = "jazz.e2ee.public-account-successor.v1";
const RECOVERY_POLICY: &str = "jazz.e2ee.recovery.v1";
const SCOPE: &str = "account";

/// `__e2ee_device_requests`: a device's identity; row ID = device ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRequest {
    pub id: String,
    pub keys: KeyColumns,
}

/// `__e2ee_device_keys`: the public projection of a request's keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceKeysRow {
    pub id: String,
    pub device_id: String,
    pub keys: KeyColumns,
}

/// `__e2ee_account_identities`: the private account root. Its ID is the
/// account ID at generation 0 and the derived reset identity otherwise.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountIdentity {
    pub id: String,
    pub ledger_version: u32,
    pub generation: u64,
    pub device_id: String,
    pub epoch_id: String,
    pub envelope: Vec<u8>,
    pub verification: Vec<u8>,
}

/// `__e2ee_account_roots`: the public, unsigned account root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRoot {
    pub id: String,
    pub ledger_version: u32,
    pub generation: u64,
    pub account_id: String,
    pub device_id: String,
    pub epoch_id: String,
}

/// `__e2ee_device_challenges`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceChallenge {
    pub id: String,
    pub device_id: String,
    pub epoch_id: String,
    pub envelope: Vec<u8>,
}

/// `__e2ee_device_proofs`; row ID = challenge ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceProof {
    pub id: String,
    pub challenge_id: String,
    pub proof: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_device_approvals`; row ID = challenge ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceApproval {
    pub id: String,
    pub challenge_id: String,
    pub signer_id: String,
    pub verification: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_device_deliveries`; row ID = challenge ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceDelivery {
    pub id: String,
    pub challenge_id: String,
    pub envelope: Vec<u8>,
    pub verification: Vec<u8>,
}

/// `__e2ee_public_device_approvals`: the key-free membership grant. A
/// recovery approval carries `recovery_root_id` and `recovery_signature`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicDeviceApproval {
    pub id: String,
    pub account_id: String,
    pub epoch_id: String,
    pub device_id: String,
    pub signer_id: String,
    pub signature: Vec<u8>,
    pub recovery_root_id: Option<String>,
    pub recovery_signature: Option<Vec<u8>>,
}

super::string_enum!(
    /// An account successor's `action`.
    AccountAction { RemoveDevice => "remove-device", RetireRecoveryRoot => "retire-recovery-root" }
);

/// `__e2ee_public_account_successors`: the key-free signed rotation. Its
/// `revision` lists public approval IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAccountSuccessor {
    pub id: String,
    pub account_id: String,
    pub predecessor: String,
    pub epoch_id: String,
    pub signer_id: String,
    pub action: AccountAction,
    pub removed_device_id: Option<String>,
    pub retired_recovery_root_id: Option<String>,
    pub membership: Vec<u8>,
    pub revision: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_account_successors`: the private rotation with the new key's
/// deliveries. Its `revision` lists private approval IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSuccessor {
    pub id: String,
    pub account_id: String,
    pub predecessor: String,
    pub epoch_id: String,
    pub signer_id: String,
    pub action: AccountAction,
    pub removed_device_id: Option<String>,
    pub retired_recovery_root_id: Option<String>,
    pub membership: Vec<u8>,
    pub revision: Vec<u8>,
    pub verification: Vec<u8>,
    pub history: Vec<u8>,
    pub deliveries: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_recovery_roots`: a registered recovery credential.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryRoot {
    pub id: String,
    pub account_id: String,
    pub signer_id: String,
    pub epoch_id: String,
    pub keys: KeyColumns,
    pub signature: Vec<u8>,
}

/// `__e2ee_recovery_deliveries`: the account epoch key sealed to a root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryDelivery {
    pub id: String,
    pub root_id: String,
    pub epoch_id: String,
    pub envelope: Vec<u8>,
}

row_id!(
    DeviceRequest,
    DeviceKeysRow,
    AccountIdentity,
    AccountRoot,
    DeviceChallenge,
    DeviceProof,
    DeviceApproval,
    DeviceDelivery,
    PublicDeviceApproval,
    PublicAccountSuccessor,
    AccountSuccessor,
    RecoveryRoot,
    RecoveryDelivery,
);

fn ensure_bytes(value: &[u8], what: &'static str) -> ProtocolResult<()> {
    ensure(!value.is_empty(), what)
}

impl DeviceRequest {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid device ID")?;
        self.keys.validate()
    }
}

impl DeviceKeysRow {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid device keys ID")?;
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")?;
        self.keys.validate()
    }

    /// Whether this projection equals `request`'s keys (R4).
    pub fn matches(&self, request: &DeviceRequest) -> bool {
        self.device_id == request.id && self.keys == request.keys
    }
}

impl AccountIdentity {
    /// The ID must be the identity ID of `account_id` at this generation.
    pub fn validate(&self, account_id: &str) -> ProtocolResult<()> {
        ensure_account(account_id, "empty account ID")?;
        ensure(
            self.id == ids::account_identity_id(account_id, self.generation),
            "identity ID does not match its generation",
        )?;
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_bytes(&self.envelope, "empty identity envelope")?;
        ensure_bytes(&self.verification, "empty identity marker")
    }
}

impl AccountRoot {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid account root ID")?;
        ensure_account(&self.account_id, "empty account ID")?;
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")
    }

    /// Whether `identity` is this root's private half: same generation,
    /// device, epoch and ledger version, with the identity ID of that
    /// generation.
    pub fn matches_identity(&self, identity: &AccountIdentity) -> bool {
        identity.id == ids::account_identity_id(&self.account_id, self.generation)
            && identity.generation == self.generation
            && identity.device_id == self.device_id
            && identity.epoch_id == self.epoch_id
            && identity.ledger_version == self.ledger_version
    }
}

impl DeviceChallenge {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid challenge ID")?;
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_bytes(&self.envelope, "empty challenge envelope")
    }

    pub fn coordinates(&self) -> ChallengeCoordinates<'_> {
        ChallengeCoordinates {
            id: &self.id,
            device_id: &self.device_id,
            epoch_id: &self.epoch_id,
        }
    }
}

impl DeviceProof {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid proof ID")?;
        ensure(
            self.id == self.challenge_id,
            "proof ID is not its challenge ID",
        )?;
        ensure_bytes(&self.proof, "empty proof")?;
        ensure_bytes(&self.signature, "empty proof signature")
    }
}

impl DeviceApproval {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid approval ID")?;
        ensure(
            self.id == self.challenge_id,
            "approval ID is not its challenge ID",
        )?;
        ensure_id(&self.signer_id, IdForm::Random, "invalid signer ID")?;
        ensure_bytes(&self.verification, "empty approval marker")?;
        ensure_bytes(&self.signature, "empty approval signature")
    }
}

impl DeviceDelivery {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid delivery ID")?;
        ensure(
            self.id == self.challenge_id,
            "delivery ID is not its challenge ID",
        )?;
        ensure_bytes(&self.envelope, "empty delivery envelope")?;
        ensure_bytes(&self.verification, "empty delivery marker")
    }
}

impl PublicDeviceApproval {
    /// A complete received approval: a recovery approval has both recovery
    /// columns, any other approval neither, and every signature is present.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty approval signature")?;
        match (&self.recovery_root_id, &self.recovery_signature) {
            (None, None) => Ok(()),
            (Some(_), Some(signature)) => ensure_bytes(signature, "empty recovery signature"),
            _ => ensure(false, "recovery approval columns do not match"),
        }
    }

    /// What the signatures cover: IDs are version 4 or 7 (a recovery
    /// root's also 8), and a recovery approval has `signerId = deviceId`.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_account(&self.account_id, "empty account ID")?;
        for id in [&self.id, &self.epoch_id, &self.device_id, &self.signer_id] {
            ensure_id(id, IdForm::Random, "invalid public approval ID")?;
        }
        if let Some(root) = &self.recovery_root_id {
            ensure_id(root, IdForm::RandomOrDerived, "invalid recovery root ID")?;
            ensure(
                self.signer_id == self.device_id,
                "recovery approval signer is not the device",
            )?;
        }
        Ok(())
    }

    pub fn is_recovery(&self) -> bool {
        self.recovery_root_id.is_some()
    }

    /// The signed bytes, the same for the device's and the recovery root's
    /// signature (§2.5, §2.9): `ctx{application: D, policy:
    /// "jazz.e2ee.public-device-approval.v1", scope: "account", identifier:
    /// account, table: "__e2ee_public_device_approvals", row: id, column:
    /// "approval-signature:" ‖ signerId, epoch, recipient: deviceId}`; a
    /// recovery approval uses policy `"jazz.e2ee.recovery-device-approval.v1"`
    /// and column `"recovery-signature:" ‖ rootId`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let (policy, column) = match &self.recovery_root_id {
            Some(root) => (
                RECOVERY_APPROVAL_POLICY,
                format!("recovery-signature:{root}"),
            ),
            None => (
                PUBLIC_APPROVAL_POLICY,
                format!("approval-signature:{}", self.signer_id),
            ),
        };
        context(CryptoContext {
            application: application.as_str(),
            policy,
            scope: SCOPE,
            identifier: &self.account_id,
            table: "__e2ee_public_device_approvals",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: &self.device_id,
        })
    }
}

/// The target of a successor: exactly the column `action` names is set.
fn successor_target<'a>(
    action: AccountAction,
    removed_device_id: &'a Option<String>,
    retired_recovery_root_id: &'a Option<String>,
) -> ProtocolResult<&'a str> {
    match (action, removed_device_id, retired_recovery_root_id) {
        (AccountAction::RemoveDevice, Some(device), None) => {
            ensure_id(device, IdForm::Random, "invalid removed device ID")?;
            Ok(device)
        }
        (AccountAction::RetireRecoveryRoot, None, Some(root)) => {
            ensure_id(root, IdForm::RandomOrDerived, "invalid retired root ID")?;
            Ok(root)
        }
        _ => Err(super::super::error::ProtocolError::InvalidRecord(
            "successor target does not match its action",
        )),
    }
}

/// Checks the columns both successor tables share and returns the target
/// and the decoded membership.
#[allow(clippy::too_many_arguments)]
fn successor_header<'a>(
    id: &str,
    account_id: &str,
    predecessor: &str,
    epoch_id: &str,
    signer_id: &str,
    action: AccountAction,
    removed_device_id: &'a Option<String>,
    retired_recovery_root_id: &'a Option<String>,
    membership: &[u8],
) -> ProtocolResult<(&'a str, Vec<String>)> {
    ensure_account(account_id, "empty account ID")?;
    for value in [id, predecessor, epoch_id, signer_id] {
        ensure_id(value, IdForm::Random, "invalid successor ID")?;
    }
    ensure(predecessor != epoch_id, "successor repeats its predecessor")?;
    let target = successor_target(action, removed_device_id, retired_recovery_root_id)?;
    let members = json::decode_id_list(membership)?;
    ensure(
        action != AccountAction::RemoveDevice || !members.iter().any(|member| member == target),
        "removed device remains a member",
    )?;
    Ok((target, members))
}

impl PublicAccountSuccessor {
    /// The target ID; malformed columns are invalid.
    pub fn target_id(&self) -> ProtocolResult<&str> {
        successor_target(
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_id,
        )
    }

    /// A complete received successor; returns the decoded membership.
    pub fn validate(&self) -> ProtocolResult<Vec<String>> {
        let members = self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty successor signature")?;
        Ok(members)
    }

    fn check_unsigned(&self) -> ProtocolResult<Vec<String>> {
        let (_, members) = successor_header(
            &self.id,
            &self.account_id,
            &self.predecessor,
            &self.epoch_id,
            &self.signer_id,
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_id,
            &self.membership,
        )?;
        json::decode_revision(&self.revision)?;
        Ok(members)
    }

    /// `ctx{application: D, policy: "jazz.e2ee.public-account-successor.v1",
    /// scope: "account", identifier: account, table:
    /// "__e2ee_public_account_successors", row: id, column: "signature",
    /// epoch: newEpoch, recipient: signerId} ‖ frame[predecessor, action,
    /// targetId, membership, revision]` (§2.8).
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let target = self.target_id()?;
        let prefix = context(CryptoContext {
            application: application.as_str(),
            policy: PUBLIC_SUCCESSOR_POLICY,
            scope: SCOPE,
            identifier: &self.account_id,
            table: "__e2ee_public_account_successors",
            row: &self.id,
            column: "signature",
            epoch: &self.epoch_id,
            recipient: &self.signer_id,
        })?;
        append_framed(
            prefix,
            &[
                self.predecessor.as_bytes(),
                self.action.as_str().as_bytes(),
                target.as_bytes(),
                &self.membership,
                &self.revision,
            ],
        )
    }
}

impl AccountSuccessor {
    pub fn target_id(&self) -> ProtocolResult<&str> {
        successor_target(
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_id,
        )
    }

    /// The private `revision` is an ID list of private approval (challenge)
    /// IDs, and `deliveries` names exactly the members. Returns the decoded
    /// membership.
    pub fn validate(&self) -> ProtocolResult<Vec<String>> {
        let members = self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty successor signature")?;
        Ok(members)
    }

    fn check_unsigned(&self) -> ProtocolResult<Vec<String>> {
        let (_, members) = successor_header(
            &self.id,
            &self.account_id,
            &self.predecessor,
            &self.epoch_id,
            &self.signer_id,
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_id,
            &self.membership,
        )?;
        json::decode_id_list(&self.revision)?;
        let deliveries = json::decode_delivery_map(&self.deliveries)?;
        ensure(
            deliveries.len() == members.len()
                && deliveries
                    .iter()
                    .zip(&members)
                    .all(|((device, _), member)| device == member),
            "successor deliveries do not match its membership",
        )?;
        ensure_bytes(&self.verification, "empty successor marker")?;
        ensure_bytes(&self.history, "empty successor history")?;
        Ok(members)
    }

    /// The private signed bytes (§2.8): `S("signature", signerId) ‖
    /// frame[predecessor, action, targetId, membership, revision,
    /// verification, history, deliveries]`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let target = self.target_id()?;
        let prefix = successor_context(
            application,
            &self.account_id,
            &self.id,
            &self.epoch_id,
            "signature",
            &self.signer_id,
        )?;
        append_framed(
            prefix,
            &[
                self.predecessor.as_bytes(),
                self.action.as_str().as_bytes(),
                target.as_bytes(),
                &self.membership,
                &self.revision,
                &self.verification,
                &self.history,
                &self.deliveries,
            ],
        )
    }

    /// Whether `public` is this successor's public row: same ID, account,
    /// predecessor, epoch, signer, action, target and membership (§2.4).
    pub fn matches_public(&self, public: &PublicAccountSuccessor) -> bool {
        self.id == public.id
            && self.account_id == public.account_id
            && self.predecessor == public.predecessor
            && self.epoch_id == public.epoch_id
            && self.signer_id == public.signer_id
            && self.action == public.action
            && self.removed_device_id == public.removed_device_id
            && self.retired_recovery_root_id == public.retired_recovery_root_id
            && self.membership == public.membership
    }
}

impl RecoveryRoot {
    /// A complete received root.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty recovery root signature")
    }

    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(
            &self.id,
            IdForm::RandomOrDerived,
            "invalid recovery root ID",
        )?;
        ensure_account(&self.account_id, "empty account ID")?;
        ensure_id(&self.signer_id, IdForm::Random, "invalid signer ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        self.keys.validate()?;
        Ok(())
    }

    /// `frame[ctx{application: D, policy: "jazz.e2ee.recovery.v1", scope:
    /// "account", identifier: account, table: "__e2ee_recovery_roots", row:
    /// rootId, column: json(["root", signerId]), epoch},
    /// env(signingMechanism, signingPublicKey), env(mechanism, publicKey)]`
    /// (§2.9).
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&["root", &self.signer_id]);
        let prefix = context(CryptoContext {
            application: application.as_str(),
            policy: RECOVERY_POLICY,
            scope: SCOPE,
            identifier: &self.account_id,
            table: "__e2ee_recovery_roots",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: "",
        })?;
        frame(&[
            &prefix,
            &self.keys.signing_envelope()?,
            &self.keys.sealing_envelope()?,
        ])
    }
}

impl RecoveryDelivery {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid recovery delivery ID")?;
        ensure_id(
            &self.root_id,
            IdForm::RandomOrDerived,
            "invalid recovery root ID",
        )?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_bytes(&self.envelope, "empty recovery envelope")
    }
}

/// `A(recipient, column)` of §2.3: `ctx{application: D, policy:
/// "jazz.e2ee.account-identity.v1", scope: "account", identifier: account,
/// table: "__e2ee_account_identities", row: identity ID, column, epoch,
/// recipient}`. The identity ID is the account ID at generation 0 (§2.11).
pub fn identity_context(
    application: &AccountDomain,
    account_id: &str,
    identity_id: &str,
    epoch_id: &str,
    column: &str,
    recipient: &str,
) -> ProtocolResult<Vec<u8>> {
    context(CryptoContext {
        application: application.as_str(),
        policy: IDENTITY_POLICY,
        scope: SCOPE,
        identifier: account_id,
        table: "__e2ee_account_identities",
        row: identity_id,
        column,
        epoch: epoch_id,
        recipient,
    })
}

/// The coordinates a challenge binds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChallengeCoordinates<'a> {
    pub id: &'a str,
    pub device_id: &'a str,
    pub epoch_id: &'a str,
}

/// `P(column)` of §2.5: `ctx{application: D, policy:
/// "jazz.e2ee.device-approval.v1", scope: "account", identifier: account,
/// table: "__e2ee_device_challenges", row: challengeId, column, epoch:
/// challenge.epochId, recipient: challenge.deviceId}`.
pub fn approval_context(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    column: &str,
) -> ProtocolResult<Vec<u8>> {
    context(CryptoContext {
        application: application.as_str(),
        policy: APPROVAL_POLICY,
        scope: SCOPE,
        identifier: account_id,
        table: "__e2ee_device_challenges",
        row: challenge.id,
        column,
        epoch: challenge.epoch_id,
        recipient: challenge.device_id,
    })
}

/// The proof signature's bytes: `P("proof-signature") ‖ frame[proof]`.
pub fn proof_signed_bytes(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    proof: &[u8],
) -> ProtocolResult<Vec<u8>> {
    append_framed(
        approval_context(application, account_id, challenge, "proof-signature")?,
        &[proof],
    )
}

/// The private approval's bytes: `P("approval-signature:" ‖ signerId) ‖
/// frame[verification]`.
pub fn approval_signed_bytes(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    signer_id: &str,
    verification: &[u8],
) -> ProtocolResult<Vec<u8>> {
    let column = format!("approval-signature:{signer_id}");
    append_framed(
        approval_context(application, account_id, challenge, &column)?,
        &[verification],
    )
}

/// The context of a delivery's marker: `P("delivery-verification") ‖
/// frame[E]`, with `E` the delivery envelope.
pub fn delivery_marker_context(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    envelope: &[u8],
) -> ProtocolResult<Vec<u8>> {
    append_framed(
        approval_context(application, account_id, challenge, "delivery-verification")?,
        &[envelope],
    )
}

/// `S(column, recipient)` of §2.8: `ctx{application: D, policy:
/// "jazz.e2ee.account-successor.v1", scope: "account", identifier: account,
/// table: "__e2ee_account_successors", row: successorId, column, epoch:
/// newEpoch, recipient}`.
pub fn successor_context(
    application: &AccountDomain,
    account_id: &str,
    successor_id: &str,
    new_epoch_id: &str,
    column: &str,
    recipient: &str,
) -> ProtocolResult<Vec<u8>> {
    context(CryptoContext {
        application: application.as_str(),
        policy: SUCCESSOR_POLICY,
        scope: SCOPE,
        identifier: account_id,
        table: "__e2ee_account_successors",
        row: successor_id,
        column,
        epoch: new_epoch_id,
        recipient,
    })
}

/// `R` of §2.9: `ctx{application: D, policy: "jazz.e2ee.recovery.v1",
/// scope: "account", identifier: account, table:
/// "__e2ee_recovery_deliveries", row: deliveryId, column: "envelope", epoch,
/// recipient: rootId}`.
pub fn recovery_delivery_context(
    application: &AccountDomain,
    account_id: &str,
    delivery: &RecoveryDelivery,
) -> ProtocolResult<Vec<u8>> {
    context(CryptoContext {
        application: application.as_str(),
        policy: RECOVERY_POLICY,
        scope: SCOPE,
        identifier: account_id,
        table: "__e2ee_recovery_deliveries",
        row: &delivery.id,
        column: "envelope",
        epoch: &delivery.epoch_id,
        recipient: &delivery.root_id,
    })
}
