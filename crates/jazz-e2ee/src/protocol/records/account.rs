//! Account and device tables (`E2EE_PROTOCOL.md` §1.2) and the contexts and
//! signed bytes of §2.3–§2.9 and §2.12. Every context's `application` is
//! the subject account's domain `Dₐ(account)`.

use crate::agility::{MechanismId, PublicKey};
use crate::context::CryptoContext;
use crate::envelope::Mechanism;

use super::super::domain::AccountDomain;

use super::super::error::ProtocolResult;
use super::super::ids::IdForm;
use super::super::json;
use super::super::position::RowId;
use super::{
    KeyColumns, append_framed, context, ensure, ensure_account, ensure_bytes, ensure_id,
    ensure_mechanism, env, frame, row_id,
};

#[allow(clippy::too_many_arguments)]
fn account_context(
    application: &AccountDomain,
    policy: &str,
    identifier: &str,
    table: &str,
    row: &str,
    column: &str,
    epoch: &str,
    recipient: &str,
) -> ProtocolResult<Vec<u8>> {
    context(CryptoContext {
        application: application.as_str(),
        policy,
        scope: SCOPE,
        identifier,
        table,
        row,
        column,
        epoch,
        recipient,
    })
}

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
const DEVICE_ACCOUNT_ROOT_POLICY: &str = "jazz.e2ee.device-account-root.v1";
const ACCOUNT_KEY_POLICY: &str = "jazz.e2ee.account-key.v1";
const REMOVAL_REQUEST_POLICY: &str = "jazz.e2ee.device-removal-request.v1";
const SCOPE: &str = "account";

/// `__e2ee_device_requests`: a device's identity; row ID = device ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRequest {
    pub id: String,
    pub keys: KeyColumns,
    /// `SHA-256(UTF8("jazz.e2ee.approval-nonce.v1\0") ‖ frame[deviceId,
    /// n_R])` of the device's approval nonce `n_R` (§2.5): committed before
    /// any challenge exists, revealed in the proof.
    pub nonce_commitment: Vec<u8>,
}

/// `__e2ee_device_keys`: the public projection of a request's keys.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceKeysRow {
    pub id: String,
    pub device_id: String,
    pub keys: KeyColumns,
}

/// `__e2ee_account_identities`: the private account root. Its ID is the
/// account ID at generation 0 and a random ID after a reset (§1.2, §2.11);
/// a generation's identity is found by its generation, device and epoch.
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

super::string_enum!(
    /// How the account signs in, as its root states it (§1.2, §2.3).
    AccountKind { DeviceAccount => "device-account", App => "app" }
);

/// `__e2ee_account_roots`: the public account root. A `device-account` root
/// carries the device-account public key that derives the account ID and that
/// key's signature over [`AccountRoot::signed_bytes`]; an `app` root carries
/// neither and is authenticated only by its author (`$createdBy`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountRoot {
    pub id: String,
    pub ledger_version: u32,
    pub generation: u64,
    pub account_id: String,
    pub device_id: String,
    pub epoch_id: String,
    pub kind: AccountKind,
    pub device_account_public_key: Option<Vec<u8>>,
    pub device_account_signature: Option<Vec<u8>>,
}

/// `__e2ee_device_challenges`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceChallenge {
    pub id: String,
    pub device_id: String,
    pub epoch_id: String,
    pub envelope: Vec<u8>,
    /// The approver's random 32-byte nonce `n_A` (§2.5).
    pub nonce: Vec<u8>,
    /// The approving device, which signs the challenge over the requester
    /// keys and nonce commitment it was shown (§2.5).
    pub signer_id: String,
    pub signature: Vec<u8>,
}

/// `__e2ee_device_proofs`; row ID = challenge ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceProof {
    pub id: String,
    pub challenge_id: String,
    pub proof: Vec<u8>,
    /// The requester's approval nonce `n_R`, revealed (§2.5).
    pub nonce: Vec<u8>,
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

/// `__e2ee_public_device_approvals`: the key-free signed approval that
/// admits the device to the account epoch. A recovery approval carries
/// `recovery_root_id` and `recovery_signature`.
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

/// The columns signed by both successor records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSuccessorColumns {
    pub id: String,
    pub account_id: String,
    pub predecessor: String,
    pub epoch_id: String,
    pub signer_id: String,
    pub action: AccountAction,
    pub removed_device_id: Option<String>,
    /// An ID list (§0) of recovery root IDs: the one root a
    /// `retire-recovery-root` successor retires, or every root a
    /// `remove-device` successor also retires (possibly none, `[]`).
    pub retired_recovery_root_ids: Vec<u8>,
    pub membership: Vec<u8>,
}

/// `__e2ee_public_account_successors`: the key-free signed rotation. Its
/// `revision` lists public approval IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PublicAccountSuccessor {
    pub columns: AccountSuccessorColumns,
    pub revision: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_account_successors`: the private rotation with the new key's
/// deliveries. Its `revision` lists private approval IDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSuccessor {
    pub columns: AccountSuccessorColumns,
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

/// `__e2ee_account_keys`: the public half of an account epoch's sealing key
/// pair for one key sealing mechanism (§2.12), signed by the device
/// `signerId`. Space keys are sealed to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountSealingKey {
    pub id: String,
    pub account_id: String,
    pub epoch_id: String,
    pub signer_id: String,
    pub mechanism: String,
    pub version: u32,
    pub public_key: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_device_removal_requests`: a device's signed request to be removed
/// from its account (§2.8). A device never removes itself; another active
/// device completes the request with a `remove-device` rotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeviceRemovalRequest {
    pub id: String,
    pub account_id: String,
    pub epoch_id: String,
    pub device_id: String,
    pub signature: Vec<u8>,
}

row_id!(
    DeviceRemovalRequest,
    DeviceRequest,
    DeviceKeysRow,
    AccountIdentity,
    AccountRoot,
    DeviceChallenge,
    DeviceProof,
    DeviceApproval,
    DeviceDelivery,
    PublicDeviceApproval,
    RecoveryRoot,
    RecoveryDelivery,
    AccountSealingKey,
);

impl RowId for PublicAccountSuccessor {
    fn row_id(&self) -> &str {
        &self.columns.id
    }
}

impl RowId for AccountSuccessor {
    fn row_id(&self) -> &str {
        &self.columns.id
    }
}

impl DeviceRequest {
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid device ID")?;
        ensure(
            self.nonce_commitment.len() == 32,
            "invalid nonce commitment",
        )?;
        self.keys.validate()
    }
}

/// The commitment to a device's approval nonce (§2.5):
/// `SHA-256(UTF8("jazz.e2ee.approval-nonce.v1\0") ‖ frame[deviceId, n_R])`.
pub fn nonce_commitment(device_id: &str, nonce: &[u8]) -> ProtocolResult<[u8; 32]> {
    let mut bytes = b"jazz.e2ee.approval-nonce.v1\0".to_vec();
    bytes.extend_from_slice(&frame(&[device_id.as_bytes(), nonce])?);
    Ok(crate::primitives::hash::sha256(&bytes))
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
    /// The ID is `account_id` at generation 0 and a version 4 or 7 UUID
    /// above it.
    pub fn validate(&self, account_id: &str) -> ProtocolResult<()> {
        ensure_account(account_id, "invalid account ID")?;
        if self.generation == 0 {
            ensure(
                self.id == account_id,
                "generation 0 identity ID is not the account ID",
            )?;
        } else {
            ensure_id(&self.id, IdForm::Random, "invalid reset identity ID")?;
        }
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_bytes(&self.envelope, "empty identity envelope")?;
        ensure_bytes(&self.verification, "empty identity marker")
    }
}

impl AccountRoot {
    /// A complete received root: a `device-account` root has a 32-byte
    /// public key and a signature, an `app` root neither.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        match &self.device_account_signature {
            Some(signature) => ensure_bytes(signature, "empty root signature"),
            None => Ok(()),
        }
    }

    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid account root ID")?;
        ensure_account(&self.account_id, "invalid account ID")?;
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        match (
            self.kind,
            &self.device_account_public_key,
            &self.device_account_signature,
        ) {
            (AccountKind::App, None, None) => Ok(()),
            (AccountKind::DeviceAccount, Some(key), Some(_)) => ensure(
                key.len() == super::super::device_account::PUBLIC_KEY_BYTES,
                "invalid device-account public key",
            ),
            _ => ensure(false, "account root columns do not match its kind"),
        }
    }

    /// The bytes a `device-account` root's signature covers (§2.3):
    /// `ctx{application: D, policy: "jazz.e2ee.device-account-root.v1",
    /// scope: "account", identifier: account, table: "__e2ee_account_roots",
    /// row: rootId, column: "device-account-signature", epoch: epochId,
    /// recipient: deviceId} ‖ frame[decimal generation, decimal
    /// ledgerVersion, deviceAccountPublicKey, env(signingMechanism,
    /// signingPublicKey), env(mechanism, publicKey)]`, with the root
    /// device's key columns. An `app` root has none.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        device_keys: &KeyColumns,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let public_key = self.device_account_public_key.as_deref().ok_or(
            super::super::error::ProtocolError::InvalidRecord("an app account root is not signed"),
        )?;
        device_keys.validate()?;
        let prefix = account_context(
            application,
            DEVICE_ACCOUNT_ROOT_POLICY,
            &self.account_id,
            "__e2ee_account_roots",
            &self.id,
            "device-account-signature",
            &self.epoch_id,
            &self.device_id,
        )?;
        append_framed(
            prefix,
            &[
                self.generation.to_string().as_bytes(),
                self.ledger_version.to_string().as_bytes(),
                public_key,
                &device_keys.signing_envelope()?,
                &device_keys.sealing_envelope()?,
            ],
        )
    }

    /// Whether `identity` is this root's private half: same generation,
    /// device, epoch and ledger version, and at generation 0 the account ID
    /// as its ID.
    pub fn matches_identity(&self, identity: &AccountIdentity) -> bool {
        (self.generation != 0 || identity.id == self.account_id)
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
        ensure_id(&self.signer_id, IdForm::Random, "invalid signer ID")?;
        ensure(self.nonce.len() == 32, "invalid challenge nonce")?;
        ensure_bytes(&self.envelope, "empty challenge envelope")?;
        ensure_bytes(&self.signature, "empty challenge signature")
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
        ensure(self.nonce.len() == 32, "invalid proof nonce")?;
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
        ensure_account(&self.account_id, "invalid account ID")?;
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
    /// "approval-signature:" ‖ signerId, epoch, recipient: deviceId} ‖
    /// frame[env(signingMechanism, signingPublicKey), env(mechanism,
    /// publicKey)]`, with the approved device's key columns, so the approval
    /// vouches for the keys and not only for the device ID; a recovery
    /// approval uses policy `"jazz.e2ee.recovery-device-approval.v1"` and
    /// column `"recovery-signature:" ‖ rootId`.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        device_keys: &KeyColumns,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        device_keys.validate()?;
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
        let prefix = account_context(
            application,
            policy,
            &self.account_id,
            "__e2ee_public_device_approvals",
            &self.id,
            &column,
            &self.epoch_id,
            &self.device_id,
        )?;
        append_framed(
            prefix,
            &[
                &device_keys.signing_envelope()?,
                &device_keys.sealing_envelope()?,
            ],
        )
    }
}

/// The targets of a successor (§2.8): a `remove-device` successor names
/// the removed device and the recovery roots it also retires (possibly
/// none); a `retire-recovery-root` successor names no device and exactly one
/// root. Returns the target and the co-retired roots.
fn successor_target(
    action: AccountAction,
    removed_device_id: &Option<String>,
    retired_recovery_root_ids: &[u8],
) -> ProtocolResult<(String, Vec<String>)> {
    let roots = json::decode_root_id_list(retired_recovery_root_ids)?;
    match (action, removed_device_id) {
        (AccountAction::RemoveDevice, Some(device)) => {
            ensure_id(device, IdForm::Random, "invalid removed device ID")?;
            Ok((device.clone(), roots))
        }
        (AccountAction::RetireRecoveryRoot, None) if roots.len() == 1 => {
            let root = roots.into_iter().next().expect("one root");
            Ok((root, Vec::new()))
        }
        _ => Err(super::super::error::ProtocolError::InvalidRecord(
            "successor target does not match its action",
        )),
    }
}

impl AccountSuccessorColumns {
    /// The target ID; malformed columns are invalid.
    pub fn target_id(&self) -> ProtocolResult<String> {
        Ok(self.targets()?.0)
    }

    /// The target ID and, for a removal, the recovery roots it also retires.
    pub fn targets(&self) -> ProtocolResult<(String, Vec<String>)> {
        successor_target(
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_ids,
        )
    }

    fn validate(&self) -> ProtocolResult<Vec<String>> {
        ensure_account(&self.account_id, "invalid account ID")?;
        let ids = [&self.id, &self.predecessor, &self.epoch_id, &self.signer_id];
        for id in ids {
            ensure_id(id, IdForm::Random, "invalid successor ID")?;
        }
        ensure(
            self.predecessor != self.epoch_id,
            "successor repeats its predecessor",
        )?;
        let target = self.target_id()?;
        let members = json::decode_id_list(&self.membership)?;
        ensure(
            self.action != AccountAction::RemoveDevice || !members.contains(&target),
            "removed device remains a member",
        )?;
        Ok(members)
    }
}

impl PublicAccountSuccessor {
    /// A complete received successor; returns the decoded membership.
    pub fn validate(&self) -> ProtocolResult<Vec<String>> {
        let members = self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty successor signature")?;
        Ok(members)
    }

    fn check_unsigned(&self) -> ProtocolResult<Vec<String>> {
        let members = self.columns.validate()?;
        json::decode_revision(&self.revision)?;
        Ok(members)
    }

    /// `ctx{application: D, policy: "jazz.e2ee.public-account-successor.v1",
    /// scope: "account", identifier: account, table:
    /// "__e2ee_public_account_successors", row: id, column: "signature",
    /// epoch: newEpoch, recipient: signerId} ‖ frame[predecessor, action,
    /// targetId, retiredRecoveryRootIds, membership, revision]` (§2.8), with
    /// the `retiredRecoveryRootIds` column's canonical bytes.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let target = self.columns.target_id()?;
        let prefix = account_context(
            application,
            PUBLIC_SUCCESSOR_POLICY,
            &self.columns.account_id,
            "__e2ee_public_account_successors",
            &self.columns.id,
            "signature",
            &self.columns.epoch_id,
            &self.columns.signer_id,
        )?;
        append_framed(
            prefix,
            &[
                self.columns.predecessor.as_bytes(),
                self.columns.action.as_str().as_bytes(),
                target.as_bytes(),
                &self.columns.retired_recovery_root_ids,
                &self.columns.membership,
                &self.revision,
            ],
        )
    }
}

impl AccountSuccessor {
    /// The private `revision` is an ID list of private approval (challenge)
    /// IDs, and `deliveries` names exactly the members. Returns the decoded
    /// membership.
    pub fn validate(&self) -> ProtocolResult<Vec<String>> {
        let members = self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty successor signature")?;
        Ok(members)
    }

    fn check_unsigned(&self) -> ProtocolResult<Vec<String>> {
        let members = self.columns.validate()?;
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
    /// frame[predecessor, action, targetId, retiredRecoveryRootIds, membership,
    /// revision, verification, history, deliveries]`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let target = self.columns.target_id()?;
        let prefix = successor_context(
            application,
            &self.columns.account_id,
            &self.columns.id,
            &self.columns.epoch_id,
            "signature",
            &self.columns.signer_id,
        )?;
        append_framed(
            prefix,
            &[
                self.columns.predecessor.as_bytes(),
                self.columns.action.as_str().as_bytes(),
                target.as_bytes(),
                &self.columns.retired_recovery_root_ids,
                &self.columns.membership,
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
        self.columns == public.columns
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
        ensure_account(&self.account_id, "invalid account ID")?;
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
        let prefix = account_context(
            application,
            RECOVERY_POLICY,
            &self.account_id,
            "__e2ee_recovery_roots",
            &self.id,
            &column,
            &self.epoch_id,
            "",
        )?;
        frame(&[
            &prefix,
            &self.keys.signing_envelope()?,
            &self.keys.sealing_envelope()?,
        ])
    }
}

impl DeviceRemovalRequest {
    /// A complete received request.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty removal request signature")
    }

    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid removal request ID")?;
        ensure_account(&self.account_id, "invalid account ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_id(&self.device_id, IdForm::Random, "invalid device ID")
    }

    /// `ctx{application: D, policy: "jazz.e2ee.device-removal-request.v1",
    /// scope: "account", identifier: account, table:
    /// "__e2ee_device_removal_requests", row: id, column: "signature", epoch,
    /// recipient: deviceId}`, signed by the device itself (§2.8).
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        account_context(
            application,
            REMOVAL_REQUEST_POLICY,
            &self.account_id,
            "__e2ee_device_removal_requests",
            &self.id,
            "signature",
            &self.epoch_id,
            &self.device_id,
        )
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

impl AccountSealingKey {
    pub fn mechanism(&self) -> Mechanism<'_> {
        Mechanism::new(&self.mechanism, self.version)
    }

    /// The published public key with its mechanism.
    pub fn public(&self) -> PublicKey {
        PublicKey {
            mechanism: MechanismId::new(self.mechanism.clone(), self.version),
            bytes: self.public_key.clone(),
        }
    }

    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty account key signature")
    }

    /// Everything the signature covers: IDs of their forms, a valid
    /// mechanism and a public key of 1 to 65,536 bytes.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid account key ID")?;
        ensure_account(&self.account_id, "invalid account ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_id(&self.signer_id, IdForm::Random, "invalid signer ID")?;
        ensure_mechanism(&self.mechanism, self.version)?;
        ensure(
            (1..=json::MAX_DELIVERY_BYTES).contains(&self.public_key.len()),
            "invalid public key length",
        )
    }

    /// `ctx{application: D, policy: "jazz.e2ee.account-key.v1", scope:
    /// "account", identifier: account, table: "__e2ee_account_keys", row:
    /// id, column: signerId, epoch: epochId} ‖ frame[env(mechanism,
    /// publicKey)]` (§2.12).
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let prefix = account_context(
            application,
            ACCOUNT_KEY_POLICY,
            &self.account_id,
            "__e2ee_account_keys",
            &self.id,
            &self.signer_id,
            &self.epoch_id,
            "",
        )?;
        append_framed(
            prefix,
            &[&env(&self.mechanism, self.version, &self.public_key)?],
        )
    }
}

/// `A(recipient, column)` of §2.3: `ctx{application: D, policy:
/// "jazz.e2ee.account-identity.v1", scope: "account", identifier: account,
/// table: "__e2ee_account_identities", row: identity ID, column, epoch,
/// recipient}`. The identity ID is the account ID at generation 0 and the
/// reset identity's random ID above it (§2.11).
pub fn identity_context(
    application: &AccountDomain,
    account_id: &str,
    identity_id: &str,
    epoch_id: &str,
    column: &str,
    recipient: &str,
) -> ProtocolResult<Vec<u8>> {
    account_context(
        application,
        IDENTITY_POLICY,
        account_id,
        "__e2ee_account_identities",
        identity_id,
        column,
        epoch_id,
        recipient,
    )
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
    account_context(
        application,
        APPROVAL_POLICY,
        account_id,
        "__e2ee_device_challenges",
        challenge.id,
        column,
        challenge.epoch_id,
        challenge.device_id,
    )
}

/// The challenge signature's bytes (§2.5): `P("challenge-signature:" ‖
/// signerId) ‖ frame[env(signingMechanism, signingPublicKey),
/// env(mechanism, publicKey), nonceCommitment, envelope, nonce]`, with the
/// requester's key columns and nonce commitment as the approver was shown
/// them, and the approver's nonce `n_A`. The requester checks it against its own
/// keys, so a server that showed the approver other keys cannot present the
/// requester a challenge for its real ones.
pub fn challenge_signed_bytes(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    signer_id: &str,
    requester: &DeviceRequest,
    envelope: &[u8],
    nonce: &[u8],
) -> ProtocolResult<Vec<u8>> {
    requester.validate()?;
    let column = format!("challenge-signature:{signer_id}");
    append_framed(
        approval_context(application, account_id, challenge, &column)?,
        &[
            &requester.keys.signing_envelope()?,
            &requester.keys.sealing_envelope()?,
            &requester.nonce_commitment,
            envelope,
            nonce,
        ],
    )
}

/// The approval comparison code (§2.5): six decimal digits, zero-padded,
/// from the first four bytes (big-endian) of `SHA-256(P("approval-code") ‖
/// frame[env(signingMechanism, signingPublicKey), env(mechanism,
/// publicKey), n_A, n_R])` modulo 10⁶, with the requester's key columns and
/// both nonces. Each side commits to its nonce before it learns the other's
/// (the requester in its request, the approver in its signed challenge), so
/// nobody between them can search for a match. The approver computes it
/// from the keys it was shown once the proof reveals `n_R`, the requester
/// from its own keys once it accepted the challenge; the user confirms they
/// match before approving.
pub fn approval_code(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    requester_keys: &KeyColumns,
    approver_nonce: &[u8],
    requester_nonce: &[u8],
) -> ProtocolResult<String> {
    requester_keys.validate()?;
    let bytes = append_framed(
        approval_context(application, account_id, challenge, "approval-code")?,
        &[
            &requester_keys.signing_envelope()?,
            &requester_keys.sealing_envelope()?,
            approver_nonce,
            requester_nonce,
        ],
    )?;
    let digest = crate::primitives::hash::sha256(&bytes);
    let value = u32::from_be_bytes([digest[0], digest[1], digest[2], digest[3]]) % 1_000_000;
    Ok(format!("{value:06}"))
}

/// The proof signature's bytes: `P("proof-signature") ‖ frame[proof,
/// nonce]`, with the requester's revealed nonce `n_R`.
pub fn proof_signed_bytes(
    application: &AccountDomain,
    account_id: &str,
    challenge: ChallengeCoordinates<'_>,
    proof: &[u8],
    nonce: &[u8],
) -> ProtocolResult<Vec<u8>> {
    append_framed(
        approval_context(application, account_id, challenge, "proof-signature")?,
        &[proof, nonce],
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
    account_context(
        application,
        SUCCESSOR_POLICY,
        account_id,
        "__e2ee_account_successors",
        successor_id,
        column,
        new_epoch_id,
        recipient,
    )
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
    account_context(
        application,
        RECOVERY_POLICY,
        account_id,
        "__e2ee_recovery_deliveries",
        &delivery.id,
        "envelope",
        &delivery.epoch_id,
        &delivery.root_id,
    )
}
