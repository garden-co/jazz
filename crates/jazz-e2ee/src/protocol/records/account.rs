//! Account and device tables (`E2EE_PROTOCOL.md` §1.2) and the contexts and
//! signed bytes of §2.3–§2.9 and §2.12. Every context's `application` is
//! the subject account's domain `Dₐ(account)`.

use crate::agility::{MechanismId, PublicKey};
use crate::context::CryptoContext;

use super::super::domain::AccountDomain;

use super::super::error::ProtocolResult;
use super::super::ids::IdForm;
use super::super::json;
use super::{
    KeyColumns, append_framed, context, ensure, ensure_account, ensure_bytes, ensure_id,
    ensure_mechanism, env, frame, row_id,
};

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
const LOCAL_FIRST_ROOT_POLICY: &str = "jazz.e2ee.local-first-account-root.v1";
const ACCOUNT_KEY_POLICY: &str = "jazz.e2ee.account-key.v1";
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
    AccountKind { LocalFirst => "local-first", App => "app" }
);

/// `__e2ee_account_roots`: the public account root. A `local-first` root
/// carries the local-first public key that derives the account ID and that
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
    pub local_first_public_key: Option<Vec<u8>>,
    pub local_first_signature: Option<Vec<u8>>,
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
    /// An ID list (§0) of recovery root IDs: the one root a
    /// `retire-recovery-root` successor retires, or every root a
    /// `remove-device` successor also retires (possibly none, `[]`).
    pub retired_recovery_root_ids: Vec<u8>,
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
    /// An ID list (§0) of recovery root IDs: the one root a
    /// `retire-recovery-root` successor retires, or every root a
    /// `remove-device` successor also retires (possibly none, `[]`).
    pub retired_recovery_root_ids: Vec<u8>,
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
    AccountSealingKey,
);

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
    /// A complete received root: a `local-first` root has a 32-byte
    /// public key and a signature, an `app` root neither.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        match &self.local_first_signature {
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
            &self.local_first_public_key,
            &self.local_first_signature,
        ) {
            (AccountKind::App, None, None) => Ok(()),
            (AccountKind::LocalFirst, Some(key), Some(_)) => ensure(
                key.len() == super::super::local_first::PUBLIC_KEY_BYTES,
                "invalid local-first public key",
            ),
            _ => ensure(false, "account root columns do not match its kind"),
        }
    }

    /// The bytes a `local-first` root's signature covers (§2.3):
    /// `ctx{application: D, policy: "jazz.e2ee.local-first-account-root.v1",
    /// scope: "account", identifier: account, table: "__e2ee_account_roots",
    /// row: rootId, column: "local-first-signature", epoch: epochId,
    /// recipient: deviceId} ‖ frame[decimal generation, decimal
    /// ledgerVersion, localFirstPublicKey, env(signingMechanism,
    /// signingPublicKey), env(mechanism, publicKey)]`, with the root
    /// device's key columns. An `app` root has none.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        device_keys: &KeyColumns,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let public_key = self.local_first_public_key.as_deref().ok_or(
            super::super::error::ProtocolError::InvalidRecord("an app account root is not signed"),
        )?;
        device_keys.validate()?;
        let prefix = context(CryptoContext {
            application: application.as_str(),
            policy: LOCAL_FIRST_ROOT_POLICY,
            scope: SCOPE,
            identifier: &self.account_id,
            table: "__e2ee_account_roots",
            row: &self.id,
            column: "local-first-signature",
            epoch: &self.epoch_id,
            recipient: &self.device_id,
        })?;
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

/// Checks the columns both successor tables share and returns the target
/// and the decoded membership.
#[allow(clippy::too_many_arguments)]
fn successor_header(
    id: &str,
    account_id: &str,
    predecessor: &str,
    epoch_id: &str,
    signer_id: &str,
    action: AccountAction,
    removed_device_id: &Option<String>,
    retired_recovery_root_ids: &[u8],
    membership: &[u8],
) -> ProtocolResult<(String, Vec<String>)> {
    ensure_account(account_id, "invalid account ID")?;
    for value in [id, predecessor, epoch_id, signer_id] {
        ensure_id(value, IdForm::Random, "invalid successor ID")?;
    }
    ensure(predecessor != epoch_id, "successor repeats its predecessor")?;
    let (target, _) = successor_target(action, removed_device_id, retired_recovery_root_ids)?;
    let members = json::decode_id_list(membership)?;
    ensure(
        action != AccountAction::RemoveDevice || !members.contains(&target),
        "removed device remains a member",
    )?;
    Ok((target, members))
}

impl PublicAccountSuccessor {
    /// The target ID; malformed columns are invalid.
    pub fn target_id(&self) -> ProtocolResult<String> {
        Ok(self.targets()?.0)
    }

    /// The target ID and, for a removal, the recovery roots it also
    /// retires.
    pub fn targets(&self) -> ProtocolResult<(String, Vec<String>)> {
        successor_target(
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_ids,
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
            &self.retired_recovery_root_ids,
            &self.membership,
        )?;
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
        let (target, _) = self.targets()?;
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
                &self.retired_recovery_root_ids,
                &self.membership,
                &self.revision,
            ],
        )
    }
}

impl AccountSuccessor {
    pub fn target_id(&self) -> ProtocolResult<String> {
        Ok(self.targets()?.0)
    }

    /// The target ID and, for a removal, the recovery roots it also
    /// retires.
    pub fn targets(&self) -> ProtocolResult<(String, Vec<String>)> {
        successor_target(
            self.action,
            &self.removed_device_id,
            &self.retired_recovery_root_ids,
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
            &self.retired_recovery_root_ids,
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
    /// frame[predecessor, action, targetId, retiredRecoveryRootIds, membership,
    /// revision, verification, history, deliveries]`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let (target, _) = self.targets()?;
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
                &self.retired_recovery_root_ids,
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
            && self.retired_recovery_root_ids == public.retired_recovery_root_ids
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

impl AccountSealingKey {
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
        let prefix = context(CryptoContext {
            application: application.as_str(),
            policy: ACCOUNT_KEY_POLICY,
            scope: SCOPE,
            identifier: &self.account_id,
            table: "__e2ee_account_keys",
            row: &self.id,
            column: &self.signer_id,
            epoch: &self.epoch_id,
            recipient: "",
        })?;
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
