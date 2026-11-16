//! Group tables (`E2EE_PROTOCOL.md` §1.3, §3.1). Contexts use
//! `D = Dₐ(group root accountId)`, except the public-key record and rotation
//! requests, which use the shared domain `Dₛ`.

use crate::context::CryptoContext;

use super::super::domain::{AccountDomain, SharedDomain};

use super::super::error::ProtocolResult;
use super::super::ids::IdForm;
use super::super::json;
use super::space::{
    ensure_author, ensure_marker_mechanism, ensure_mechanism, ensure_nonempty, ensure_recipient,
    triple,
};
use super::{
    KeyColumns, MemberKind, Operation, Role, append_framed, context, ensure, ensure_account,
    ensure_id, env, frame, row_id,
};

const GROUP_POLICY: &str = "jazz.e2ee.group.v1";

/// `G(column, recipient)` at `epoch`: `ctx{application: D, policy:
/// "jazz.e2ee.group.v1", scope: "group", identifier: groupId, table:
/// "__e2ee_groups", row: groupId, column, epoch, recipient}`, with `epoch`
/// always the record's own `epochId`.
pub fn group_context(
    application: &AccountDomain,
    group_id: &str,
    column: &str,
    recipient: &str,
    epoch_id: &str,
) -> ProtocolResult<Vec<u8>> {
    ensure_id(group_id, IdForm::Random, "invalid group ID")?;
    ensure_id(epoch_id, IdForm::Random, "invalid epoch ID")?;
    context(CryptoContext {
        application: application.as_str(),
        policy: GROUP_POLICY,
        scope: "group",
        identifier: group_id,
        table: "__e2ee_groups",
        row: group_id,
        column,
        epoch: epoch_id,
        recipient,
    })
}

/// A group epoch's marker context, `G("verification")` at that epoch.
pub fn group_marker_context(
    application: &AccountDomain,
    group_id: &str,
    epoch_id: &str,
) -> ProtocolResult<Vec<u8>> {
    group_context(application, group_id, "verification", "", epoch_id)
}

/// `__e2ee_groups`: the group root; its ID is random and opaque.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRoot {
    pub id: String,
    pub account_id: String,
    pub device_id: String,
    pub account_epoch_id: String,
    pub epoch_id: String,
    pub mechanism: String,
    pub version: u32,
    pub verification: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_bindings` (row ID = group ID): the link to the governing
/// application row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupBinding {
    pub id: String,
    pub group_id: String,
    pub scope_id: String,
    pub identifier: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_public_keys`: one epoch's public keys, chained to the
/// previous epoch. The root epoch's record has the group ID as its ID and
/// empty predecessor, proof epoch and predecessor signature.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupPublicKey {
    pub id: String,
    pub group_id: String,
    pub epoch_id: String,
    pub predecessor_epoch_id: String,
    pub proof_epoch_id: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub keys: KeyColumns,
    pub key_signature: Vec<u8>,
    pub predecessor_signature: Vec<u8>,
    pub author_signature: Vec<u8>,
}

/// `__e2ee_group_membership`: a signed membership change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupMembership {
    pub id: String,
    pub group_id: String,
    pub epoch_id: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub operation: Operation,
    pub role: Role,
    pub member_kind: MemberKind,
    pub member_id: String,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_successors`: a group rotation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupSuccessor {
    pub id: String,
    pub group_id: String,
    pub predecessor: String,
    pub proof_epoch_id: String,
    pub epoch_id: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub commitment: Vec<u8>,
    pub verification: Vec<u8>,
    pub history: Vec<u8>,
    pub author_envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_deliveries`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupDelivery {
    pub id: String,
    pub group_id: String,
    pub epoch_id: String,
    pub sender_account_id: String,
    pub sender_device_id: String,
    pub sender_epoch_id: String,
    pub recipient_account_id: String,
    pub recipient_device_id: String,
    pub recipient_epoch_id: String,
    pub envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_child_deliveries`: the parent epoch key sealed to a child
/// group's epoch public key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupChildDelivery {
    pub id: String,
    pub group_id: String,
    pub epoch_id: String,
    pub sender_account_id: String,
    pub sender_device_id: String,
    pub sender_epoch_id: String,
    pub recipient_group_id: String,
    pub recipient_group_epoch_id: String,
    pub envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_recovery_deliveries`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRecoveryDelivery {
    pub id: String,
    pub group_id: String,
    pub epoch_id: String,
    pub sender_account_id: String,
    pub sender_device_id: String,
    pub sender_epoch_id: String,
    pub recipient_account_id: String,
    pub recovery_root_id: String,
    pub recipient_epoch_id: String,
    pub envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_group_repairs`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GroupRepair {
    pub id: String,
    pub group_id: String,
    pub epoch_id: String,
    pub delivery_id: String,
    pub account_id: String,
    pub device_id: String,
    pub account_epoch_id: String,
    pub signature: Vec<u8>,
}

super::string_enum!(
    /// A rotation request's `causeKind`.
    CauseKind { Remove => "remove", Rotation => "rotation", Coverage => "coverage" }
);

/// `__e2ee_rotation_requests`. `covers` holds an ID list's canonical text;
/// a `remove` request has empty `source_epoch_id` and `covers`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RotationRequest {
    pub id: String,
    pub source_group_id: String,
    pub source_epoch_id: String,
    pub cause_kind: CauseKind,
    pub cause_id: String,
    pub covers: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub signature: Vec<u8>,
}

row_id!(
    GroupRoot,
    GroupBinding,
    GroupPublicKey,
    GroupMembership,
    GroupSuccessor,
    GroupDelivery,
    GroupChildDelivery,
    GroupRecoveryDelivery,
    GroupRepair,
    RotationRequest,
);

impl GroupRoot {
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// Everything the signatures cover; signatures need not exist yet.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid group ID")?;
        ensure_author(&self.account_id, &self.device_id, &self.account_epoch_id)?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_mechanism(&self.mechanism, self.version)?;
        ensure_marker_mechanism(&self.verification, &self.mechanism, self.version)
    }

    /// `frame[G(json(["root", accountId, deviceId, accountEpochId])),
    /// env(mechanism, verification)]` at the root epoch.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&[
            "root",
            &self.account_id,
            &self.device_id,
            &self.account_epoch_id,
        ]);
        frame(&[
            &group_context(application, &self.id, &column, "", &self.epoch_id)?,
            &env(&self.mechanism, self.version, &self.verification)?,
        ])
    }
}

impl GroupBinding {
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// Everything the signatures cover; signatures need not exist yet.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.group_id, IdForm::Random, "invalid group ID")?;
        ensure(self.id == self.group_id, "binding ID is not its group ID")?;
        ensure_id(&self.scope_id, IdForm::Any, "invalid binding scope ID")?;
        ensure_id(&self.identifier, IdForm::Any, "invalid binding identifier")?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )?;
        Ok(())
    }

    /// `ctx{application: D, policy: "jazz.e2ee.group-binding.v1", scope:
    /// "group", identifier: groupId, table: "__e2ee_group_bindings", row:
    /// groupId, column: json([scopeId, identifier]), epoch: root epochId,
    /// recipient: json([authorAccountId, authorDeviceId, authorEpochId])}`,
    /// unframed. The root epoch comes from the group root.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        root_epoch_id: &str,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        ensure_id(root_epoch_id, IdForm::Random, "invalid epoch ID")?;
        let column = json::strings(&[&self.scope_id, &self.identifier]);
        let recipient = triple(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        );
        context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.group-binding.v1",
            scope: "group",
            identifier: &self.group_id,
            table: "__e2ee_group_bindings",
            row: &self.group_id,
            column: &column,
            epoch: root_epoch_id,
            recipient: &recipient,
        })
    }
}

impl GroupPublicKey {
    /// Whether this is the root epoch's record.
    pub fn is_root(&self) -> bool {
        self.predecessor_epoch_id.is_empty()
    }

    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.key_signature, "empty key signature")?;
        ensure_nonempty(&self.author_signature, "empty author signature")?;
        // The root epoch has no predecessor to sign with.
        ensure(
            self.is_root() == self.predecessor_signature.is_empty(),
            "predecessor signature does not match the record's place in the chain",
        )
    }

    /// Everything the signatures cover; signatures need not exist yet.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.group_id, IdForm::Random, "invalid group ID")?;
        ensure_id(&self.id, IdForm::Random, "invalid public key ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )?;
        self.keys.validate()?;
        if self.is_root() {
            ensure(
                self.id == self.group_id && self.proof_epoch_id.is_empty(),
                "invalid root public key record",
            )
        } else {
            ensure_id(
                &self.predecessor_epoch_id,
                IdForm::Random,
                "invalid predecessor epoch ID",
            )?;
            ensure_id(
                &self.proof_epoch_id,
                IdForm::Random,
                "invalid proof epoch ID",
            )?;
            ensure(
                self.id != self.group_id && self.epoch_id != self.predecessor_epoch_id,
                "invalid successor public key record",
            )
        }
    }

    /// `P = ctx{application: Dₛ, policy: "jazz.e2ee.group-public-key.v1",
    /// scope: "group", identifier: groupId, table:
    /// "__e2ee_group_public_keys", row: id, column: json([predecessorEpochId,
    /// proofEpochId, authorAccountId, authorDeviceId, authorEpochId]), epoch:
    /// epochId} ‖ frame[env(mechanism, publicKey), env(signingMechanism,
    /// signingPublicKey)]`, signed three times.
    pub fn signed_bytes(&self, shared_domain: &SharedDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&[
            &self.predecessor_epoch_id,
            &self.proof_epoch_id,
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        ]);
        let prefix = context(CryptoContext {
            application: shared_domain.as_str(),
            policy: "jazz.e2ee.group-public-key.v1",
            scope: "group",
            identifier: &self.group_id,
            table: "__e2ee_group_public_keys",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: "",
        })?;
        append_framed(
            prefix,
            &[
                &self.keys.sealing_envelope()?,
                &self.keys.signing_envelope()?,
            ],
        )
    }
}

impl GroupMembership {
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// Everything the signatures cover; signatures need not exist yet.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::RandomOrDerived, "invalid membership ID")?;
        ensure_id(&self.group_id, IdForm::Random, "invalid group ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )?;
        match self.member_kind {
            MemberKind::Account => ensure_account(&self.member_id, "empty member account ID")?,
            MemberKind::Group => {
                ensure_id(&self.member_id, IdForm::Random, "invalid member group ID")?
            }
        }
        Ok(())
    }

    /// `ctx{policy: "jazz.e2ee.group-membership.v1", scope: "group",
    /// identifier: groupId, table: "__e2ee_group_membership", row: id,
    /// column: json([operation, role, authorAccountId, authorDeviceId,
    /// authorEpochId]), epoch, recipient: json([memberKind, memberId])}`,
    /// unframed.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&[
            self.operation.as_str(),
            self.role.as_str(),
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        ]);
        let recipient = json::strings(&[self.member_kind.as_str(), &self.member_id]);
        context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.group-membership.v1",
            scope: "group",
            identifier: &self.group_id,
            table: "__e2ee_group_membership",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: &recipient,
        })
    }
}

impl GroupSuccessor {
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// What the successor's contexts bind.
    fn coordinates(&self) -> ProtocolResult<()> {
        for id in [
            &self.id,
            &self.group_id,
            &self.predecessor,
            &self.proof_epoch_id,
            &self.epoch_id,
        ] {
            ensure_id(id, IdForm::Random, "invalid successor ID")?;
        }
        ensure(
            self.predecessor != self.epoch_id && self.id != self.group_id,
            "successor repeats its predecessor or its group ID",
        )?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )
    }

    /// Everything the signature covers.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        self.coordinates()?;
        ensure(self.commitment.len() == 32, "commitment is not 32 bytes")?;
        ensure_nonempty(&self.verification, "empty successor marker")?;
        ensure_nonempty(&self.history, "empty successor history")?;
        ensure_nonempty(&self.author_envelope, "empty author envelope")?;
        Ok(())
    }

    /// The successor context with `column`: `ctx{policy:
    /// "jazz.e2ee.group-successor.v1", scope: "group", identifier: groupId,
    /// table: "__e2ee_group_successors", row: id, column, epoch: new,
    /// recipient: json([authorAccountId, authorDeviceId, authorEpochId])}`.
    /// `history` and `authorEnvelope` use columns `"history"` and
    /// `"author-envelope"`.
    pub fn context(&self, application: &AccountDomain, column: &str) -> ProtocolResult<Vec<u8>> {
        self.coordinates()?;
        let recipient = triple(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        );
        context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.group-successor.v1",
            scope: "group",
            identifier: &self.group_id,
            table: "__e2ee_group_successors",
            row: &self.id,
            column,
            epoch: &self.epoch_id,
            recipient: &recipient,
        })
    }

    /// `<context "signature"> ‖ frame[predecessor, proofEpochId, commitment,
    /// verification, history, authorEnvelope]`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        append_framed(
            self.context(application, "signature")?,
            &[
                self.predecessor.as_bytes(),
                self.proof_epoch_id.as_bytes(),
                &self.commitment,
                &self.verification,
                &self.history,
                &self.author_envelope,
            ],
        )
    }
}

macro_rules! group_delivery_bytes {
    ($row:ty, $label:literal, |$d:ident| [$($recipient:expr),+], $validate:block) => {
        impl $row {
            /// A complete received delivery.
            pub fn validate(&self) -> ProtocolResult<()> {
                self.coordinates()?;
                ensure_nonempty(&self.envelope, "empty delivery envelope")?;
                ensure_nonempty(&self.signature, "empty delivery signature")
            }

            /// What the delivery's context binds; the envelope is sealed with
            /// that context, so it is not required here.
            fn coordinates(&self) -> ProtocolResult<()> {
                let $d = self;
                ensure_id(&$d.group_id, IdForm::Random, "invalid group ID")?;
                ensure_id(&$d.epoch_id, IdForm::Random, "invalid epoch ID")?;
                ensure_author(&$d.sender_account_id, &$d.sender_device_id, &$d.sender_epoch_id)?;
                $validate
                Ok(())
            }

            /// The context the envelope is sealed with: `G(json([label, id,
            /// senderAccountId, senderDeviceId, senderEpochId]), json([…]))`.
            pub fn seal_context(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
                self.coordinates()?;
                let $d = self;
                let column = json::strings(&[
                    $label,
                    &$d.id,
                    &$d.sender_account_id,
                    &$d.sender_device_id,
                    &$d.sender_epoch_id,
                ]);
                let recipient = json::strings(&[$($recipient),+]);
                group_context(application, &$d.group_id, &column, &recipient, &$d.epoch_id)
            }

            /// `frame[<seal context>, envelope]`.
            pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
                ensure_nonempty(&self.envelope, "empty delivery envelope")?;
                frame(&[&self.seal_context(application)?, &self.envelope])
            }
        }
    };
}

group_delivery_bytes!(
    GroupDelivery,
    "delivery",
    |d| [
        &d.recipient_account_id,
        &d.recipient_device_id,
        &d.recipient_epoch_id
    ],
    {
        ensure_id(&d.id, IdForm::RandomOrDerived, "invalid delivery ID")?;
        ensure_account(&d.recipient_account_id, "empty recipient account ID")?;
        ensure_id(
            &d.recipient_device_id,
            IdForm::Random,
            "invalid recipient device ID",
        )?;
        ensure_id(
            &d.recipient_epoch_id,
            IdForm::Random,
            "invalid recipient epoch ID",
        )?;
    }
);

group_delivery_bytes!(
    GroupChildDelivery,
    "child-delivery",
    |d| [&d.recipient_group_id, &d.recipient_group_epoch_id],
    {
        ensure_id(&d.id, IdForm::Random, "invalid delivery ID")?;
        ensure_recipient(
            MemberKind::Group,
            &d.recipient_group_id,
            &d.recipient_group_epoch_id,
        )?;
    }
);

impl GroupRecoveryDelivery {
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.envelope, "empty delivery envelope")?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// What the delivery's context binds; the envelope is sealed with that
    /// context, so it is not required here.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid delivery ID")?;
        ensure_id(&self.group_id, IdForm::Random, "invalid group ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_author(
            &self.sender_account_id,
            &self.sender_device_id,
            &self.sender_epoch_id,
        )?;
        ensure_account(&self.recipient_account_id, "empty recipient account ID")?;
        ensure_id(
            &self.recovery_root_id,
            IdForm::RandomOrDerived,
            "invalid recovery root ID",
        )?;
        ensure_id(
            &self.recipient_epoch_id,
            IdForm::Random,
            "invalid recipient epoch ID",
        )?;
        Ok(())
    }

    /// `ctx{policy: "jazz.e2ee.group-recovery.v1", scope: "group",
    /// identifier: groupId, table: "__e2ee_group_recovery_deliveries", row:
    /// id, column: json([senderAccountId, senderDeviceId, senderEpochId]),
    /// epoch, recipient: json([recipientAccountId, recoveryRootId,
    /// recipientEpochId])}`, the context the envelope is sealed with.
    pub fn seal_context(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = triple(
            &self.sender_account_id,
            &self.sender_device_id,
            &self.sender_epoch_id,
        );
        let recipient = json::strings(&[
            &self.recipient_account_id,
            &self.recovery_root_id,
            &self.recipient_epoch_id,
        ]);
        context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.group-recovery.v1",
            scope: "group",
            identifier: &self.group_id,
            table: "__e2ee_group_recovery_deliveries",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: &recipient,
        })
    }

    /// `frame[<seal context>, envelope]`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        ensure_nonempty(&self.envelope, "empty delivery envelope")?;
        frame(&[&self.seal_context(application)?, &self.envelope])
    }
}

impl GroupRepair {
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// Everything the signatures cover; signatures need not exist yet.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid repair ID")?;
        ensure_id(&self.group_id, IdForm::Random, "invalid group ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_id(
            &self.delivery_id,
            IdForm::RandomOrDerived,
            "invalid delivery ID",
        )?;
        ensure_author(&self.account_id, &self.device_id, &self.account_epoch_id)?;
        Ok(())
    }

    /// `ctx{policy: "jazz.e2ee.group-repair.v1", scope: "group", identifier:
    /// groupId, table: "__e2ee_group_repairs", row: id, column:
    /// json([deliveryId, accountId, deviceId]), epoch, recipient:
    /// accountEpochId}`, unframed.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&[&self.delivery_id, &self.account_id, &self.device_id]);
        context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.group-repair.v1",
            scope: "group",
            identifier: &self.group_id,
            table: "__e2ee_group_repairs",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: &self.account_epoch_id,
        })
    }
}

impl RotationRequest {
    /// A `remove` request names a membership record and has no source epoch
    /// and no covers; a `rotation` or `coverage` request names an epoch of the
    /// source group and carries a canonical ID list of covered requests.
    /// A complete received record.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty signature")?;
        Ok(())
    }

    /// Everything the signatures cover; signatures need not exist yet.
    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid rotation request ID")?;
        ensure_id(
            &self.source_group_id,
            IdForm::Random,
            "invalid source group ID",
        )?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )?;
        match self.cause_kind {
            CauseKind::Remove => {
                ensure_id(&self.cause_id, IdForm::RandomOrDerived, "invalid cause ID")?;
                ensure(
                    self.source_epoch_id.is_empty() && self.covers.is_empty(),
                    "remove request with a source epoch or covers",
                )
            }
            CauseKind::Rotation | CauseKind::Coverage => {
                ensure_id(&self.cause_id, IdForm::Random, "invalid cause ID")?;
                ensure_id(
                    &self.source_epoch_id,
                    IdForm::Random,
                    "invalid source epoch ID",
                )?;
                json::decode_id_list(self.covers.as_bytes()).map(|_| ())
            }
        }
    }

    /// `ctx{application: Dₛ, policy: "jazz.e2ee.rotation-request.v1", scope:
    /// "group", identifier: sourceGroupId, table: "__e2ee_rotation_requests",
    /// row: id, column: json([causeKind, causeId, sourceEpochId,
    /// authorAccountId, authorDeviceId, authorEpochId]), epoch:
    /// authorEpochId} ‖ frame[covers]`. `covers` is framed rather than put in
    /// the column, so a long list is not bound by a context field's limit.
    pub fn signed_bytes(&self, shared_domain: &SharedDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&[
            self.cause_kind.as_str(),
            &self.cause_id,
            &self.source_epoch_id,
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        ]);
        let prefix = context(CryptoContext {
            application: shared_domain.as_str(),
            policy: "jazz.e2ee.rotation-request.v1",
            scope: "group",
            identifier: &self.source_group_id,
            table: "__e2ee_rotation_requests",
            row: &self.id,
            column: &column,
            epoch: &self.author_epoch_id,
            recipient: "",
        })?;
        append_framed(prefix, &[self.covers.as_bytes()])
    }

    /// The covered request IDs.
    pub fn covered(&self) -> ProtocolResult<Vec<String>> {
        if self.covers.is_empty() {
            return Ok(Vec::new());
        }
        json::decode_id_list(self.covers.as_bytes())
    }
}
