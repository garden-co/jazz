//! Space tables (`E2EE_PROTOCOL.md` §1.3, §3.2). Contexts use
//! `D = Dₐ(space root creatorAccountId)`.
//!
//! Only the root carries the space's address; every other record names the
//! space by ID, so its bytes take the [`SpaceAddress`] from the root.

use crate::context::CryptoContext;

use super::super::domain::AccountDomain;

use super::super::derive;
use super::super::error::ProtocolResult;
use super::super::ids::{self, IdForm};
use super::super::json;
use super::{
    MemberKind, Operation, Role, append_framed, context, ensure, ensure_account, ensure_id, env,
    frame, row_id,
};

const SPACE_POLICY: &str = "jazz.e2ee.space.v1";

/// A space's address and its derived root ID (§3.2). All three are UUIDs.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceAddress {
    pub scope_id: String,
    pub identifier: String,
    pub space_id: String,
}

impl SpaceAddress {
    /// The address of the space `identifier` of space table `scope_id`.
    pub fn new(scope_id: &str, identifier: &str) -> ProtocolResult<Self> {
        ensure_id(scope_id, IdForm::Any, "invalid space scope ID")?;
        ensure_id(identifier, IdForm::Any, "invalid space identifier")?;
        Ok(Self {
            scope_id: scope_id.to_owned(),
            identifier: identifier.to_owned(),
            space_id: ids::space_id(scope_id, identifier),
        })
    }

    /// `X(column, recipient, epoch) = ctx{application: D, policy:
    /// "jazz.e2ee.space.v1", scope: scopeId, identifier, table:
    /// "__e2ee_spaces", row: spaceId, column, epoch, recipient}`.
    pub fn context(
        &self,
        application: &AccountDomain,
        column: &str,
        recipient: &str,
        epoch_id: &str,
    ) -> ProtocolResult<Vec<u8>> {
        ensure_id(epoch_id, IdForm::Random, "invalid epoch ID")?;
        context(CryptoContext {
            application: application.as_str(),
            policy: SPACE_POLICY,
            scope: &self.scope_id,
            identifier: &self.identifier,
            table: "__e2ee_spaces",
            row: &self.space_id,
            column,
            epoch: epoch_id,
            recipient,
        })
    }

    fn check(&self, space_id: &str) -> ProtocolResult<()> {
        ensure(space_id == self.space_id, "record of another space")
    }
}

/// `__e2ee_spaces`: the space root; its ID is derived from its address.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRoot {
    pub id: String,
    pub scope_id: String,
    pub identifier: String,
    pub creator_account_id: String,
    pub device_id: String,
    pub account_epoch_id: String,
    pub epoch_id: String,
    pub initial_grant_id: String,
    pub mechanism: String,
    pub version: u32,
    pub verification: Vec<u8>,
    pub author_envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_space_grants`: a signed recipient change.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceGrant {
    pub id: String,
    pub space_id: String,
    pub epoch_id: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub operation: Operation,
    pub role: Role,
    pub recipient_kind: MemberKind,
    pub recipient_id: String,
    pub recipient_epoch_id: String,
    pub signature: Vec<u8>,
}

/// `__e2ee_space_successors`: a space rotation; `commitment` hides the
/// recipient list.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceSuccessor {
    pub id: String,
    pub space_id: String,
    pub predecessor: String,
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

/// `__e2ee_space_successor_recipients` (row ID = successor ID), and the same
/// shape for groups.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SuccessorRecipients {
    pub id: String,
    /// The space or group ID.
    pub scope_id: String,
    pub salt: Vec<u8>,
    pub membership: Vec<u8>,
    pub revision: Vec<u8>,
}

/// `__e2ee_space_deliveries`. A key-request answer has the derived ID of its
/// request (§3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceDelivery {
    pub id: String,
    pub space_id: String,
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

/// `__e2ee_space_group_deliveries`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceGroupDelivery {
    pub id: String,
    pub space_id: String,
    pub epoch_id: String,
    pub sender_account_id: String,
    pub sender_device_id: String,
    pub sender_epoch_id: String,
    pub recipient_group_id: String,
    pub recipient_group_epoch_id: String,
    pub envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_space_recovery_deliveries`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRecoveryDelivery {
    pub id: String,
    pub space_id: String,
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

super::string_enum!(
    /// A space repair's `deliveryKind`.
    DeliveryKind { Device => "device", Recovery => "recovery" }
);

/// `__e2ee_space_repairs`: a claim that a delivery does not open.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRepair {
    pub id: String,
    pub space_id: String,
    pub epoch_id: String,
    pub delivery_kind: DeliveryKind,
    pub delivery_id: String,
    pub account_id: String,
    pub device_id: String,
    pub account_epoch_id: String,
    pub signature: Vec<u8>,
}

row_id!(
    SpaceRoot,
    SpaceGrant,
    SpaceSuccessor,
    SuccessorRecipients,
    SpaceDelivery,
    SpaceGroupDelivery,
    SpaceRecoveryDelivery,
    SpaceRepair,
);

/// `json([account, device, epoch])` of an author or sender.
pub(crate) fn triple(account: &str, device: &str, epoch: &str) -> String {
    json::strings(&[account, device, epoch])
}

pub(crate) fn ensure_author(account: &str, device: &str, epoch: &str) -> ProtocolResult<()> {
    ensure_account(account, "empty author account ID")?;
    ensure_id(device, IdForm::Random, "invalid author device ID")?;
    ensure_id(epoch, IdForm::Random, "invalid author epoch ID")
}

pub(crate) fn ensure_recipient(kind: MemberKind, id: &str, epoch: &str) -> ProtocolResult<()> {
    match kind {
        MemberKind::Account => ensure_account(id, "empty recipient account ID")?,
        MemberKind::Group => ensure_id(id, IdForm::Random, "invalid recipient group ID")?,
    }
    ensure_id(epoch, IdForm::Random, "invalid recipient epoch ID")
}

pub(crate) fn ensure_nonempty(value: &[u8], what: &'static str) -> ProtocolResult<()> {
    ensure(!value.is_empty(), what)
}

pub(crate) fn ensure_mechanism(id: &str, version: u32) -> ProtocolResult<()> {
    ensure(
        crate::envelope::Mechanism::new(id, version)
            .validate()
            .is_ok(),
        "invalid mechanism",
    )
}

/// A root marker is an envelope of the mechanism the root's `mechanism` and
/// `version` columns name.
pub(crate) fn ensure_marker_mechanism(marker: &[u8], id: &str, version: u32) -> ProtocolResult<()> {
    let matches = crate::envelope::parse(marker).is_ok_and(|(marker_id, marker_version, _)| {
        marker_id == id.as_bytes() && marker_version == version
    });
    ensure(matches, "marker is not of the root's mechanism")
}

impl SpaceRoot {
    pub fn address(&self) -> ProtocolResult<SpaceAddress> {
        SpaceAddress::new(&self.scope_id, &self.identifier)
    }

    /// The creator's initial grant ID: the derived ID of its self-enrolment
    /// as a manager, the first such grant of the space (§3.3, §3.8).
    pub fn expected_initial_grant_id(space_id: &str, creator_account_id: &str) -> String {
        ids::space_grant_id(
            space_id,
            ids::StagedChange {
                role: Role::Manager.as_str(),
                operation: Operation::Add.as_str(),
                kind: MemberKind::Account.as_str(),
                recipient_id: creator_account_id,
                n: 0,
            },
        )
    }

    /// What the root's contexts bind: the ID is the address's derived ID,
    /// the initial grant ID the creator's derived self-enrolment, the
    /// mechanism a valid one; every other ID is random.
    fn coordinates(&self) -> ProtocolResult<SpaceAddress> {
        let address = self.address()?;
        address.check(&self.id)?;
        ensure_author(
            &self.creator_account_id,
            &self.device_id,
            &self.account_epoch_id,
        )?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure(
            self.initial_grant_id
                == Self::expected_initial_grant_id(&self.id, &self.creator_account_id),
            "initial grant ID does not match its derivation",
        )?;
        ensure_mechanism(&self.mechanism, self.version)?;
        Ok(address)
    }

    /// Everything the signature covers: the coordinates, and a marker that
    /// is an envelope of the root's own mechanism, and the author's copy.
    fn check_unsigned(&self) -> ProtocolResult<SpaceAddress> {
        let address = self.coordinates()?;
        ensure_marker_mechanism(&self.verification, &self.mechanism, self.version)?;
        ensure_nonempty(&self.author_envelope, "empty author envelope")?;
        Ok(address)
    }

    /// A complete received root.
    pub fn validate(&self) -> ProtocolResult<SpaceAddress> {
        let address = self.check_unsigned()?;
        ensure_nonempty(&self.signature, "empty space root signature")?;
        Ok(address)
    }

    /// `frame[X(json(["root", creatorAccountId, deviceId, accountEpochId,
    /// initialGrantId])), env(mechanism, verification), authorEnvelope]` at
    /// the root epoch.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        let address = self.check_unsigned()?;
        let column = json::strings(&[
            "root",
            &self.creator_account_id,
            &self.device_id,
            &self.account_epoch_id,
            &self.initial_grant_id,
        ]);
        frame(&[
            &address.context(application, &column, "", &self.epoch_id)?,
            &env(&self.mechanism, self.version, &self.verification)?,
            &self.author_envelope,
        ])
    }

    /// The root marker's context, `X("verification")` at the root epoch.
    pub fn marker_context(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.coordinates()?
            .context(application, "verification", "", &self.epoch_id)
    }

    /// The creator's copy's context, `X("author", deviceId)`.
    pub fn author_envelope_context(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.coordinates()?
            .context(application, "author", &self.device_id, &self.epoch_id)
    }
}

impl SpaceGrant {
    /// A complete received grant.
    pub fn validate(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        self.check_unsigned(address)?;
        ensure_nonempty(&self.signature, "empty grant signature")
    }

    fn check_unsigned(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        address.check(&self.space_id)?;
        ensure_id(&self.id, IdForm::RandomOrDerived, "invalid grant ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )?;
        ensure_recipient(
            self.recipient_kind,
            &self.recipient_id,
            &self.recipient_epoch_id,
        )?;
        Ok(())
    }

    /// `X(json(["grant", id, authorAccountId, authorDeviceId, authorEpochId,
    /// operation, role]), json([recipientKind, recipientId,
    /// recipientEpochId]), epochId)`, unframed.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned(address)?;
        let column = json::strings(&[
            "grant",
            &self.id,
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
            self.operation.as_str(),
            self.role.as_str(),
        ]);
        let recipient = json::strings(&[
            self.recipient_kind.as_str(),
            &self.recipient_id,
            &self.recipient_epoch_id,
        ]);
        address.context(application, &column, &recipient, &self.epoch_id)
    }
}

impl SpaceSuccessor {
    /// A complete received successor.
    pub fn validate(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        self.check_unsigned(address)?;
        ensure_nonempty(&self.signature, "empty successor signature")
    }

    /// Everything the signature covers.
    fn check_unsigned(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        self.coordinates(address)?;
        ensure(self.commitment.len() == 32, "commitment is not 32 bytes")?;
        ensure_nonempty(&self.verification, "empty successor marker")?;
        ensure_nonempty(&self.history, "empty successor history")?;
        ensure_nonempty(&self.author_envelope, "empty author envelope")
    }

    /// What the successor's contexts bind.
    fn coordinates(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        address.check(&self.space_id)?;
        for id in [&self.id, &self.predecessor, &self.epoch_id] {
            ensure_id(id, IdForm::Random, "invalid successor ID")?;
        }
        ensure(
            self.predecessor != self.epoch_id,
            "successor repeats its predecessor",
        )?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )
    }

    /// `X(json(["successor", id, role]), recipient, newEpoch)`.
    fn role_context(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
        role: &str,
        recipient: &str,
    ) -> ProtocolResult<Vec<u8>> {
        let column = json::strings(&["successor", &self.id, role]);
        address.context(application, &column, recipient, &self.epoch_id)
    }

    /// `X(json(["successor", id, "signature"]), json([authorAccountId,
    /// authorDeviceId, authorEpochId]), newEpoch) ‖ frame[predecessor,
    /// commitment, verification, history, authorEnvelope]`.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned(address)?;
        let author = triple(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        );
        append_framed(
            self.role_context(application, address, "signature", &author)?,
            &[
                self.predecessor.as_bytes(),
                &self.commitment,
                &self.verification,
                &self.history,
                &self.author_envelope,
            ],
        )
    }

    /// The author's copy: `X(json(["successor", id, "author-envelope"]),
    /// authorDeviceId, newEpoch)`.
    pub fn author_envelope_context(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(address)?;
        self.role_context(
            application,
            address,
            "author-envelope",
            &self.author_device_id,
        )
    }

    /// The history link: `X(json(["successor", id, "history"]), "",
    /// newEpoch)`, with an empty recipient.
    pub fn history_context(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(address)?;
        self.role_context(application, address, "history", "")
    }

    /// The marker's context: `X("verification", "", newEpoch)`.
    pub fn marker_context(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(address)?;
        address.context(application, "verification", "", &self.epoch_id)
    }
}

impl SuccessorRecipients {
    /// A 32-byte salt, a canonical member map and a canonical revision.
    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid recipients ID")?;
        ensure_id(
            &self.scope_id,
            IdForm::RandomOrDerived,
            "invalid recipients space or group ID",
        )?;
        ensure(self.salt.len() == 32, "salt is not 32 bytes")?;
        json::decode_member_map(&self.membership)?;
        json::decode_revision(&self.revision)?;
        Ok(())
    }

    /// The commitment these recipients open (§3.1).
    pub fn commitment(&self) -> ProtocolResult<[u8; 32]> {
        self.validate()?;
        let salt: &[u8; 32] = self.salt[..].try_into().map_err(|_| {
            super::super::error::ProtocolError::InvalidRecord("salt is not 32 bytes")
        })?;
        derive::recipient_commitment(salt, &self.membership, &self.revision)
    }
}

macro_rules! delivery_bytes {
    ($row:ty, $label:literal, |$this:ident| [$($recipient:expr),+], |$v:ident| $validate:block) => {
        impl $row {
            /// A complete received delivery.
            pub fn validate(&self, address: &SpaceAddress) -> ProtocolResult<()> {
                self.coordinates(address)?;
                ensure_nonempty(&self.envelope, "empty delivery envelope")?;
                ensure_nonempty(&self.signature, "empty delivery signature")
            }

            /// What the delivery's context binds; the envelope is sealed with
            /// that context, so it is not required here.
            fn coordinates(&self, address: &SpaceAddress) -> ProtocolResult<()> {
                let $v = self;
                address.check(&$v.space_id)?;
                ensure_id(&$v.epoch_id, IdForm::Random, "invalid epoch ID")?;
                ensure_author(&$v.sender_account_id, &$v.sender_device_id, &$v.sender_epoch_id)?;
                $validate
                Ok(())
            }

            /// The context the envelope is sealed with.
            pub fn seal_context(&self, application: &AccountDomain, address: &SpaceAddress) -> ProtocolResult<Vec<u8>> {
                self.coordinates(address)?;
                let $this = self;
                let column = json::strings(&[
                    $label,
                    &$this.id,
                    &$this.sender_account_id,
                    &$this.sender_device_id,
                    &$this.sender_epoch_id,
                ]);
                let recipient = json::strings(&[$($recipient),+]);
                address.context(application, &column, &recipient, &$this.epoch_id)
            }

            /// `frame[<seal context>, envelope]`.
            pub fn signed_bytes(&self, application: &AccountDomain, address: &SpaceAddress) -> ProtocolResult<Vec<u8>> {
                ensure_nonempty(&self.envelope, "empty delivery envelope")?;
                frame(&[&self.seal_context(application, address)?, &self.envelope])
            }
        }
    };
}

delivery_bytes!(
    SpaceDelivery,
    "delivery",
    |d| [
        &d.recipient_account_id,
        &d.recipient_device_id,
        &d.recipient_epoch_id
    ],
    |d| {
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

delivery_bytes!(
    SpaceGroupDelivery,
    "group-delivery",
    |d| [&d.recipient_group_id, &d.recipient_group_epoch_id],
    |d| {
        ensure_id(&d.id, IdForm::Random, "invalid delivery ID")?;
        ensure_id(
            &d.recipient_group_id,
            IdForm::Random,
            "invalid recipient group ID",
        )?;
        ensure_id(
            &d.recipient_group_epoch_id,
            IdForm::Random,
            "invalid recipient epoch ID",
        )?;
    }
);

delivery_bytes!(
    SpaceRecoveryDelivery,
    "recovery-delivery",
    |d| [
        &d.recipient_account_id,
        &d.recovery_root_id,
        &d.recipient_epoch_id
    ],
    |d| {
        ensure_id(&d.id, IdForm::Random, "invalid delivery ID")?;
        ensure_account(&d.recipient_account_id, "empty recipient account ID")?;
        ensure_id(
            &d.recovery_root_id,
            IdForm::RandomOrDerived,
            "invalid recovery root ID",
        )?;
        ensure_id(
            &d.recipient_epoch_id,
            IdForm::Random,
            "invalid recipient epoch ID",
        )?;
    }
);

impl SpaceRepair {
    /// A complete received repair.
    pub fn validate(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        self.check_unsigned(address)?;
        ensure_nonempty(&self.signature, "empty repair signature")
    }

    fn check_unsigned(&self, address: &SpaceAddress) -> ProtocolResult<()> {
        address.check(&self.space_id)?;
        ensure_id(&self.id, IdForm::Random, "invalid repair ID")?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_id(
            &self.delivery_id,
            IdForm::RandomOrDerived,
            "invalid delivery ID",
        )?;
        ensure_author(&self.account_id, &self.device_id, &self.account_epoch_id)?;
        Ok(())
    }

    /// `X(json(["repair", id, deliveryKind, deliveryId, accountId,
    /// deviceId]), accountEpochId, epoch)`, unframed.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        address: &SpaceAddress,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned(address)?;
        let column = json::strings(&[
            "repair",
            &self.id,
            self.delivery_kind.as_str(),
            &self.delivery_id,
            &self.account_id,
            &self.device_id,
        ]);
        address.context(application, &column, &self.account_epoch_id, &self.epoch_id)
    }
}
