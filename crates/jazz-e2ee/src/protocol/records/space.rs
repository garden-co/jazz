//! Space tables (`E2EE_PROTOCOL.md` §1.3, §3.1): members, roots, successors
//! and deliveries. Every context's `application` is the domain of the
//! space root's creator, `D = Dₐ(creatorAccountId)`, whoever writes the
//! record.
//!
//! Every row names its space by its address `(scopeId, identifier)`. The
//! contexts of the key records also bind the row ID of the space's root, so
//! a successor or a delivery is read against the root it extends, through a
//! [`SpaceScope`]. Which root is the space's root, which successors form its
//! chain and whether an author was active are decided over other rows
//! (§3.2, §3.3), not here.

use crate::context::CryptoContext;
use crate::envelope::{self, Mechanism};

use super::super::domain::AccountDomain;

use super::super::error::ProtocolResult;
use super::super::ids::IdForm;
use super::super::json;
use super::{
    append_framed, context, ensure, ensure_account, ensure_bytes, ensure_id, ensure_mechanism, env,
    frame, row_id,
};

const SPACE_POLICY: &str = "jazz.e2ee.space.v1";
const SPACES_TABLE: &str = "__e2ee_spaces";

/// A space's address: the catalogue identity of its space table and the
/// space row's ID. Both are UUIDs of any version.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SpaceAddress {
    pub scope_id: String,
    pub identifier: String,
}

impl SpaceAddress {
    pub fn new(scope_id: &str, identifier: &str) -> ProtocolResult<Self> {
        ensure_id(scope_id, IdForm::Any, "invalid space scope ID")?;
        ensure_id(identifier, IdForm::Any, "invalid space identifier")?;
        Ok(Self {
            scope_id: scope_id.to_owned(),
            identifier: identifier.to_owned(),
        })
    }

    fn check(&self, scope_id: &str, identifier: &str) -> ProtocolResult<()> {
        ensure(
            self.scope_id == scope_id && self.identifier == identifier,
            "record of another space",
        )
    }
}

/// What the contexts of a space's key records bind: its address and the
/// row ID of its root.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SpaceScope {
    pub address: SpaceAddress,
    pub root_id: String,
}

impl SpaceScope {
    pub fn new(scope_id: &str, identifier: &str, root_id: &str) -> ProtocolResult<Self> {
        ensure_id(root_id, IdForm::Random, "invalid space root ID")?;
        Ok(Self {
            address: SpaceAddress::new(scope_id, identifier)?,
            root_id: root_id.to_owned(),
        })
    }

    /// `X(column, recipient, epoch) = ctx{application: D, policy:
    /// "jazz.e2ee.space.v1", scope: scopeId, identifier, table:
    /// "__e2ee_spaces", row: rootId, column, epoch, recipient}`. The epoch
    /// is a version 4 or 7 UUID.
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
            scope: &self.address.scope_id,
            identifier: &self.address.identifier,
            table: SPACES_TABLE,
            row: &self.root_id,
            column,
            epoch: epoch_id,
            recipient,
        })
    }
}

/// `__e2ee_members`: `accountId` is a member of the space. Unsigned; its
/// row ID carries no meaning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceMember {
    pub id: String,
    pub scope_id: String,
    pub identifier: String,
    pub account_id: String,
}

/// `__e2ee_spaces`: a space root candidate, the first key version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRoot {
    pub id: String,
    pub scope_id: String,
    pub identifier: String,
    pub creator_account_id: String,
    pub device_id: String,
    pub account_epoch_id: String,
    pub epoch_id: String,
    /// The key wrapping mechanism of `verification`.
    pub mechanism: String,
    pub version: u32,
    pub verification: Vec<u8>,
    pub account_envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_space_successors`: the next key version, linked to the previous
/// one. `history` wraps the key of `historyEpochId`: the predecessor's,
/// except in a recovery rotation (§3.5).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceSuccessor {
    pub id: String,
    pub scope_id: String,
    pub identifier: String,
    pub predecessor: String,
    pub history_epoch_id: String,
    pub epoch_id: String,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub verification: Vec<u8>,
    pub history: Vec<u8>,
    pub account_envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

/// `__e2ee_space_deliveries`: the key of `keyEpochId` sealed to the account
/// sealing key of `recipientAccountId` for `recipientAccountEpochId`.
/// Unsigned: the server authenticates the sender, and a delivered key is
/// accepted only after it authenticates against the chain (§3.1, §3.3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceDelivery {
    pub id: String,
    pub scope_id: String,
    pub identifier: String,
    pub key_epoch_id: String,
    pub sender_account_id: String,
    pub recipient_account_id: String,
    pub recipient_account_epoch_id: String,
    pub envelope: Vec<u8>,
}

row_id!(SpaceMember, SpaceRoot, SpaceSuccessor, SpaceDelivery);

/// A root marker is an envelope of the mechanism the root's `mechanism` and
/// `version` columns name.
fn ensure_marker_mechanism(marker: &[u8], id: &str, version: u32) -> ProtocolResult<()> {
    let matches = envelope::parse(marker)
        .is_ok_and(|(mechanism, _)| mechanism == Mechanism::new(id, version));
    ensure(matches, "marker is not of the root's mechanism")
}

impl SpaceMember {
    pub fn address(&self) -> ProtocolResult<SpaceAddress> {
        SpaceAddress::new(&self.scope_id, &self.identifier)
    }

    pub fn validate(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid member row ID")?;
        self.address()?;
        ensure_account(&self.account_id, "invalid member account ID")
    }
}

impl SpaceRoot {
    pub fn address(&self) -> ProtocolResult<SpaceAddress> {
        SpaceAddress::new(&self.scope_id, &self.identifier)
    }

    /// The scope this root's records bind: its address and its own ID.
    pub fn scope(&self) -> ProtocolResult<SpaceScope> {
        SpaceScope::new(&self.scope_id, &self.identifier, &self.id)
    }

    /// What the root's contexts bind: the address, the root ID, the creator
    /// and its account epoch, the device, the root epoch and the marker's
    /// mechanism.
    fn coordinates(&self) -> ProtocolResult<SpaceScope> {
        let scope = self.scope()?;
        ensure_account(&self.creator_account_id, "invalid creator account ID")?;
        ensure_id(&self.device_id, IdForm::Random, "invalid creator device ID")?;
        ensure_id(
            &self.account_epoch_id,
            IdForm::Random,
            "invalid creator account epoch ID",
        )?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_mechanism(&self.mechanism, self.version)?;
        Ok(scope)
    }

    /// Everything the signature covers: the coordinates, a marker that is
    /// an envelope of the root's own mechanism, and the creator's copy.
    fn check_unsigned(&self) -> ProtocolResult<SpaceScope> {
        let scope = self.coordinates()?;
        ensure_marker_mechanism(&self.verification, &self.mechanism, self.version)?;
        ensure_bytes(&self.account_envelope, "empty account envelope")?;
        Ok(scope)
    }

    /// A complete received root.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure_bytes(&self.signature, "empty space root signature")
    }

    /// `frame[X(json(["root", creatorAccountId, deviceId, accountEpochId]),
    /// "", epochId), env(mechanism, verification), accountEnvelope]`.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        let scope = self.check_unsigned()?;
        let column = json::strings(&[
            "root",
            &self.creator_account_id,
            &self.device_id,
            &self.account_epoch_id,
        ]);
        frame(&[
            &scope.context(application, &column, "", &self.epoch_id)?,
            &env(&self.mechanism, self.version, &self.verification)?,
            &self.account_envelope,
        ])
    }

    /// The marker's context: `X("verification", "", epochId)`.
    pub fn marker_context(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.coordinates()?
            .context(application, "verification", "", &self.epoch_id)
    }

    /// The creator's copy: `X("account-envelope", json([creatorAccountId,
    /// accountEpochId]), epochId)`, sealed to the creator account's sealing
    /// key of `accountEpochId`.
    pub fn account_envelope_context(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        let recipient = json::strings(&[&self.creator_account_id, &self.account_epoch_id]);
        self.coordinates()?
            .context(application, "account-envelope", &recipient, &self.epoch_id)
    }
}

impl SpaceSuccessor {
    pub fn address(&self) -> ProtocolResult<SpaceAddress> {
        SpaceAddress::new(&self.scope_id, &self.identifier)
    }

    /// What the successor's contexts bind: every ID is of its form, the new
    /// epoch differs from the predecessor and from the history epoch, and
    /// the successor names `scope`'s space.
    fn coordinates(&self, scope: &SpaceScope) -> ProtocolResult<()> {
        self.address()?;
        scope.address.check(&self.scope_id, &self.identifier)?;
        for id in [
            &self.id,
            &self.predecessor,
            &self.history_epoch_id,
            &self.epoch_id,
        ] {
            ensure_id(id, IdForm::Random, "invalid successor ID")?;
        }
        ensure(
            self.epoch_id != self.predecessor && self.epoch_id != self.history_epoch_id,
            "successor repeats an earlier epoch",
        )?;
        ensure_account(&self.author_account_id, "invalid author account ID")?;
        ensure_id(
            &self.author_device_id,
            IdForm::Random,
            "invalid author device ID",
        )?;
        ensure_id(
            &self.author_epoch_id,
            IdForm::Random,
            "invalid author account epoch ID",
        )
    }

    /// Everything the signature covers.
    fn check_unsigned(&self, scope: &SpaceScope) -> ProtocolResult<()> {
        self.coordinates(scope)?;
        ensure_bytes(&self.verification, "empty successor marker")?;
        ensure_bytes(&self.history, "empty successor history")?;
        ensure_bytes(&self.account_envelope, "empty account envelope")
    }

    /// A complete received successor of `scope`'s space.
    pub fn validate(&self, scope: &SpaceScope) -> ProtocolResult<()> {
        self.check_unsigned(scope)?;
        ensure_bytes(&self.signature, "empty successor signature")
    }

    /// `X(json(["successor", id, role]), recipient, epochId)`.
    fn role_context(
        &self,
        application: &AccountDomain,
        scope: &SpaceScope,
        role: &str,
        recipient: &str,
    ) -> ProtocolResult<Vec<u8>> {
        let column = json::strings(&["successor", &self.id, role]);
        scope.context(application, &column, recipient, &self.epoch_id)
    }

    /// `X(json(["successor", id, "signature"]), json([authorAccountId,
    /// authorDeviceId, authorEpochId]), epochId) ‖ frame[predecessor,
    /// historyEpochId, verification, history, accountEnvelope]`.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        scope: &SpaceScope,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned(scope)?;
        let author = json::strings(&[
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        ]);
        append_framed(
            self.role_context(application, scope, "signature", &author)?,
            &[
                self.predecessor.as_bytes(),
                self.history_epoch_id.as_bytes(),
                &self.verification,
                &self.history,
                &self.account_envelope,
            ],
        )
    }

    /// The marker's context: `X("verification", "", epochId)`.
    pub fn marker_context(
        &self,
        application: &AccountDomain,
        scope: &SpaceScope,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(scope)?;
        scope.context(application, "verification", "", &self.epoch_id)
    }

    /// The history link: `X(json(["successor", id, "history"]),
    /// historyEpochId, epochId)`, so the version it opens cannot be
    /// substituted.
    pub fn history_context(
        &self,
        application: &AccountDomain,
        scope: &SpaceScope,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(scope)?;
        self.role_context(application, scope, "history", &self.history_epoch_id)
    }

    /// The author's copy: `X(json(["successor", id, "account-envelope"]),
    /// json([authorAccountId, authorEpochId]), epochId)`, sealed to the
    /// author account's sealing key of `authorEpochId`.
    pub fn account_envelope_context(
        &self,
        application: &AccountDomain,
        scope: &SpaceScope,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(scope)?;
        let recipient = json::strings(&[&self.author_account_id, &self.author_epoch_id]);
        self.role_context(application, scope, "account-envelope", &recipient)
    }
}

impl SpaceDelivery {
    pub fn address(&self) -> ProtocolResult<SpaceAddress> {
        SpaceAddress::new(&self.scope_id, &self.identifier)
    }

    /// What the delivery's context binds; the envelope is sealed with that
    /// context, so it is not required here.
    fn coordinates(&self, scope: &SpaceScope) -> ProtocolResult<()> {
        self.address()?;
        scope.address.check(&self.scope_id, &self.identifier)?;
        ensure_id(&self.id, IdForm::Random, "invalid delivery ID")?;
        ensure_id(&self.key_epoch_id, IdForm::Random, "invalid key epoch ID")?;
        ensure_account(&self.sender_account_id, "invalid sender account ID")?;
        ensure_account(&self.recipient_account_id, "invalid recipient account ID")?;
        ensure_id(
            &self.recipient_account_epoch_id,
            IdForm::Random,
            "invalid recipient account epoch ID",
        )
    }

    /// A complete received delivery of `scope`'s space.
    pub fn validate(&self, scope: &SpaceScope) -> ProtocolResult<()> {
        self.coordinates(scope)?;
        ensure_bytes(&self.envelope, "empty delivery envelope")
    }

    /// The context the envelope is sealed with: `X(json(["delivery", id,
    /// senderAccountId]), json([recipientAccountId,
    /// recipientAccountEpochId]), keyEpochId)`.
    pub fn seal_context(
        &self,
        application: &AccountDomain,
        scope: &SpaceScope,
    ) -> ProtocolResult<Vec<u8>> {
        self.coordinates(scope)?;
        let column = json::strings(&["delivery", &self.id, &self.sender_account_id]);
        let recipient =
            json::strings(&[&self.recipient_account_id, &self.recipient_account_epoch_id]);
        scope.context(application, &column, &recipient, &self.key_epoch_id)
    }
}
