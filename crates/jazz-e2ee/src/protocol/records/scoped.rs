//! Records with the same shape for spaces and groups: notices
//! (`__e2ee_space_notices`, `__e2ee_group_notices`, §2.11) and key requests
//! (`__e2ee_key_requests`, §3.4). Contexts use the creator's domain `D`.

use crate::context::CryptoContext;

use super::super::domain::AccountDomain;

use super::super::error::ProtocolResult;
use super::super::ids::{self, IdForm};
use super::super::json;
use super::group::group_context;
use super::space::{SpaceAddress, ensure_author};
use super::{NoticeKind, ScopeKind, context, ensure, ensure_id, row_id};

/// Whose notice: a space (by its address) or a group.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NoticeScope<'a> {
    Space(&'a SpaceAddress),
    Group(&'a str),
}

impl NoticeScope<'_> {
    fn id(&self) -> &str {
        match self {
            Self::Space(address) => &address.space_id,
            Self::Group(group_id) => group_id,
        }
    }
}

/// A reset approval or admission notice. `scope_id` is the `spaceId` or
/// `groupId` column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Notice {
    pub id: String,
    pub scope_id: String,
    pub epoch_id: String,
    pub kind: NoticeKind,
    pub account_id: String,
    pub generation: u64,
    pub author_account_id: String,
    pub author_device_id: String,
    pub author_epoch_id: String,
    pub signature: Vec<u8>,
}

/// `__e2ee_key_requests`: a manager recipient's request for the current key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyRequest {
    pub id: String,
    pub scope_kind: ScopeKind,
    pub scope_id: String,
    pub epoch_id: String,
    pub account_id: String,
    pub device_id: String,
    pub account_epoch_id: String,
    pub signature: Vec<u8>,
}

row_id!(Notice, KeyRequest);

impl Notice {
    /// The derived ID this notice must have.
    pub fn expected_id(
        scope_id: &str,
        kind: NoticeKind,
        account_id: &str,
        generation: u64,
    ) -> String {
        match kind {
            NoticeKind::ResetApproved => ids::reset_notice_id(scope_id, account_id, generation),
            NoticeKind::Admitted => ids::admission_notice_id(scope_id, account_id, generation),
        }
    }

    /// A complete received notice.
    pub fn validate(&self, scope: NoticeScope<'_>) -> ProtocolResult<()> {
        self.check_unsigned(scope)?;
        ensure(!self.signature.is_empty(), "empty notice signature")
    }

    /// The ID is derived from the scope, kind, account and generation. Both
    /// kinds name a generation above 0 (generation 0 needs no approval and
    /// no notice), and a reset approval is never by the approved account
    /// itself (§2.11).
    fn check_unsigned(&self, scope: NoticeScope<'_>) -> ProtocolResult<()> {
        if let NoticeScope::Group(group_id) = scope {
            ensure_id(group_id, IdForm::Random, "invalid notice group ID")?;
        }
        ensure(
            self.scope_id == scope.id(),
            "notice of another space or group",
        )?;
        ensure(!self.account_id.is_empty(), "empty notice account ID")?;
        ensure(
            self.id
                == Self::expected_id(&self.scope_id, self.kind, &self.account_id, self.generation),
            "notice ID does not match its derivation",
        )?;
        ensure(self.generation > 0, "notice of generation 0")?;
        ensure(
            self.kind != NoticeKind::ResetApproved || self.author_account_id != self.account_id,
            "reset approval by the approved account",
        )?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_author(
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        )?;
        Ok(())
    }

    /// `X(json(["notice", id, kind, authorAccountId, authorDeviceId,
    /// authorEpochId]), json([accountId, generation]), epochId)` for a space,
    /// and `G` with the same column, recipient and epoch for a group.
    pub fn signed_bytes(
        &self,
        application: &AccountDomain,
        scope: NoticeScope<'_>,
    ) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned(scope)?;
        let column = json::strings(&[
            "notice",
            &self.id,
            self.kind.as_str(),
            &self.author_account_id,
            &self.author_device_id,
            &self.author_epoch_id,
        ]);
        let recipient = json::strings(&[&self.account_id, &self.generation.to_string()]);
        match scope {
            NoticeScope::Space(address) => {
                address.context(application, &column, &recipient, &self.epoch_id)
            }
            NoticeScope::Group(group_id) => {
                group_context(application, group_id, &column, &recipient, &self.epoch_id)
            }
        }
    }
}

impl KeyRequest {
    /// A complete received request.
    pub fn validate(&self) -> ProtocolResult<()> {
        self.check_unsigned()?;
        ensure(!self.signature.is_empty(), "empty key request signature")
    }

    fn check_unsigned(&self) -> ProtocolResult<()> {
        ensure_id(&self.id, IdForm::Random, "invalid key request ID")?;
        ensure_id(
            &self.scope_id,
            IdForm::RandomOrDerived,
            "invalid key request scope ID",
        )?;
        ensure(
            self.scope_kind != ScopeKind::Group || ids::is_id(&self.scope_id, IdForm::Random),
            "invalid key request group ID",
        )?;
        ensure(
            self.scope_kind != ScopeKind::Space || ids::is_id(&self.scope_id, IdForm::Derived),
            "invalid key request space ID",
        )?;
        ensure_id(&self.epoch_id, IdForm::Random, "invalid epoch ID")?;
        ensure_author(&self.account_id, &self.device_id, &self.account_epoch_id)?;
        Ok(())
    }

    /// `ctx{application: D, policy: "jazz.e2ee.key-request.v1", scope:
    /// scopeKind, identifier: scopeId, table: "__e2ee_key_requests", row: id,
    /// column: json([accountId, deviceId]), epoch: epochId, recipient:
    /// accountEpochId}`, unframed.
    pub fn signed_bytes(&self, application: &AccountDomain) -> ProtocolResult<Vec<u8>> {
        self.check_unsigned()?;
        let column = json::strings(&[&self.account_id, &self.device_id]);
        context(CryptoContext {
            application: application.as_str(),
            policy: "jazz.e2ee.key-request.v1",
            scope: self.scope_kind.as_str(),
            identifier: &self.scope_id,
            table: "__e2ee_key_requests",
            row: &self.id,
            column: &column,
            epoch: &self.epoch_id,
            recipient: &self.account_epoch_id,
        })
    }

    /// The ID of the one delivery that answers this request.
    pub fn answer_id(&self) -> String {
        ids::key_request_answer_id(&self.id)
    }
}
