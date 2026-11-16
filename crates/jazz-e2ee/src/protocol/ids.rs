//! Identifiers (`E2EE_PROTOCOL.md` §0, "Identifiers").
//!
//! Record, epoch, device and recovery root IDs are lowercase UUIDs. Random
//! IDs are versions 4 or 7. Version 8 IDs are derived, in two families:
//!
//! - **public derived IDs**: the first 16 bytes of
//!   `SHA-256(UTF8(json([label, …])))` with the version and variant bits set,
//!   recomputable by the server from public row fields;
//! - **secret-derived recovery root IDs** (§2.9), keyed BLAKE2b from a
//!   recovery seed, which only the account can recompute
//!   ([`super::derive::RecoveryKeys`]).

use crate::primitives::hash;

use super::json;

pub const SPACE_ID_LABEL: &str = "jazz.e2ee.space-id.v1";
pub const ACCOUNT_RESET_LABEL: &str = "jazz.e2ee.account-reset.v1";
pub const RESET_NOTICE_LABEL: &str = "jazz.e2ee.reset-notice.v1";
pub const ADMISSION_NOTICE_LABEL: &str = "jazz.e2ee.admission-notice.v1";
pub const KEY_REQUEST_ANSWER_LABEL: &str = "jazz.e2ee.key-request-answer.v1";
pub const SPACE_GRANT_ID_LABEL: &str = "jazz.e2ee.space-grant-id.v1";
pub const GROUP_MEMBERSHIP_ID_LABEL: &str = "jazz.e2ee.group-membership-id.v1";

/// Which UUID versions a column accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdForm {
    /// Versions 4 or 7: every ID no derivation covers.
    Random,
    /// Versions 4, 7 or 8: recovery roots, space grants and group membership
    /// records.
    RandomOrDerived,
    /// Version 8 only: space roots, reset identities, notices and key-request
    /// answers.
    Derived,
    /// Any version: catalogue identities and application row IDs, which the
    /// protocol does not generate.
    Any,
}

/// The version nibble of a lowercase hyphenated UUID with the RFC variant
/// (`[89ab]`), or `None` for anything else.
pub fn uuid_version(value: &str) -> Option<u8> {
    let bytes = value.as_bytes();
    let shape = bytes.len() == 36
        && bytes.iter().enumerate().all(|(index, byte)| match index {
            8 | 13 | 18 | 23 => *byte == b'-',
            _ => matches!(byte, b'0'..=b'9' | b'a'..=b'f'),
        });
    if !shape || !matches!(bytes[19], b'8' | b'9' | b'a' | b'b') {
        return None;
    }
    char::from(bytes[14]).to_digit(16).map(|digit| digit as u8)
}

/// Whether `value` is a lowercase UUID of a version `form` accepts.
pub fn is_id(value: &str, form: IdForm) -> bool {
    match (form, uuid_version(value)) {
        (_, None) => false,
        (IdForm::Random, Some(version)) => matches!(version, 4 | 7),
        (IdForm::RandomOrDerived, Some(version)) => matches!(version, 4 | 7 | 8),
        (IdForm::Derived, Some(version)) => version == 8,
        (IdForm::Any, Some(_)) => true,
    }
}

/// Formats 16 bytes as a version 8 UUID: `b[6] = (b[6] & 0x0f) | 0x80`,
/// `b[8] = (b[8] & 0x3f) | 0x80`.
pub fn uuid_v8(mut bytes: [u8; 16]) -> String {
    bytes[6] = (bytes[6] & 0x0f) | 0x80;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    format_uuid(&bytes)
}

/// A public derived ID: the version 8 UUID of
/// `SHA-256(UTF8(json(parts)))[0..16]`. The first part is the label.
pub fn public_derived_id(parts: &[&str]) -> String {
    let digest = hash::sha256(json::strings(parts).as_bytes());
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    uuid_v8(bytes)
}

/// The space root ID of address `(scopeId, identifier)` (§3.2).
pub fn space_id(scope_id: &str, identifier: &str) -> String {
    public_derived_id(&[SPACE_ID_LABEL, scope_id, identifier])
}

/// The identity ID of `account` at `generation` (§1.2): the account ID for
/// generation 0, otherwise the derived reset identity.
pub fn account_identity_id(account_id: &str, generation: u64) -> String {
    if generation == 0 {
        account_id.to_owned()
    } else {
        public_derived_id(&[ACCOUNT_RESET_LABEL, account_id, &generation.to_string()])
    }
}

/// A reset approval's ID (§2.11); `scope_id` is the space or group ID.
pub fn reset_notice_id(scope_id: &str, account_id: &str, generation: u64) -> String {
    public_derived_id(&[
        RESET_NOTICE_LABEL,
        scope_id,
        account_id,
        &generation.to_string(),
    ])
}

/// An admission notice's ID (§2.11); `scope_id` is the space or group ID.
pub fn admission_notice_id(scope_id: &str, account_id: &str, generation: u64) -> String {
    public_derived_id(&[
        ADMISSION_NOTICE_LABEL,
        scope_id,
        account_id,
        &generation.to_string(),
    ])
}

/// The ID of the one delivery that answers key request `request_id` (§3.4).
pub fn key_request_answer_id(request_id: &str) -> String {
    public_derived_id(&[KEY_REQUEST_ANSWER_LABEL, request_id])
}

/// The coordinates of a staged grant or membership record (§3.8): `n` is
/// the count of accepted rows of the space (group) with the same role,
/// operation and recipient before it, whether replay applied them or not.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StagedChange<'a> {
    pub role: &'a str,
    pub operation: &'a str,
    pub kind: &'a str,
    pub recipient_id: &'a str,
    pub n: u64,
}

/// A space grant staged by the recipient sync or by self-enrolment.
pub fn space_grant_id(space_id: &str, change: StagedChange<'_>) -> String {
    staged_id(SPACE_GRANT_ID_LABEL, space_id, change)
}

/// A group membership record staged by the sync or by self-enrolment.
pub fn group_membership_id(group_id: &str, change: StagedChange<'_>) -> String {
    staged_id(GROUP_MEMBERSHIP_ID_LABEL, group_id, change)
}

fn staged_id(label: &str, scope: &str, change: StagedChange<'_>) -> String {
    public_derived_id(&[
        label,
        scope,
        change.role,
        change.operation,
        change.kind,
        change.recipient_id,
        &change.n.to_string(),
    ])
}

/// A fresh random version 4 UUID.
pub fn random_v4(rng: &mut dyn crate::RandomSource) -> crate::Result<String> {
    let mut bytes = [0u8; 16];
    rng.fill(&mut bytes)?;
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    Ok(format_uuid(&bytes))
}

pub(crate) fn format_uuid(bytes: &[u8; 16]) -> String {
    let mut out = String::with_capacity(36);
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push('-');
        }
        out.push_str(&format!("{byte:02x}"));
    }
    out
}
