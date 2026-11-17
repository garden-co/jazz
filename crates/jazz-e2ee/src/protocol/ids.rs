//! Identifiers (`E2EE_PROTOCOL.md` §0, "Identifiers").
//!
//! Record, epoch, device and recovery root IDs are lowercase UUIDs, random
//! at versions 4 or 7. The one derived form is the version 8 ID of a
//! recovery root derived from a recovery secret (§2.9), keyed BLAKE2b from
//! the recovery seed, which only the account can recompute
//! ([`super::derive::RecoveryKeys`]). A generation-0 account identity's ID
//! is its account ID. Account IDs and the space address are UUIDs of any
//! version.

/// Which UUID versions a column accepts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IdForm {
    /// Versions 4 or 7: every ID the protocol generates at random.
    Random,
    /// Versions 4, 7 or 8: recovery root IDs, random or derived from a
    /// recovery secret.
    RandomOrDerived,
    /// Any version: account IDs, catalogue identities and application row
    /// IDs, which the protocol does not generate.
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
