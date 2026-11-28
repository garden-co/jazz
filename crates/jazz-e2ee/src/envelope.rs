//! The common envelope, version 1 (`E2EE_CRYPTO_FORMAT.md`).
//!
//! `"JE2E" || 01 || id length || id || u32be version || payload`. The header
//! is routing metadata, not an authentication claim: each mechanism binds its
//! own header into its authenticated data.

use crate::error::{Error, Result};

const MAGIC: [u8; 4] = *b"JE2E";
const FORMAT: u8 = 1;
const MAX_ID_LEN: usize = 64;

/// A mechanism's stable ID and wire-format version.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Mechanism<'a> {
    pub id: &'a str,
    pub version: u32,
}

impl<'a> Mechanism<'a> {
    pub const fn new(id: &'a str, version: u32) -> Self {
        Self { id, version }
    }

    /// IDs are 1–64 characters of `[a-z0-9.-]`; versions are non-zero.
    pub fn validate(&self) -> Result<()> {
        let id_ok = (1..=MAX_ID_LEN).contains(&self.id.len())
            && self
                .id
                .bytes()
                .all(|byte| matches!(byte, b'a'..=b'z' | b'0'..=b'9' | b'.' | b'-'));
        if id_ok && self.version != 0 {
            Ok(())
        } else {
            Err(Error::InvalidInput)
        }
    }

    /// The header alone: the envelope with an empty payload.
    pub fn header(&self) -> Result<Vec<u8>> {
        encode(*self, &[])
    }

    pub(crate) fn header_len(&self) -> usize {
        10 + self.id.len()
    }
}

/// Encodes `payload` under `mechanism`'s header.
pub fn encode(mechanism: Mechanism<'_>, payload: &[u8]) -> Result<Vec<u8>> {
    mechanism.validate()?;
    let mut bytes = Vec::with_capacity(mechanism.header_len() + payload.len());
    bytes.extend_from_slice(&MAGIC);
    bytes.push(FORMAT);
    bytes.push(mechanism.id.len() as u8);
    bytes.extend_from_slice(mechanism.id.as_bytes());
    bytes.extend_from_slice(&mechanism.version.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

/// Returns the payload of an envelope for exactly `mechanism`.
///
/// A malformed or truncated header is [`Error::InvalidCiphertext`]; a
/// well-formed header for another mechanism or version is
/// [`Error::UnsupportedFormat`]. The payload is not authenticated here.
pub fn decode<'b>(mechanism: Mechanism<'_>, envelope: &'b [u8]) -> Result<&'b [u8]> {
    mechanism.validate()?;
    let (actual, payload) = parse(envelope)?;
    if actual != mechanism {
        return Err(Error::UnsupportedFormat);
    }
    Ok(payload)
}

/// Splits any well-formed envelope into its mechanism and payload.
/// A header that is malformed, including an ID outside `[a-z0-9.-]` or
/// version 0, is [`Error::InvalidCiphertext`].
pub(crate) fn parse(envelope: &[u8]) -> Result<(Mechanism<'_>, &[u8])> {
    if envelope.len() < 11 || envelope[..4] != MAGIC || envelope[4] != FORMAT {
        return Err(Error::InvalidCiphertext);
    }
    let id_len = usize::from(envelope[5]);
    if envelope.len() < 10 + id_len {
        return Err(Error::InvalidCiphertext);
    }
    let id =
        core::str::from_utf8(&envelope[6..6 + id_len]).map_err(|_| Error::InvalidCiphertext)?;
    let version = u32::from_be_bytes(
        envelope[6 + id_len..10 + id_len]
            .try_into()
            .expect("four version bytes"),
    );
    let mechanism = Mechanism::new(id, version);
    mechanism.validate().map_err(|_| Error::InvalidCiphertext)?;
    Ok((mechanism, &envelope[10 + id_len..]))
}
