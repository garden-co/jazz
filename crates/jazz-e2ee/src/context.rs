//! Canonical context, version 1, and length-delimited record framing.

use crate::error::{Error, Result};

const PREFIX: [u8; 5] = [b'J', b'E', b'2', b'C', 1];
const MAX_FIELD_BYTES: usize = 65_535;

/// The authenticated coordinates of one operation: stable IDs, not display
/// names. Optional fields (`table`, `row`, `column`, `recipient`) are empty
/// when unused; empty and omitted mean the same thing.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CryptoContext<'a> {
    pub application: &'a str,
    pub policy: &'a str,
    pub scope: &'a str,
    pub identifier: &'a str,
    pub table: &'a str,
    pub row: &'a str,
    pub column: &'a str,
    pub epoch: &'a str,
    pub recipient: &'a str,
}

impl CryptoContext<'_> {
    /// `"JE2C" || 01`, then nine u32be-length-prefixed UTF-8 fields in the
    /// order application, policy, scope, identifier, table, row, column,
    /// epoch, recipient. Application, policy, scope, identifier and epoch
    /// must be non-empty; every field is at most 65,535 bytes. No Unicode
    /// normalisation takes place.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let fields = [
            (self.application, true),
            (self.policy, true),
            (self.scope, true),
            (self.identifier, true),
            (self.table, false),
            (self.row, false),
            (self.column, false),
            (self.epoch, true),
            (self.recipient, false),
        ];
        let mut size = PREFIX.len();
        for (value, required) in fields {
            if value.len() > MAX_FIELD_BYTES || (required && value.is_empty()) {
                return Err(Error::InvalidInput);
            }
            size += 4 + value.len();
        }
        let mut bytes = Vec::with_capacity(size);
        bytes.extend_from_slice(&PREFIX);
        for (value, _) in fields {
            bytes.extend_from_slice(&(value.len() as u32).to_be_bytes());
            bytes.extend_from_slice(value.as_bytes());
        }
        Ok(bytes)
    }
}

/// Concatenates `parts`, each prefixed with its u32be byte length.
pub fn frame_record(parts: &[&[u8]]) -> Result<Vec<u8>> {
    let mut size = 0usize;
    for part in parts {
        if u32::try_from(part.len()).is_err() {
            return Err(Error::InvalidInput);
        }
        size += 4 + part.len();
    }
    let mut bytes = Vec::with_capacity(size);
    for part in parts {
        bytes.extend_from_slice(&(part.len() as u32).to_be_bytes());
        bytes.extend_from_slice(part);
    }
    Ok(bytes)
}
