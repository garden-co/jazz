//! The inline scalar envelope for `String`, `Bytes` and JSON cells.
//!
//! A record stores each such cell as a one-arm variant record: tag `0`
//! followed by the logical bytes. Chunked (content-addressed) large values
//! are not supported yet; an encoding that selects any other arm is
//! rejected as malformed.

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::records::ValueType;

/// The logical kind of a variable-width scalar.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum LargeValueKind {
    /// Arbitrary bytes.
    Bytes,
    /// UTF-8 text.
    String,
    /// UTF-8 text that parses as JSON.
    Json,
}

/// Constructs the descriptor-only physical scalar type used for JSON
/// columns. The resulting type is intentionally impossible to name or
/// construct through the public `records::ValueType` API.
pub fn physical_storage_value_type(kind: LargeValueKind) -> ValueType {
    ValueType::stored_scalar(kind)
}

/// Why an inline scalar was refused.
#[derive(Clone, Debug, Error, PartialEq, Eq)]
pub enum Error {
    /// A string payload is not UTF-8.
    #[error("string is not valid UTF-8")]
    InvalidUtf8,
    /// A JSON payload does not parse.
    #[error("JSON is not valid JSON")]
    InvalidJson,
    /// The envelope is not the inline arm.
    #[error("malformed physical scalar encoding")]
    MalformedScalar,
}

pub(crate) fn encode_primitive_stored_scalar_into(
    kind: LargeValueKind,
    bytes: &[u8],
    output: &mut Vec<u8>,
) -> Result<(), Error> {
    validate_logical(kind, bytes)?;
    crate::records::append_variant_record(output, 0, bytes);
    Ok(())
}

/// The logical bytes of an inline scalar, borrowed from its encoding.
pub fn inline_scalar_bytes(kind: LargeValueKind, encoded: &[u8]) -> Result<&[u8], Error> {
    let (tag, payload) =
        crate::records::split_variant_record(encoded).map_err(|_| Error::MalformedScalar)?;
    if tag != 0 {
        return Err(Error::MalformedScalar);
    }
    // The inline arm has exactly one raw field, so its canonical record is the
    // payload itself and logical validation is the whole acceptance check.
    validate_logical(kind, payload).map_err(|error| match error {
        Error::InvalidUtf8 => Error::InvalidUtf8,
        _ => Error::MalformedScalar,
    })?;
    Ok(payload)
}

fn validate_logical(kind: LargeValueKind, bytes: &[u8]) -> Result<(), Error> {
    match kind {
        LargeValueKind::Bytes => Ok(()),
        LargeValueKind::String => std::str::from_utf8(bytes)
            .map(|_| ())
            .map_err(|_| Error::InvalidUtf8),
        LargeValueKind::Json => {
            let text = std::str::from_utf8(bytes).map_err(|_| Error::InvalidUtf8)?;
            serde_json::from_str::<serde_json::Value>(text)
                .map(|_| ())
                .map_err(|_| Error::InvalidJson)
        }
    }
}
