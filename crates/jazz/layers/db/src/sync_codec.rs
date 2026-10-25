//! Exact decoding at SQLite's host and server semantic-message boundaries.

use crate::db::{Error, ErrorCode, SyncMessage};

/// Decode exactly one canonical SQLite sync message within the logical-message
/// budget. Trailing bytes and alternate varint spellings are protocol errors;
/// callers must reject them before queueing or applying any part of the frame.
/// This preserves the existing postcard encoding and does not admit old Groove
/// semantic messages or confer identity, scope, or permission authority.
pub fn decode_sync_message(bytes: &[u8]) -> Result<SyncMessage, Error> {
    let invalid = |message: String| {
        Error::new(
            ErrorCode::Protocol,
            format!("invalid SQLite sync frame: {message}"),
        )
    };
    jazz_protocol::protocol_limits::validate_logical_message_len(bytes.len()).map_err(invalid)?;
    jazz_types::postcard_exact::decode_postcard_exact(bytes)
        .map_err(|error| invalid(error.to_string()))
}

#[cfg(test)]
mod tests;
