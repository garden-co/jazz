//! Exact decoding at SQLite's host and server semantic-message boundaries.

use crate::db::{Error, ErrorCode, SyncMessage};

/// Decode exactly one canonical SQLite sync message within the logical-message
/// budget. Trailing bytes and alternate varint spellings are protocol errors;
/// callers must reject them before queueing or applying any part of the frame.
/// The jazz.sqlite.sync.v2 header refuses both older SQLite layouts and Groove
/// messages. Codec admission confers no identity, scope or permission authority.
pub fn decode_sync_message(bytes: &[u8]) -> Result<SyncMessage, Error> {
    let invalid = |message: String| {
        Error::new(
            ErrorCode::Protocol,
            format!("invalid SQLite sync frame: {message}"),
        )
    };
    jazz_protocol::protocol_limits::validate_logical_message_len(bytes.len()).map_err(invalid)?;
    if !bytes.starts_with(crate::db::SYNC_ENCODING_PREFIX) {
        return Err(invalid(
            "unsupported encoding; expected jazz.sqlite.sync.v2".into(),
        ));
    }
    crate::db::decode_message(bytes).map_err(|error| invalid(error.to_string()))
}

#[cfg(test)]
mod tests;
