//! The WebSocket carriage of wire frames: the opening `Hello` (and the
//! refusal for a peer on another wire version), and batches of encoded
//! frames packed into bounded WebSocket messages.

use jazz::wire::{WireError, WireErrorCode, WireHello, WireRetry};

/// Decode the opening `Hello`, which arrives as a one-frame batch or a bare
/// frame. A peer on another wire version gets `UnsupportedProtocolVersion`
/// even when the rest of its `Hello` uses that version's layout.
pub fn decode_ws_hello(bytes: &[u8]) -> Result<WireHello, WireError> {
    let not_hello = || {
        WireError::new(
            WireErrorCode::MalformedFrame,
            WireRetry::Never,
            "websocket expects first wire frame to be WireFrame::Hello",
        )
    };
    let frame = match decode_ws_encoded_frame_batch(bytes) {
        Ok(mut frames) if frames.len() == 1 => frames.remove(0),
        Ok(_) => return Err(not_hello()),
        Err(_) => bytes.to_vec(),
    };
    jazz::wire::decode_hello_frame(&frame).map_err(|error| match error.code {
        WireErrorCode::UnsupportedProtocolVersion => error,
        _ => not_hello(),
    })
}

/// The refusal for a client whose `Hello` advertises a wire version range
/// without this server's, encoded so that client decodes it: an alpha.59
/// client is told to upgrade instead of failing on a frame it cannot read.
/// `None` when the first frame is not a `Hello` or speaks this version.
pub fn ws_foreign_version_refusal(bytes: &[u8]) -> Option<Vec<u8>> {
    let frame = match decode_ws_encoded_frame_batch(bytes) {
        Ok(mut frames) if frames.len() == 1 => frames.remove(0),
        Ok(_) => return None,
        Err(_) => bytes.to_vec(),
    };
    let (min, max) = jazz::wire::wire_hello_version_range(&frame)?;
    let ours = jazz::wire::WIRE_PROTOCOL_VERSION;
    let message = if max < ours {
        format!(
            "this Jazz client speaks wire protocol {min}..={max}, older than this server's \
             {ours}: upgrade the client to the server's Jazz release (a client from \
             2.0.0-alpha.59 or earlier cannot connect to 2.0.0-alpha.60 or later)"
        )
    } else if min > ours {
        format!(
            "this Jazz client speaks wire protocol {min}..={max}, newer than this server's \
             {ours}: upgrade the server to the client's Jazz release"
        )
    } else if min == ours && max == ours {
        return None;
    } else {
        format!(
            "this Jazz client advertises wire protocol {min}..={max}; this server speaks \
             exactly {ours}: use the same Jazz release on client and server"
        )
    };
    let error = WireError::new(
        WireErrorCode::UnsupportedProtocolVersion,
        WireRetry::Never,
        message,
    );
    jazz::wire::encode_error_frame_for_peer(&error, max).ok()
}

#[cfg(test)]
pub fn decode_ws_frame_batch(bytes: &[u8]) -> Result<Vec<jazz::wire::WireFrame>, postcard::Error> {
    let encoded_frames = decode_ws_encoded_frame_batch(bytes)?;
    encoded_frames
        .iter()
        .map(|frame| jazz::wire::decode_frame(frame))
        .collect()
}

pub fn decode_ws_encoded_frame_batch(bytes: &[u8]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    jazz::wire::decode_websocket_frame_batch(bytes)
}

/// Pack encoded wire frames into as few bounded WebSocket batch payloads as fit.
pub fn encode_ws_frame_batches(frames: &[Vec<u8>]) -> Result<Vec<Vec<u8>>, postcard::Error> {
    let mut batches = Vec::new();
    let mut current = Vec::new();
    for frame in frames {
        let mut candidate = current.clone();
        candidate.push(frame.clone());
        let candidate_fits = jazz::wire::encode_websocket_frame_batch(&candidate).is_ok();
        if !candidate_fits && !current.is_empty() {
            batches.push(jazz::wire::encode_websocket_frame_batch(&current)?);
            current.clear();
            // A singleton has its own count and length prefixes. Validate the
            // actual carrier after the flush instead of assuming a raw frame
            // at the frame limit can fit by itself.
            jazz::wire::encode_websocket_frame_batch(std::slice::from_ref(frame))?;
        } else if !candidate_fits {
            return Err(postcard::Error::SerializeBufferFull);
        }
        current.push(frame.clone());
    }
    if !current.is_empty() {
        batches.push(jazz::wire::encode_websocket_frame_batch(&current)?);
    }
    Ok(batches)
}
