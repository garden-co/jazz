//! Wire-frame adaptation, bounded-message fragmentation, and reassembly.
//!
//! This stays below the database facade: it converts authenticated byte frames
//! into logical sync messages without changing peer dispatch semantics.

use std::collections::{BTreeMap, HashMap, VecDeque};

use super::{ConnectionSessionContext, Transport};
use crate::protocol::SyncMessage;
use crate::protocol_limits::{
    MAX_INFLIGHT_LOGICAL_MESSAGE_BYTES, MAX_INFLIGHT_LOGICAL_MESSAGES,
    validate_logical_message_len, validate_wire_frame_len,
};
use crate::wire::{
    FEATURE_MESSAGE_FRAGMENTATION, TransportError, WIRE_PROTOCOL_VERSION, WireEnvelope, WireError,
    WireErrorCode, WireFeatures, WireFrame, WireMessageFragment, WireRetry, WireSession,
    WireStreamDecoder, WireStreamEncoder, WireTransport, current_wire_features, decode_frame,
    decode_sync_message_for_features, encode_frame, encode_sync_message_for_features,
};

const WIRE_FRAGMENT_PAYLOAD_BYTES: usize = 512 * 1024;
const RECENT_COMPLETED_LOGICAL_MESSAGES: usize = 64;

/// Adapter from postcard wire frames to the internal sync-message transport.
pub(super) struct IncompleteLogicalMessage {
    protocol_version: u16,
    features: WireFeatures,
    session: Option<WireSession>,
    message_digest: [u8; 32],
    total_len: usize,
    received_len: usize,
    extents: BTreeMap<usize, Vec<u8>>,
}

#[derive(Default)]
pub(super) struct LogicalMessageReassembler {
    pub(super) incomplete: HashMap<u64, IncompleteLogicalMessage>,
    pub(super) staged_bytes: usize,
    recently_completed: VecDeque<(u64, [u8; 32])>,
    highest_completed_message_id: Option<u64>,
}

impl LogicalMessageReassembler {
    pub(super) fn discard(&mut self, message_id: u64) {
        if let Some(state) = self.incomplete.remove(&message_id) {
            self.staged_bytes = self.staged_bytes.saturating_sub(state.received_len);
        }
    }

    pub(super) fn push(
        &mut self,
        fragment: WireMessageFragment,
    ) -> Result<Option<WireEnvelope>, String> {
        if let Some((_, digest)) = self
            .recently_completed
            .iter()
            .find(|(message_id, _)| *message_id == fragment.message_id)
        {
            return if digest == &fragment.message_digest {
                Ok(None)
            } else {
                Err("completed logical message id was reused with another digest".to_owned())
            };
        }
        if self
            .highest_completed_message_id
            .is_some_and(|completed| fragment.message_id <= completed)
        {
            return Ok(None);
        }
        let total_len = usize::try_from(fragment.total_len)
            .map_err(|_| "logical message length does not fit this receiver".to_owned())?;
        validate_logical_message_len(total_len)?;
        let offset = usize::try_from(fragment.offset)
            .map_err(|_| "logical message fragment offset does not fit this receiver".to_owned())?;
        let end = offset
            .checked_add(fragment.payload.len())
            .ok_or_else(|| "logical message fragment range overflow".to_owned())?;
        if fragment.payload.is_empty() || end > total_len {
            return Err("logical message fragment has an empty or out-of-range extent".to_owned());
        }
        if !self.incomplete.contains_key(&fragment.message_id) {
            if self.incomplete.len() >= MAX_INFLIGHT_LOGICAL_MESSAGES {
                return Err("too many incomplete logical messages for peer".to_owned());
            }
            self.incomplete.insert(
                fragment.message_id,
                IncompleteLogicalMessage {
                    protocol_version: fragment.protocol_version,
                    features: fragment.features,
                    session: fragment.session.clone(),
                    message_digest: fragment.message_digest,
                    total_len,
                    received_len: 0,
                    extents: BTreeMap::new(),
                },
            );
        }
        let state = self
            .incomplete
            .get_mut(&fragment.message_id)
            .expect("logical message state inserted");
        if state.total_len != total_len
            || state.protocol_version != fragment.protocol_version
            || state.features != fragment.features
            || state.session != fragment.session
            || state.message_digest != fragment.message_digest
        {
            return Err("logical message fragments disagree on metadata".to_owned());
        }
        if let Some(existing) = state.extents.get(&offset) {
            return if existing == &fragment.payload {
                Ok(None)
            } else {
                Err("conflicting duplicate logical message fragment".to_owned())
            };
        }
        if state
            .extents
            .range(..=offset)
            .next_back()
            .is_some_and(|(start, bytes)| *start + bytes.len() > offset)
            || state
                .extents
                .range(offset..)
                .next()
                .is_some_and(|(start, _)| *start < end)
        {
            return Err("overlapping logical message fragments".to_owned());
        }
        let next_staged = self
            .staged_bytes
            .checked_add(fragment.payload.len())
            .ok_or_else(|| "logical message staging byte count overflow".to_owned())?;
        if next_staged > MAX_INFLIGHT_LOGICAL_MESSAGE_BYTES {
            return Err("incomplete logical messages exceed peer staging budget".to_owned());
        }
        self.staged_bytes = next_staged;
        state.received_len += fragment.payload.len();
        state.extents.insert(offset, fragment.payload);
        if state.received_len != state.total_len {
            return Ok(None);
        }

        let state = self
            .incomplete
            .remove(&fragment.message_id)
            .expect("completed logical message state exists");
        self.staged_bytes -= state.received_len;
        let mut cursor = 0;
        let mut payload = Vec::with_capacity(state.total_len);
        for (offset, extent) in state.extents {
            if offset != cursor {
                return Err(
                    "logical message fragments do not provide contiguous coverage".to_owned(),
                );
            }
            cursor += extent.len();
            payload.extend_from_slice(&extent);
        }
        if cursor != state.total_len || *blake3::hash(&payload).as_bytes() != state.message_digest {
            return Err("logical message digest mismatch".to_owned());
        }
        self.recently_completed
            .push_back((fragment.message_id, state.message_digest));
        self.highest_completed_message_id = Some(fragment.message_id);
        if self.recently_completed.len() > RECENT_COMPLETED_LOGICAL_MESSAGES {
            self.recently_completed.pop_front();
        }
        Ok(Some(WireEnvelope {
            protocol_version: state.protocol_version,
            features: state.features,
            session: state.session,
            payload,
        }))
    }
}

/// Converts logical sync messages to negotiated bounded wire frames and back.
pub struct WireTransportAdapter<T> {
    inner: T,
    protocol_version: u16,
    features: WireFeatures,
    session: Option<WireSession>,
    session_context: Option<ConnectionSessionContext>,
    outbound_stream: WireStreamEncoder,
    inbound_stream: WireStreamDecoder,
    pub(super) reassembler: LogicalMessageReassembler,
    pending_outbound_frames: VecDeque<Vec<u8>>,
    next_outbound_message_id: u64,
}

impl<T> WireTransportAdapter<T>
where
    T: WireTransport,
{
    /// Wrap a byte transport with the current Jazz wire defaults.
    pub fn current(inner: T) -> Self {
        Self::new(inner, WIRE_PROTOCOL_VERSION, current_wire_features(), None)
    }

    /// Wrap a byte transport with explicit negotiated frame metadata.
    pub fn new(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
    ) -> Self {
        Self::new_with_session_context(inner, protocol_version, features, session, None)
    }

    /// Wrap a transport after authenticated hello/session admission supplied
    /// immutable endpoint identities and epochs.
    pub fn new_with_session_context(
        inner: T,
        protocol_version: u16,
        features: WireFeatures,
        session: Option<WireSession>,
        session_context: Option<ConnectionSessionContext>,
    ) -> Self {
        let outbound_stream = WireStreamEncoder::new(features)
            .expect("negotiated wire compression must be compiled into this binary");
        let inbound_stream = WireStreamDecoder::new(features)
            .expect("negotiated wire compression must be compiled into this binary");
        Self {
            inner,
            protocol_version,
            features,
            session,
            session_context,
            outbound_stream,
            inbound_stream,
            reassembler: LogicalMessageReassembler::default(),
            pending_outbound_frames: VecDeque::new(),
            next_outbound_message_id: 0,
        }
    }

    /// Consume the adapter and return the wrapped byte transport.
    pub fn into_inner(self) -> T {
        self.inner
    }

    fn send_wire_error(&mut self, error: WireError) {
        if let Ok(frame) = encode_frame(&WireFrame::Error(error)) {
            let _ = self.inner.send_frame(frame);
        }
    }

    fn flush_pending_outbound(&mut self) -> Result<(), TransportError> {
        while let Some(frame) = self.pending_outbound_frames.pop_front() {
            if let Err(error) = self.inner.send_frame(frame.clone()) {
                self.pending_outbound_frames.push_front(frame);
                return Err(error);
            }
        }
        Ok(())
    }

    fn send_encoded_frames(&mut self, frames: Vec<Vec<u8>>) -> Result<(), TransportError> {
        for (index, frame) in frames.iter().enumerate() {
            match self.inner.send_frame(frame.clone()) {
                Ok(()) => {}
                Err(TransportError::Backpressure) => {
                    // Encoding may have advanced a connection-stream compressor. Accept the
                    // logical message once encoded and retain every unaccepted frame so the
                    // semantic caller must never retry it against advanced codec state.
                    self.pending_outbound_frames
                        .extend(frames[index..].iter().cloned());
                    return Ok(());
                }
                Err(error @ TransportError::Failed(_)) => return Err(error),
            }
        }
        Ok(())
    }

    fn validate_inbound_session(&self, envelope: &WireEnvelope) -> Result<(), WireError> {
        let Some(expected) = &self.session else {
            return Ok(());
        };
        let Some(actual) = &envelope.session else {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterAuth,
                "missing wire session metadata",
            ));
        };
        if actual.session_id != expected.session_id {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterResume,
                "wire session id does not match this connection",
            ));
        }
        if actual.identity != expected.identity {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterAuth,
                "wire session identity does not match this connection",
            ));
        }
        if actual.epoch < expected.epoch {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterResume,
                "stale wire session epoch",
            ));
        }
        if actual.epoch != expected.epoch {
            return Err(WireError::new(
                WireErrorCode::AuthFailed,
                WireRetry::AfterResume,
                "wire session epoch does not match this connection",
            ));
        }
        Ok(())
    }

    fn validate_inbound_metadata(&self, envelope: &WireEnvelope) -> Result<(), WireError> {
        if envelope.protocol_version != self.protocol_version {
            return Err(WireError::new(
                WireErrorCode::UnsupportedProtocolVersion,
                WireRetry::AfterResume,
                format!(
                    "wire message protocol version {} does not match negotiated {}",
                    envelope.protocol_version, self.protocol_version
                ),
            ));
        }
        let unnegotiated = envelope.features & !self.features;
        if unnegotiated != 0 {
            return Err(WireError::new(
                WireErrorCode::UnsupportedFeature,
                WireRetry::AfterResume,
                format!("wire message declares unnegotiated features {unnegotiated:#x}"),
            ));
        }
        Ok(())
    }

    fn decode_inbound_envelope(
        &mut self,
        envelope: WireEnvelope,
    ) -> Result<SyncMessage, WireError> {
        self.validate_inbound_metadata(&envelope)?;
        self.validate_inbound_session(&envelope)?;
        let payload = self
            .inbound_stream
            .decode_message(&envelope.payload, envelope.features)
            .map_err(|message| {
                WireError::new(WireErrorCode::MalformedFrame, WireRetry::Never, message)
            })?;
        validate_logical_message_len(payload.len()).map_err(|message| {
            WireError::new(WireErrorCode::MalformedFrame, WireRetry::Never, message)
        })?;
        let message =
            decode_sync_message_for_features(&payload, self.features).map_err(|error| {
                WireError::new(
                    error.code,
                    error.retry,
                    format!(
                        "{}; payload_bytes={}; payload_hex={}",
                        error.message,
                        payload.len(),
                        hex_diagnostic(&payload)
                    ),
                )
            })?;
        Ok(message)
    }

    /// Strict receive mode for bootstrap-only exchanges. Unlike a live peer,
    /// bootstrap cannot safely skip a malformed physical frame and continue to
    /// a later catalogue snapshot: that would make the authority boundary
    /// depend on first-valid-message behavior.
    pub(crate) fn try_recv_strict(&mut self) -> Result<Option<SyncMessage>, WireError> {
        let _ = self.flush_pending_outbound();
        while let Some(bytes) = self.inner.try_recv_frame() {
            validate_wire_frame_len(bytes.len()).map_err(|message| {
                WireError::new(WireErrorCode::MalformedFrame, WireRetry::Never, message)
            })?;
            let frame = decode_frame(&bytes).map_err(|error| {
                WireError::new(
                    WireErrorCode::MalformedFrame,
                    WireRetry::Never,
                    format!("failed to decode wire frame: {error}"),
                )
            })?;
            match frame {
                WireFrame::Message(envelope) => {
                    return self.decode_inbound_envelope(envelope).map(Some);
                }
                WireFrame::MessageFragment(fragment) => {
                    let fragment_message_id = fragment.message_id;
                    let metadata = WireEnvelope {
                        protocol_version: fragment.protocol_version,
                        features: fragment.features,
                        session: fragment.session.clone(),
                        payload: Vec::new(),
                    };
                    self.validate_inbound_metadata(&metadata)?;
                    self.validate_inbound_session(&metadata)?;
                    if self.features & FEATURE_MESSAGE_FRAGMENTATION == 0
                        || fragment.features & FEATURE_MESSAGE_FRAGMENTATION == 0
                    {
                        return Err(WireError::new(
                            WireErrorCode::UnsupportedFeature,
                            WireRetry::Never,
                            "fragment does not declare logical-message fragmentation",
                        ));
                    }
                    match self.reassembler.push(fragment) {
                        Ok(Some(envelope)) => {
                            return self.decode_inbound_envelope(envelope).map(Some);
                        }
                        Ok(None) => {}
                        Err(message) => {
                            self.reassembler.discard(fragment_message_id);
                            return Err(WireError::new(
                                WireErrorCode::MalformedFrame,
                                WireRetry::AfterResume,
                                message,
                            ));
                        }
                    }
                }
                WireFrame::Hello(_) => {
                    return Err(WireError::new(
                        WireErrorCode::UnsupportedFeature,
                        WireRetry::AfterResume,
                        "hello frames must be handled before constructing a peer connection",
                    ));
                }
                WireFrame::Error(error) => return Err(error),
            }
        }
        Ok(None)
    }
}

impl<T> Transport for WireTransportAdapter<T>
where
    T: WireTransport,
{
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.flush_pending_outbound()?;
        let payload = match encode_sync_message_for_features(&message, self.features) {
            Ok(payload) => payload,
            Err(error) => {
                self.send_wire_error(error);
                return Ok(());
            }
        };
        if let Err(message) = validate_logical_message_len(payload.len()) {
            return Err(TransportError::Failed(message));
        }
        let payload = match self.outbound_stream.encode_message(&payload) {
            Ok(payload) => payload,
            Err(message) => return Err(TransportError::Failed(message)),
        };
        let active_features = (self.features
            & !(crate::wire::FEATURE_PAYLOAD_LZ4 | crate::wire::FEATURE_PAYLOAD_ZSTD))
            | self.outbound_stream.active_feature();
        let mut envelope = WireEnvelope::new(self.protocol_version, active_features, payload);
        if let Some(session) = self.session.clone() {
            envelope = envelope.with_session(session);
        }
        match encode_frame(&WireFrame::Message(envelope.clone())) {
            Ok(frame) if frame.len() <= WIRE_FRAGMENT_PAYLOAD_BYTES => {
                self.send_encoded_frames(vec![frame])
            }
            Ok(_) if self.features & FEATURE_MESSAGE_FRAGMENTATION == 0 => {
                Err(TransportError::Failed(
                    "peer did not negotiate logical-message fragmentation".to_owned(),
                ))
            }
            Ok(_) => {
                let message_digest = *blake3::hash(&envelope.payload).as_bytes();
                let message_id = self.next_outbound_message_id;
                self.next_outbound_message_id = self
                    .next_outbound_message_id
                    .checked_add(1)
                    .ok_or_else(|| {
                        TransportError::Failed("logical message id space exhausted".to_owned())
                    })?;
                let total_len = u64::try_from(envelope.payload.len()).map_err(|_| {
                    TransportError::Failed("logical message is too large".to_owned())
                })?;
                let mut frames = Vec::new();
                for (index, payload) in envelope
                    .payload
                    .chunks(WIRE_FRAGMENT_PAYLOAD_BYTES)
                    .enumerate()
                {
                    let offset = u64::try_from(index * WIRE_FRAGMENT_PAYLOAD_BYTES)
                        .expect("fragment offset is bounded by logical message size");
                    let fragment = WireMessageFragment {
                        protocol_version: envelope.protocol_version,
                        features: envelope.features,
                        session: envelope.session.clone(),
                        message_id,
                        message_digest,
                        total_len,
                        offset,
                        payload: payload.to_vec(),
                    };
                    let frame =
                        encode_frame(&WireFrame::MessageFragment(fragment)).map_err(|error| {
                            TransportError::Failed(format!(
                                "failed to encode logical message fragment: {error}"
                            ))
                        })?;
                    validate_wire_frame_len(frame.len()).map_err(TransportError::Failed)?;
                    frames.push(frame);
                }
                self.send_encoded_frames(frames)
            }
            Err(err) => {
                self.send_wire_error(WireError::new(
                    WireErrorCode::Internal,
                    WireRetry::Never,
                    format!("failed to encode wire frame: {err}"),
                ));
                Ok(())
            }
        }
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        let _ = self.flush_pending_outbound();
        while let Some(bytes) = self.inner.try_recv_frame() {
            if let Err(message) = validate_wire_frame_len(bytes.len()) {
                self.send_wire_error(WireError::new(
                    WireErrorCode::MalformedFrame,
                    WireRetry::Never,
                    message,
                ));
                continue;
            }
            let frame = match decode_frame(&bytes) {
                Ok(frame) => frame,
                Err(err) => {
                    self.send_wire_error(WireError::new(
                        WireErrorCode::MalformedFrame,
                        WireRetry::Never,
                        format!("failed to decode wire frame: {err}"),
                    ));
                    continue;
                }
            };
            match frame {
                WireFrame::Message(envelope) => match self.decode_inbound_envelope(envelope) {
                    Ok(message) => return Some(message),
                    Err(error) => self.send_wire_error(error),
                },
                WireFrame::MessageFragment(fragment) => {
                    let fragment_message_id = fragment.message_id;
                    let metadata = WireEnvelope {
                        protocol_version: fragment.protocol_version,
                        features: fragment.features,
                        session: fragment.session.clone(),
                        payload: Vec::new(),
                    };
                    if let Err(error) = self.validate_inbound_metadata(&metadata) {
                        self.send_wire_error(error);
                        continue;
                    }
                    if let Err(error) = self.validate_inbound_session(&metadata) {
                        self.send_wire_error(error);
                        continue;
                    }
                    if self.features & FEATURE_MESSAGE_FRAGMENTATION == 0
                        || fragment.features & FEATURE_MESSAGE_FRAGMENTATION == 0
                    {
                        self.send_wire_error(WireError::new(
                            WireErrorCode::UnsupportedFeature,
                            WireRetry::Never,
                            "fragment does not declare logical-message fragmentation",
                        ));
                        continue;
                    }
                    match self.reassembler.push(fragment) {
                        Ok(Some(envelope)) => match self.decode_inbound_envelope(envelope) {
                            Ok(message) => return Some(message),
                            Err(error) => self.send_wire_error(error),
                        },
                        Ok(None) => {}
                        Err(message) => {
                            self.reassembler.discard(fragment_message_id);
                            self.send_wire_error(WireError::new(
                                WireErrorCode::MalformedFrame,
                                WireRetry::AfterResume,
                                message,
                            ));
                        }
                    }
                }
                WireFrame::Hello(_) => self.send_wire_error(WireError::new(
                    WireErrorCode::UnsupportedFeature,
                    WireRetry::AfterResume,
                    "hello frames must be handled before constructing a peer connection",
                )),
                WireFrame::Error(_) => {}
            }
        }
        None
    }

    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.session_context
    }
}

fn hex_diagnostic(bytes: &[u8]) -> String {
    if bytes.len() <= 128 {
        return hex_prefix(bytes, bytes.len());
    }
    hex_prefix(bytes, 16)
}

fn hex_prefix(bytes: &[u8], max: usize) -> String {
    bytes
        .iter()
        .take(max)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join("")
}
