//! Versioned transport frames around Jazz sync semantics.
//!
//! The wire layer is intentionally thinner than [`crate::protocol`]: it owns
//! link/session negotiation, feature discovery, binary framing, and structured
//! protocol errors. The frame payload is opaque bytes for now so bindings and
//! server shells can adopt the envelope before the full [`crate::protocol::SyncMessage`]
//! encoder is frozen.

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;

use crate::ids::{AuthorSubject, NodeUuid};
use crate::protocol::SyncMessage;
use crate::protocol_limits::{validate_logical_message_len, validate_wire_frame_len};

/// Current Jazz wire protocol version.
/// Version 14 combines the v13 Groove canonical large-scalar descriptor and
/// canonical `[iss,sub]` author encoding with Unix-millisecond public row
/// provenance. The packed HLC remains internal ordering state and is never
/// protocol data. This is an intentional breaking baseline: a v13 peer would
/// interpret provenance payloads differently, so negotiation rejects it.
pub const WIRE_PROTOCOL_VERSION: u16 = 14;

/// No optional features.
pub const FEATURE_NONE: WireFeatures = 0;
/// Frame payloads contain encoded Jazz sync messages.
pub const FEATURE_SYNC_MESSAGE_PAYLOAD: WireFeatures = 1 << 0;
/// Frames may carry an explicit resumable session id and epoch.
pub const FEATURE_SESSION_FRAME: WireFeatures = 1 << 1;
/// Peers understand structured [`WireError`] frames.
pub const FEATURE_STRUCTURED_ERRORS: WireFeatures = 1 << 2;
/// Message frame payloads may be LZ4-compressed at the transport frame seam.
pub const FEATURE_PAYLOAD_LZ4: WireFeatures = 1 << 3;
/// Message frame payloads may be Zstandard-compressed at the transport frame seam.
pub const FEATURE_PAYLOAD_ZSTD: WireFeatures = 1 << 4;
/// Logical sync messages may be decomposed into bounded physical frames.
pub const FEATURE_MESSAGE_FRAGMENTATION: WireFeatures = 1 << 5;
/// Semantic frames may carry authorization-support purposes and receipts.
///
/// This feature is deliberately separate from framing: an older peer can
/// still exchange every pre-existing sync message, but must never be asked to
/// deserialize the new semantic enum variants or extension fields.
pub const FEATURE_AUTHORIZATION_SCOPE_RECEIPTS: WireFeatures = 1 << 6;
/// Authority-owned authorization scope hydration.  Unlike the first receipt
/// experiment this never accepts caller supplied support query identities.
pub const FEATURE_AUTHORIZATION_SCOPE_VIEWS: WireFeatures = 1 << 7;
/// Peers support Groove chunk misses on the independently driven auxiliary lane.
pub const FEATURE_AUXILIARY_CHUNKS: WireFeatures = 1 << 8;

const FEATURE_PAYLOAD_COMPRESSION_MASK: WireFeatures = FEATURE_PAYLOAD_LZ4 | FEATURE_PAYLOAD_ZSTD;

/// Bitset of optional protocol features advertised by one peer.
pub type WireFeatures = u64;

/// One transport frame exchanged between Jazz runtimes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WireFrame {
    /// Capability and version negotiation frame.
    Hello(WireHello),
    /// Opaque semantic sync payload with negotiated framing metadata.
    Message(WireEnvelope),
    /// Structured protocol/session error.
    Error(WireError),
    /// One physical extent of an encoded logical sync message.
    MessageFragment(WireMessageFragment),
}

/// A bounded physical extent of one encoded logical message.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireMessageFragment {
    /// Negotiated protocol version used by the logical message.
    pub protocol_version: u16,
    /// Optional features active for the encoded logical message.
    pub features: WireFeatures,
    /// Optional authenticated/resumable session metadata.
    pub session: Option<WireSession>,
    /// Monotone identity within this connection direction.
    pub message_id: u64,
    /// Integrity digest of the complete encoded payload.
    pub message_digest: [u8; 32],
    /// Exact encoded payload length before fragmentation.
    pub total_len: u64,
    /// Byte offset of this extent in the encoded payload.
    pub offset: u64,
    /// Bytes at `offset`.
    pub payload: Vec<u8>,
}

impl std::fmt::Debug for WireMessageFragment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireMessageFragment")
            .field("protocol_version", &self.protocol_version)
            .field("features", &self.features)
            .field("session", &self.session)
            .field("message_id", &self.message_id)
            .field("message_digest", &hex::encode(self.message_digest))
            .field("total_len", &self.total_len)
            .field("offset", &self.offset)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

/// Link role advertised during handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WirePeerRole {
    /// End-user or local application runtime.
    Client,
    /// Durable server or authority runtime.
    Core,
    /// Edge runtime terminating client identity and policy composition.
    Edge,
    /// Relay/cache runtime without a terminated end-user identity.
    Relay,
}

/// Handshake payload used to negotiate a common wire version and feature set.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireHello {
    /// Lowest protocol version this peer can speak.
    pub min_protocol_version: u16,
    /// Highest protocol version this peer can speak.
    pub max_protocol_version: u16,
    /// Optional features supported by this peer.
    pub features: WireFeatures,
    /// Runtime/link role for topology and admission decisions.
    pub role: WirePeerRole,
    /// Authority endpoint bound by the authenticated handshake when the
    /// authorization-scope receipt feature is offered.  Semantic sync frames
    /// never self-assert this identity.
    #[serde(default)]
    pub authority: Option<WireAuthorityEndpoint>,
}

/// Fresh, authenticated authority endpoint identity for one negotiated link.
///
/// The session/admission layer allocates this nonce before constructing the
/// eventual sync connection; receipts use it to reject reconnect replay.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireAuthorityEndpoint {
    /// Stable authority node identity authenticated by the transport.
    pub node: NodeUuid,
    /// Fresh non-resumable epoch for this accepted connection.
    pub epoch: u64,
}

impl WireHello {
    /// Construct a hello frame for the current implementation.
    pub fn current(role: WirePeerRole, features: WireFeatures) -> Self {
        Self {
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            features,
            role,
            authority: None,
        }
    }

    /// Attach the endpoint allocated by authenticated session admission.
    pub fn with_authority(mut self, node: NodeUuid, epoch: u64) -> Self {
        self.authority = Some(WireAuthorityEndpoint { node, epoch });
        self
    }
}

/// Agreed version and optional features for one peer link.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WireNegotiated {
    /// Highest mutually supported protocol version.
    pub protocol_version: u16,
    /// Intersection of both peers' optional features.
    pub features: WireFeatures,
}

/// Transport payload compression codec.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WireCompression {
    /// No transport compression.
    None,
    /// LZ4 stream payload compression.
    Lz4,
    /// Zstandard stream payload compression.
    Zstd,
}

impl WireCompression {
    /// Select the active codec from negotiated feature bits.
    ///
    /// LZ4 wins ties intentionally: it is the default low-CPU transport codec.
    pub fn from_features(features: WireFeatures) -> Self {
        if features & FEATURE_PAYLOAD_LZ4 != 0 {
            Self::Lz4
        } else if features & FEATURE_PAYLOAD_ZSTD != 0 {
            Self::Zstd
        } else {
            Self::None
        }
    }

    /// Feature bit carried on frames using this codec.
    pub fn feature(self) -> WireFeatures {
        match self {
            Self::None => FEATURE_NONE,
            Self::Lz4 => FEATURE_PAYLOAD_LZ4,
            Self::Zstd => FEATURE_PAYLOAD_ZSTD,
        }
    }
}

/// Session metadata carried by message frames after handshake/admission.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireSession {
    /// Binding/server assigned resumable session id.
    pub session_id: String,
    /// Monotone session incarnation. Reconnects that abandon prior ordering use a new epoch.
    pub epoch: u64,
    /// Authenticated user identity for edge/client links, once admission succeeds.
    pub identity: Option<AuthorSubject>,
}

impl std::fmt::Debug for WireSession {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let fingerprint = blake3::hash(self.session_id.as_bytes()).to_hex();
        f.debug_struct("WireSession")
            .field("session_id_len", &self.session_id.len())
            .field("session_id_fingerprint", &&fingerprint[..12])
            .field("epoch", &self.epoch)
            .field("identity", &self.identity)
            .finish()
    }
}

/// Metadata and payload for one semantic sync message.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireEnvelope {
    /// Negotiated protocol version used to encode this payload.
    pub protocol_version: u16,
    /// Optional features active for this frame.
    pub features: WireFeatures,
    /// Optional session metadata for reconnectable links.
    pub session: Option<WireSession>,
    /// Encoded semantic payload, usually a [`crate::protocol::SyncMessage`].
    pub payload: Vec<u8>,
}

impl std::fmt::Debug for WireEnvelope {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WireEnvelope")
            .field("protocol_version", &self.protocol_version)
            .field("features", &self.features)
            .field("session", &self.session)
            .field("payload_len", &self.payload.len())
            .finish()
    }
}

impl WireEnvelope {
    /// Construct a payload frame with no session metadata.
    pub fn new(protocol_version: u16, features: WireFeatures, payload: Vec<u8>) -> Self {
        Self {
            protocol_version,
            features,
            session: None,
            payload,
        }
    }

    /// Attach session metadata to the envelope.
    pub fn with_session(mut self, session: WireSession) -> Self {
        self.session = Some(session);
        self
    }
}

/// Structured wire error code.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireErrorCode {
    /// Peers do not share a supported protocol version.
    UnsupportedProtocolVersion,
    /// A required feature was not negotiated.
    UnsupportedFeature,
    /// The frame could not be decoded or violates the envelope contract.
    MalformedFrame,
    /// Authentication or authorization failed.
    AuthFailed,
    /// Receiver is currently overloaded.
    Backpressure,
    /// Internal implementation error.
    Internal,
}

/// Retry guidance for bindings and transports.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum WireRetry {
    /// Retrying the same operation cannot succeed.
    Never,
    /// Retry after refreshing credentials or re-running admission.
    AfterAuth,
    /// Retry after reconnecting/resuming the session.
    AfterResume,
    /// Retry later with transport backoff.
    Later,
}

/// Structured protocol/session error frame.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireError {
    /// Machine-readable error code.
    pub code: WireErrorCode,
    /// Binding-facing retry guidance.
    pub retry: WireRetry,
    /// Human-readable diagnostic. Not part of semantic compatibility.
    pub message: String,
}

impl WireError {
    /// Construct a wire error.
    pub fn new(code: WireErrorCode, retry: WireRetry, message: impl Into<String>) -> Self {
        Self {
            code,
            retry,
            message: message.into(),
        }
    }
}

/// Serialize a wire frame with the canonical Jazz frame codec.
pub fn encode_frame(frame: &WireFrame) -> Result<Vec<u8>, postcard::Error> {
    to_allocvec(frame)
}

/// Decode a wire frame serialized by [`encode_frame`].
pub fn decode_frame(bytes: &[u8]) -> Result<WireFrame, postcard::Error> {
    if validate_wire_frame_len(bytes.len()).is_err() {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    let frame: WireFrame = from_bytes(bytes)?;
    // `postcard` is a compact transport, not a permissive interchange
    // language.  A payload with another spelling for the same semantic frame
    // would undermine byte-addressed receipts and golden fixtures.  Decode
    // first so ordinary malformed/trailing inputs retain postcard's error,
    // then require the one encoder spelling for every accepted frame.
    if to_allocvec(&frame)? != bytes {
        return Err(postcard::Error::DeserializeBadOption);
    }
    Ok(frame)
}

/// Serialize a semantic sync message with the canonical Jazz payload codec.
pub fn encode_sync_message(message: &SyncMessage) -> Result<Vec<u8>, postcard::Error> {
    message
        .validate_version_carriers()
        .map_err(|_| postcard::Error::SerdeSerCustom)?;
    to_allocvec(message)
}

/// Serialize a semantic message only when its required capabilities were
/// negotiated for this link.
///
/// Keep this check immediately adjacent to the canonical codec.  Postcard
/// encodes Rust enums by ordinal, so allowing an unsupported variant past this
/// seam would make an older peer decode a different message (or fail after it
/// has already accepted a semantic frame).
pub fn encode_sync_message_for_features(
    message: &SyncMessage,
    negotiated_features: WireFeatures,
) -> Result<Vec<u8>, WireError> {
    ensure_sync_message_features(message, negotiated_features)?;
    encode_sync_message(message).map_err(|error| {
        WireError::new(
            WireErrorCode::MalformedFrame,
            WireRetry::Never,
            format!("failed to encode sync message payload: {error}"),
        )
    })
}

/// Decode a semantic sync message serialized by [`encode_sync_message`].
pub fn decode_sync_message(bytes: &[u8]) -> Result<SyncMessage, postcard::Error> {
    if validate_logical_message_len(bytes.len()).is_err() {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    let message: SyncMessage = from_bytes(bytes)?;
    message
        .validate_version_carriers()
        .map_err(|_| postcard::Error::DeserializeBadOption)?;
    // Wire receipts and replay fixtures name bytes, not only deserialized
    // values.  Do not accept an alternate postcard representation for the
    // same transaction/version message.
    if to_allocvec(&message)? != bytes {
        return Err(postcard::Error::DeserializeBadOption);
    }
    Ok(message)
}

/// Decode a semantic message only when its required capabilities were
/// negotiated for this link.
pub fn decode_sync_message_for_features(
    bytes: &[u8],
    negotiated_features: WireFeatures,
) -> Result<SyncMessage, WireError> {
    let message = decode_sync_message(bytes).map_err(|error| {
        WireError::new(
            WireErrorCode::MalformedFrame,
            WireRetry::Never,
            format!("failed to decode sync message payload: {error}"),
        )
    })?;
    ensure_sync_message_features(&message, negotiated_features)?;
    Ok(message)
}

/// Decode a semantic sync message for receiver apply.
pub fn decode_sync_message_for_receive(bytes: &[u8]) -> Result<SyncMessage, postcard::Error> {
    decode_sync_message(bytes)
}

/// Reject semantic extensions that this connection did not negotiate.
pub fn ensure_sync_message_features(
    message: &SyncMessage,
    negotiated_features: WireFeatures,
) -> Result<(), WireError> {
    let required = message.required_wire_features();
    let missing = required & !negotiated_features;
    if missing == 0 {
        return Ok(());
    }
    Err(WireError::new(
        WireErrorCode::UnsupportedFeature,
        WireRetry::AfterResume,
        format!("sync message requires unnegotiated features {missing:#x}"),
    ))
}

/// Optional transport compression features enabled for this process.
pub fn runtime_transport_compression_features() -> WireFeatures {
    let Ok(value) = std::env::var("JAZZ_TRANSPORT_COMPRESSION") else {
        return default_transport_compression_features();
    };
    match value.to_ascii_lowercase().as_str() {
        "0" | "false" | "off" | "none" | "disabled" => FEATURE_NONE,
        "lz4" => cfg_lz4_feature(),
        "zstd" | "zstd-3" => cfg_zstd_feature(),
        "1" | "true" | "on" | "auto" => cfg_lz4_feature() | cfg_zstd_feature(),
        _ => FEATURE_NONE,
    }
}

fn default_transport_compression_features() -> WireFeatures {
    #[cfg(any(
        all(not(target_arch = "wasm32"), feature = "transport-compression-zstd"),
        all(
            target_arch = "wasm32",
            any(
                feature = "transport-compression-zstd",
                feature = "transport-compression-ruzstd"
            )
        )
    ))]
    {
        FEATURE_PAYLOAD_ZSTD
    }
    #[cfg(not(any(
        all(not(target_arch = "wasm32"), feature = "transport-compression-zstd"),
        all(
            target_arch = "wasm32",
            any(
                feature = "transport-compression-zstd",
                feature = "transport-compression-ruzstd"
            )
        )
    )))]
    {
        FEATURE_NONE
    }
}

/// Base sync frame features plus any runtime-enabled transport compression.
pub fn current_wire_features() -> WireFeatures {
    FEATURE_SYNC_MESSAGE_PAYLOAD
        | FEATURE_STRUCTURED_ERRORS
        | FEATURE_MESSAGE_FRAGMENTATION
        | FEATURE_AUTHORIZATION_SCOPE_RECEIPTS
        | FEATURE_AUTHORIZATION_SCOPE_VIEWS
        | FEATURE_AUXILIARY_CHUNKS
        | runtime_transport_compression_features()
}

fn cfg_lz4_feature() -> WireFeatures {
    #[cfg(feature = "transport-compression-lz4")]
    {
        FEATURE_PAYLOAD_LZ4
    }
    #[cfg(not(feature = "transport-compression-lz4"))]
    {
        FEATURE_NONE
    }
}

fn cfg_zstd_feature() -> WireFeatures {
    #[cfg(feature = "transport-compression-zstd")]
    {
        FEATURE_PAYLOAD_ZSTD
    }
    #[cfg(not(feature = "transport-compression-zstd"))]
    {
        FEATURE_NONE
    }
}

/// Compress a sync payload for one message envelope.
///
/// Production peer links use the same per-message encoding through
/// [`WireStreamEncoder`], allowing decompression to enforce an output cap.
pub fn compress_sync_payload(
    payload: Vec<u8>,
    negotiated_features: WireFeatures,
) -> Result<(Vec<u8>, WireFeatures), String> {
    let codec = WireCompression::from_features(negotiated_features);
    let active_feature = codec.feature();
    let payload = match codec {
        WireCompression::None => payload,
        WireCompression::Lz4 => compress_lz4(&payload)?,
        WireCompression::Zstd => compress_zstd(&payload)?,
    };
    Ok((payload, active_feature))
}

/// Decompress a sync payload according to the envelope's active feature bit.
pub fn decompress_sync_payload(
    payload: &[u8],
    envelope_features: WireFeatures,
) -> Result<Vec<u8>, String> {
    let active = envelope_features & FEATURE_PAYLOAD_COMPRESSION_MASK;
    if active.count_ones() > 1 {
        return Err("wire frame declares more than one payload compression codec".to_owned());
    }
    match WireCompression::from_features(active) {
        WireCompression::None => Ok(payload.to_vec()),
        WireCompression::Lz4 => decompress_lz4(payload),
        WireCompression::Zstd => decompress_zstd(payload),
    }
}

fn compress_lz4(payload: &[u8]) -> Result<Vec<u8>, String> {
    jazz_compression::compress_lz4(payload)
}

fn decompress_lz4(payload: &[u8]) -> Result<Vec<u8>, String> {
    jazz_compression::decompress_lz4(payload, crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES)
}

fn compress_zstd(payload: &[u8]) -> Result<Vec<u8>, String> {
    jazz_compression::compress_zstd(payload)
}

fn decompress_zstd(payload: &[u8]) -> Result<Vec<u8>, String> {
    jazz_compression::decompress_zstd(payload, crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES)
}

/// Negotiated per-message compression for sync payloads.
pub struct WireStreamEncoder {
    codec: WireCompression,
}

impl WireStreamEncoder {
    /// Create encoder state for one outbound connection direction.
    pub fn new(features: WireFeatures) -> Result<Self, String> {
        let codec = outbound_wire_compression_from_features(features);
        match codec {
            WireCompression::None => {}
            WireCompression::Lz4 if cfg_lz4_feature() == FEATURE_NONE => {
                return Err("lz4 transport compression feature is not compiled in".to_owned());
            }
            WireCompression::Zstd if !cfg_can_encode_zstd() => {
                return Err("zstd transport compression feature is not compiled in".to_owned());
            }
            _ => {}
        }
        Ok(Self { codec })
    }

    /// Active feature bit carried by message envelopes for this stream.
    pub fn active_feature(&self) -> WireFeatures {
        self.codec.feature()
    }

    /// Encode one sync payload into the connection stream and return the bytes
    /// newly emitted by this message.
    pub fn encode_message(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        match self.codec {
            WireCompression::None => Ok(payload.to_vec()),
            WireCompression::Lz4 => compress_lz4(payload),
            WireCompression::Zstd => compress_zstd(payload),
        }
    }
}

fn outbound_wire_compression_from_features(features: WireFeatures) -> WireCompression {
    match WireCompression::from_features(features) {
        WireCompression::Zstd if !cfg_can_encode_zstd() => WireCompression::None,
        codec => codec,
    }
}

fn cfg_can_encode_zstd() -> bool {
    cfg!(feature = "transport-compression-zstd")
}

/// Negotiated per-message decompression for sync payloads.
pub struct WireStreamDecoder {
    codec: WireCompression,
}

impl WireStreamDecoder {
    /// Create decoder state for one inbound connection direction.
    pub fn new(features: WireFeatures) -> Result<Self, String> {
        let codec = WireCompression::from_features(features);
        Ok(Self { codec })
    }

    /// Decode one message's stream chunk into one owned semantic sync payload.
    pub fn decode_message(
        &mut self,
        payload: &[u8],
        envelope_features: WireFeatures,
    ) -> Result<Vec<u8>, String> {
        self.decode_message_borrowed(payload, envelope_features)
            .map(Cow::into_owned)
    }

    pub(crate) fn decode_message_borrowed<'a>(
        &mut self,
        payload: &'a [u8],
        envelope_features: WireFeatures,
    ) -> Result<Cow<'a, [u8]>, String> {
        let active = envelope_features & FEATURE_PAYLOAD_COMPRESSION_MASK;
        if active.count_ones() > 1 {
            return Err("wire frame declares more than one payload compression codec".to_owned());
        }
        if active == FEATURE_NONE {
            return Ok(Cow::Borrowed(payload));
        }
        if WireCompression::from_features(active) != self.codec {
            return Err("wire frame compression codec changed within one connection".to_owned());
        }
        let decoded = decompress_sync_payload(payload, active)?;
        if decoded.len() > crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES {
            return Err("decompressed logical message exceeds receiver budget".to_owned());
        }
        Ok(Cow::Owned(decoded))
    }
}

/// Binding-supplied byte transport for one wire-framed peer link.
///
/// Implementations own the actual socket, worker port, or host channel. The
/// core only sees already-buffered postcard frame bytes and never blocks inside
/// this trait.
pub trait WireTransport {
    /// Hand an encoded [`WireFrame`] to the binding's wire.
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError>;
    /// Pull the next encoded [`WireFrame`] staged by the binding, if any.
    fn try_recv_frame(&mut self) -> Option<Vec<u8>>;
}

impl WireTransport for Box<dyn WireTransport + Send> {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        (**self).send_frame(frame)
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        (**self).try_recv_frame()
    }
}

/// Fallible local transport result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TransportError {
    /// Bounded local queue cannot accept more data right now.
    Backpressure,
    /// The local transport failed before accepting the message.
    Failed(String),
}

/// Negotiate a common wire version and optional feature intersection.
pub fn negotiate_wire(
    remote: &WireHello,
    local_min_protocol_version: u16,
    local_max_protocol_version: u16,
    local_features: WireFeatures,
) -> Result<WireNegotiated, WireError> {
    let min = remote.min_protocol_version.max(local_min_protocol_version);
    let max = remote.max_protocol_version.min(local_max_protocol_version);
    if min > max {
        return Err(WireError::new(
            WireErrorCode::UnsupportedProtocolVersion,
            WireRetry::Never,
            format!(
                "no common wire protocol version: remote {}..={}, local {}..={}",
                remote.min_protocol_version,
                remote.max_protocol_version,
                local_min_protocol_version,
                local_max_protocol_version
            ),
        ));
    }
    let features = remote.features & local_features;
    Ok(WireNegotiated {
        protocol_version: max,
        features,
    })
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use groove::Intern;
    use groove::schema::ColumnType;
    use serde_json::json;

    use super::*;
    use crate::ids::SchemaVersionId;
    use crate::ids::{NodeUuid, RowUuid};
    use crate::protocol::{
        AuthorizationScopePurpose, AuthorizationSupportScopeKey, ChunkRequestBatch,
        ChunkRequestEntry, PermissionAdviceAction, PermissionAdviceRequestId, RegisterShapeOptions,
        ResultRowEntry, ShapeAst, Subscribe, SubscribeRejectReason, SubscriptionKey, VersionBundle,
        VersionBundleRun, VersionBundleRunError, VersionCarrier, VersionRecord,
        build_version_bundle_runs_from_singletons,
    };
    use crate::protocol_limits::{MAX_CHUNK_REQUEST_BATCH_ENTRIES, MAX_WIRE_FRAME_BYTES};
    use crate::query::{BindingId, Query, ShapeId};
    use crate::schema::{ColumnSchema, TableSchema};
    use crate::time::{GlobalTime, TxTime};
    use crate::tx::{DurabilityTier, Fate, RejectionReason, Transaction, TxId, TxKind};

    #[test]
    fn hello_json_shape_is_stable() {
        let frame = WireFrame::Hello(WireHello::current(
            WirePeerRole::Client,
            FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS,
        ));

        assert_eq!(
            serde_json::to_value(frame).unwrap(),
            json!({
                "Hello": {
                    "min_protocol_version": WIRE_PROTOCOL_VERSION,
                    "max_protocol_version": WIRE_PROTOCOL_VERSION,
                    "features": 5,
                    "role": "client",
                    "authority": null
                }
            })
        );
    }

    #[test]
    fn message_payload_round_trips_as_bytes() {
        let session = WireSession {
            session_id: "session-1".to_owned(),
            epoch: 3,
            identity: Some(AuthorSubject::for_test_bytes([0x42; 16])),
        };
        let frame = WireFrame::Message(
            WireEnvelope::new(1, FEATURE_SESSION_FRAME, vec![1, 2, 3, 4])
                .with_session(session.clone()),
        );

        let encoded = serde_json::to_vec(&frame).unwrap();
        let decoded: WireFrame = serde_json::from_slice(&encoded).unwrap();

        assert_eq!(
            decoded,
            WireFrame::Message(
                WireEnvelope::new(1, FEATURE_SESSION_FRAME, vec![1, 2, 3, 4]).with_session(session)
            )
        );
    }

    #[test]
    fn wire_payload_debug_is_bounded_and_content_safe() {
        let secret = b"do-not-log-wire-payload";
        let envelope = WireEnvelope::new(1, 0, vec![b'x'; 1_000_000]);
        let envelope_debug = format!("{envelope:?}");
        assert_eq!(
            envelope_debug,
            "WireEnvelope { protocol_version: 1, features: 0, session: None, payload_len: 1000000 }"
        );
        assert!(envelope_debug.len() < 128);
        assert!(!envelope_debug.contains(std::str::from_utf8(secret).unwrap()));

        let fragment = WireMessageFragment {
            protocol_version: 1,
            features: 0,
            session: None,
            message_id: 7,
            message_digest: [0xab; 32],
            total_len: 1_000_000,
            offset: 0,
            payload: vec![b'x'; 1_000_000],
        };
        let fragment_debug = format!("{fragment:?}");
        assert!(fragment_debug.contains("message_digest: \"abab"));
        assert!(fragment_debug.contains("payload_len: 1000000"));
        assert!(fragment_debug.len() < 256);
        assert!(!fragment_debug.contains("xxxxx"));
    }

    #[test]
    fn wire_session_debug_stays_bounded_when_nested_in_payload_frames() {
        let session = WireSession {
            session_id: "credential-bearing-session-id".repeat(10_000),
            epoch: 3,
            identity: Some(AuthorSubject::for_test_bytes([0x42; 16])),
        };
        let distinct_session = WireSession {
            session_id: "credential-bearing-session-ix".repeat(10_000),
            ..session.clone()
        };
        let session_debug = format!("{session:?}");
        assert!(session_debug.contains("session_id_len: 290000"));
        assert!(session_debug.len() < 256);
        assert!(!session_debug.contains("credential-bearing-session-id"));
        assert_ne!(session_debug, format!("{distinct_session:?}"));

        let envelope = WireEnvelope::new(1, 0, vec![]).with_session(session.clone());
        let envelope_debug = format!("{envelope:?}");
        assert!(envelope_debug.len() < 384);
        assert!(!envelope_debug.contains("credential-bearing-session-id"));

        let fragment = WireMessageFragment {
            protocol_version: 1,
            features: 0,
            session: Some(session),
            message_id: 7,
            message_digest: [0xab; 32],
            total_len: 0,
            offset: 0,
            payload: vec![],
        };
        let fragment_debug = format!("{fragment:?}");
        assert!(fragment_debug.len() < 512);
        assert!(!fragment_debug.contains("credential-bearing-session-id"));
    }

    #[test]
    fn frame_round_trips_through_postcard_codec() {
        let frame = WireFrame::Error(WireError::new(
            WireErrorCode::Backpressure,
            WireRetry::Later,
            "receiver overloaded",
        ));

        let encoded = encode_frame(&frame).unwrap();
        let decoded = decode_frame(&encoded).unwrap();

        assert_eq!(decoded, frame);
    }

    #[test]
    fn oversized_wire_frame_rejects_before_postcard_decode() {
        let oversized = vec![0_u8; MAX_WIRE_FRAME_BYTES + 1];

        assert!(decode_frame(&oversized).is_err());
    }

    #[test]
    fn sync_message_round_trips_through_postcard_codec() {
        let tx_id = TxId::new(TxTime(12), NodeUuid::from_bytes([0x11; 16]));
        let message = SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::Cascade { root: tx_id }),
            global_time: Some(GlobalTime(7)),
            durability: Some(DurabilityTier::Global),
        };

        let encoded = encode_sync_message(&message).unwrap();
        let decoded = decode_sync_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    // This stays a codec-level test: exact transport bytes are intentionally
    // not observable through the public database API.  The fixture is written
    // independently of the encoder call so a field reordering, enum-tag drift,
    // UUID-width change, or fate/durability swap cannot update its own oracle.
    #[test]
    fn transaction_fate_receipt_has_one_canonical_postcard_spelling() {
        const ACCEPTED_GLOBAL_RECEIPT_HEX: &str =
            "040c10111111111111111111111111111111110101070103";
        let tx_id = TxId::new(TxTime(12), NodeUuid::from_bytes([0x11; 16]));
        let expected = SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(7)),
            durability: Some(DurabilityTier::Global),
        };
        let fixture = hex::decode(ACCEPTED_GLOBAL_RECEIPT_HEX).expect("fixture hex");

        // semantic -> bytes
        assert_eq!(encode_sync_message(&expected).unwrap(), fixture);
        // independent bytes -> semantic
        assert_eq!(decode_sync_message(&fixture).unwrap(), expected);

        // Sensitivity plant: the final enum tag is durability.  A receiver
        // must not silently retain Global when a payload says Edge.
        let mut edge = fixture.clone();
        *edge.last_mut().expect("non-empty fixture") = 2;
        assert_eq!(
            decode_sync_message(&edge).unwrap(),
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(7)),
                durability: Some(DurabilityTier::Edge),
            }
        );
    }

    #[test]
    fn transaction_fate_receipt_rejects_trailing_and_noncanonical_bytes() {
        let canonical =
            hex::decode("040c10111111111111111111111111111111110101070103").expect("fixture hex");

        let mut trailing = canonical.clone();
        trailing.push(0);
        assert!(decode_sync_message(&trailing).is_err());

        // `12` has the one-byte varint spelling `0x0c`; `0x8c, 0x00` is the
        // same number in a permissive LEB128 decoder.  Whether postcard
        // rejects it directly or after decode, Jazz must reject it.
        let mut noncanonical = canonical;
        noncanonical.splice(1..2, [0x8c, 0x00]);
        assert!(decode_sync_message(&noncanonical).is_err());
    }

    #[test]
    fn oversized_chunk_request_batches_are_rejected_during_decode() {
        let request = |request_id| ChunkRequestEntry {
            request_id,
            locator: groove::large_values::Locator::random(),
            expected_hash: [0x22; 32],
            remaining_hops: 1,
        };
        let at_limit = SyncMessage::ChunkRequestBatch(ChunkRequestBatch {
            requests: (0..MAX_CHUNK_REQUEST_BATCH_ENTRIES as u64)
                .map(request)
                .collect(),
        });
        let encoded = encode_sync_message(&at_limit).expect("encode limit request fixture");
        assert_eq!(
            decode_sync_message(&encoded).expect("limit request fixture remains valid"),
            at_limit
        );

        let mut requests = (0..MAX_CHUNK_REQUEST_BATCH_ENTRIES as u64)
            .map(request)
            .collect::<Vec<_>>();
        requests.push(request(MAX_CHUNK_REQUEST_BATCH_ENTRIES as u64));
        let over_limit = SyncMessage::ChunkRequestBatch(ChunkRequestBatch { requests });
        let encoded = encode_sync_message(&over_limit).expect("encode oversized request fixture");
        assert!(
            decode_sync_message(&encoded).is_err(),
            "remote request cardinality must be bounded before storage work"
        );
    }

    #[test]
    fn authorization_scope_view_has_a_nonrecursive_view_update_payload() {
        let view = crate::protocol::ViewUpdatePayload::from_view_update(view_update_with_carriers(
            Vec::new(),
        ))
        .expect("fixture is a view update");
        let message = SyncMessage::AuthorizationScopeView {
            request_id: PermissionAdviceRequestId([0x11; 16]),
            key: AuthorizationSupportScopeKey {
                support_shape_digest: [0x22; 32],
                subject: AuthorSubject::for_test_bytes([0x33; 16]),
                claims_digest: [0x44; 32],
                policy_digest: [0x55; 32],
            },
            clause_index: 0,
            clause_count: 1,
            view,
        };
        let encoded = encode_sync_message(&message).expect("encode scope view fixture");
        assert_eq!(
            decode_sync_message(&encoded).expect("decode scope view fixture"),
            message,
            "the scope wrapper admits only its dedicated, non-recursive payload"
        );
    }

    #[test]
    fn chunk_request_locator_decode_requires_exactly_256_bits() {
        #[derive(serde::Serialize)]
        struct RawChunkRequestBatch {
            requests: Vec<RawChunkRequestEntry>,
        }
        #[derive(serde::Serialize)]
        struct RawChunkRequestEntry {
            request_id: u64,
            locator: Vec<u8>,
            expected_hash: [u8; 32],
            remaining_hops: u8,
        }

        for length in [31, 32, 33] {
            let encoded = postcard::to_allocvec(&RawChunkRequestBatch {
                requests: vec![RawChunkRequestEntry {
                    request_id: 1,
                    locator: vec![0x11; length],
                    expected_hash: [0x22; 32],
                    remaining_hops: 1,
                }],
            })
            .unwrap();
            assert_eq!(
                postcard::from_bytes::<ChunkRequestBatch>(&encoded).is_ok(),
                length == groove::large_values::LOCATOR_BYTES,
                "locator length {length} must {}decode",
                if length == groove::large_values::LOCATOR_BYTES {
                    ""
                } else {
                    "not "
                }
            );
        }
    }

    #[test]
    fn shared_view_update_payload_preserves_postcard_shape() {
        #[allow(dead_code)]
        #[derive(serde::Serialize)]
        enum LegacySyncMessage {
            V0,
            V1,
            V2,
            V3,
            V4,
            V5,
            V6,
            V7,
            V8,
            V9,
            V10,
            V11,
            V12,
            V13,
            ViewUpdate {
                subscription: SubscriptionKey,
                settled_through: GlobalTime,
                reset_result_set: bool,
                version_carriers: Vec<VersionCarrier>,
                version_bundles: Vec<VersionBundle>,
                peer_payload_inventory: crate::protocol::PeerPayloadInventory,
                result_member_adds: Vec<crate::protocol::ResultMemberEntry>,
                result_member_removes: Vec<crate::protocol::ResultMemberEntry>,
                terminal_operations: Vec<groove::ivm::TerminalOperation>,
                program_fact_adds: Vec<crate::protocol::ProgramFactEntry>,
                program_fact_removes: Vec<crate::protocol::ProgramFactEntry>,
            },
        }

        let SyncMessage::ViewUpdate(payload) = view_update_with_carriers(Vec::new()) else {
            unreachable!()
        };
        let legacy = LegacySyncMessage::ViewUpdate {
            subscription: payload.subscription,
            settled_through: payload.settled_through,
            reset_result_set: payload.reset_result_set,
            version_carriers: payload.version_carriers.clone(),
            version_bundles: payload.version_bundles.clone(),
            peer_payload_inventory: payload.peer_payload_inventory.clone(),
            result_member_adds: payload.result_member_adds.clone(),
            result_member_removes: payload.result_member_removes.clone(),
            terminal_operations: payload.terminal_operations.clone(),
            program_fact_adds: payload.program_fact_adds.clone(),
            program_fact_removes: payload.program_fact_removes.clone(),
        };
        let current = SyncMessage::ViewUpdate(payload);

        assert_eq!(
            encode_sync_message(&current).unwrap(),
            postcard::to_allocvec(&legacy).unwrap(),
            "a newtype struct is postcard-transparent relative to the former struct variant"
        );
    }

    #[test]
    fn view_update_mixed_version_carrier_runs_round_trip_and_survive_receive_decode() {
        let bundles = version_bundles(4);
        let singleton_run = VersionCarrier::Run(
            build_version_bundle_runs_from_singletons(&bundles[..1])
                .unwrap()
                .remove(0),
        );
        let multi_run = VersionCarrier::Run(
            build_version_bundle_runs_from_singletons(&bundles[1..])
                .unwrap()
                .remove(0),
        );
        let message = view_update_with_carriers(vec![singleton_run, multi_run]);

        let encoded = encode_sync_message(&message).unwrap();
        assert_eq!(decode_sync_message(&encoded).unwrap(), message);

        assert_eq!(decode_sync_message_for_receive(&encoded).unwrap(), message);
        let expanded = message.expand_version_carriers_for_receive().unwrap();
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            version_carriers,
            version_bundles,
            ..
        }) = expanded
        else {
            panic!("expected view update");
        };
        assert!(version_carriers.is_empty());
        assert_eq!(version_bundles, bundles);
    }

    #[test]
    fn large_version_carrier_run_round_trips() {
        let bundles = version_bundles(128);
        let run = build_version_bundle_runs_from_singletons(&bundles)
            .unwrap()
            .remove(0);
        let message = view_update_with_carriers(vec![VersionCarrier::Run(run)]);
        let encoded = encode_sync_message(&message).unwrap();

        assert_eq!(decode_sync_message_for_receive(&encoded).unwrap(), message);
        let expanded = message.expand_version_carriers_for_receive().unwrap();
        let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            version_bundles, ..
        }) = expanded
        else {
            panic!("expected view update");
        };
        assert_eq!(version_bundles, bundles);
    }

    #[test]
    fn malformed_version_carrier_run_body_count_is_rejected() {
        let mut run = build_version_bundle_runs_from_singletons(&version_bundles(2))
            .unwrap()
            .remove(0);
        run.header.body_count = 3;

        assert_eq!(
            run.validate(),
            Err(VersionBundleRunError::BodyCountMismatch {
                declared: 3,
                actual: 2
            })
        );
        assert!(encode_then_decode_run(run).is_err());
    }

    #[test]
    fn malformed_version_carrier_run_is_rejected_in_ordinary_and_scope_views() {
        let mut run = build_version_bundle_runs_from_singletons(&version_bundles(2))
            .unwrap()
            .remove(0);
        run.header.body_count = 3;

        let ordinary = view_update_with_carriers(vec![VersionCarrier::Run(run.clone())]);
        let scope_view = SyncMessage::AuthorizationScopeView {
            request_id: PermissionAdviceRequestId([0x11; 16]),
            key: AuthorizationSupportScopeKey {
                support_shape_digest: [0x22; 32],
                subject: AuthorSubject::for_test_bytes([0x33; 16]),
                claims_digest: [0x44; 32],
                policy_digest: [0x55; 32],
            },
            clause_index: 0,
            clause_count: 1,
            view: crate::protocol::ViewUpdatePayload::from_view_update(view_update_with_carriers(
                vec![VersionCarrier::Run(run)],
            ))
            .expect("fixture is a view update"),
        };

        for message in [ordinary, scope_view] {
            let encoded = encode_sync_message(&message).expect("encode malformed fixture");
            assert!(
                decode_sync_message(&encoded).is_err(),
                "malformed runs must be rejected at either view-update seam"
            );
        }
    }

    #[test]
    fn malformed_version_carrier_run_override_index_is_rejected() {
        let mut run = build_version_bundle_runs_from_singletons(&version_bundles(2))
            .unwrap()
            .remove(0);
        run.overrides
            .push(crate::protocol::VersionBundleRunOverride {
                body_index: 2,
                tx: None,
                scope: None,
                fate: Some(Fate::Pending),
                global_time: None,
                durability: None,
            });

        assert_eq!(
            run.validate(),
            Err(VersionBundleRunError::OverrideIndexOutOfRange {
                index: 2,
                body_count: 2
            })
        );
        assert!(encode_then_decode_run(run).is_err());
    }

    #[test]
    fn build_expand_version_carrier_run_preserves_singletons() {
        let bundles = version_bundles(6);
        let run = build_version_bundle_runs_from_singletons(&bundles)
            .unwrap()
            .remove(0);

        assert_eq!(run.expand().unwrap(), bundles);
        assert_eq!(
            VersionCarrier::Run(run).expand().unwrap(),
            bundles,
            "expanded run applies the same singleton carrier sequence at the type level"
        );
    }

    fn encode_then_decode_run(run: VersionBundleRun) -> Result<SyncMessage, postcard::Error> {
        let message = view_update_with_carriers(vec![VersionCarrier::Run(run)]);
        decode_sync_message(&encode_sync_message(&message).unwrap())
    }

    fn view_update_with_carriers(version_carriers: Vec<VersionCarrier>) -> SyncMessage {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription: SubscriptionKey {
                shape_id: ShapeId(uuid::Uuid::from_bytes([0x22; 16])),
                binding_id: BindingId(uuid::Uuid::from_bytes([0x33; 16])),
                read_view: Default::default(),
            },
            settled_through: GlobalTime(500),
            reset_result_set: false,
            version_carriers,
            version_bundles: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
    }

    fn version_bundles(count: usize) -> Vec<VersionBundle> {
        let table = TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]);
        let schema_version = SchemaVersionId::from_bytes([0x44; 16]);
        let node = NodeUuid::from_bytes([0x11; 16]);
        let author = AuthorSubject::for_test_bytes([0x55; 16]);
        (0..count)
            .map(|index| {
                let tx_id = TxId::new(TxTime(1_000 + index as u64), node);
                VersionBundle {
                    tx: Transaction {
                        tx_id,
                        kind: TxKind::Mergeable,
                        n_total_writes: 1,
                        made_by: author,
                        permission_subject: None,
                        base_snapshot: None,
                        row_read_set: None,
                        absent_read_set: None,
                        predicate_read_set: None,
                        user_metadata_json: None,
                        contribution_merge: None,
                    },
                    versions: vec![
                        VersionRecord::from_cells(
                            &table,
                            schema_version,
                            RowUuid::from_bytes([index as u8; 16]),
                            Vec::new(),
                            author,
                            1_000 + index as u64,
                            author,
                            1_000 + index as u64,
                            &BTreeMap::from([("title".to_owned(), format!("todo-{index}"))]),
                            None,
                        )
                        .unwrap(),
                    ],
                    scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                    fate: Fate::Accepted,
                    global_time: Some(GlobalTime(10_000 + index as u64)),
                    // A sequence is the global-authority receipt, and so its
                    // companion durability is Global in every valid fixture.
                    durability: DurabilityTier::Global,
                }
            })
            .collect()
    }

    #[cfg(all(not(target_arch = "wasm32"), feature = "transport-compression-zstd"))]
    #[test]
    fn native_default_transport_compression_advertises_zstd() {
        assert_eq!(
            default_transport_compression_features(),
            FEATURE_PAYLOAD_ZSTD
        );
    }

    #[test]
    fn uncompressed_stream_round_trips_message_boundaries() {
        let mut encoder = WireStreamEncoder::new(FEATURE_NONE).unwrap();
        let mut decoder = WireStreamDecoder::new(FEATURE_NONE).unwrap();
        let first = vec![1, 2, 3];
        let second = vec![4, 5];

        let encoded_first = encoder.encode_message(&first).unwrap();
        let encoded_second = encoder.encode_message(&second).unwrap();

        assert_eq!(
            decoder
                .decode_message(&encoded_first, FEATURE_NONE)
                .unwrap(),
            first
        );
        assert_eq!(
            decoder
                .decode_message(&encoded_second, FEATURE_NONE)
                .unwrap(),
            second
        );
    }

    #[test]
    fn uncompressed_stream_decoder_borrows_payload() {
        let mut decoder = WireStreamDecoder::new(FEATURE_NONE).unwrap();
        let message = b"uncompressed logical message".to_vec();

        let decoded = decoder
            .decode_message_borrowed(&message, FEATURE_NONE)
            .unwrap();

        assert_eq!(decoded.as_ptr(), message.as_ptr());
    }

    #[cfg(feature = "transport-compression-zstd")]
    #[test]
    fn compressed_stream_decoder_accepts_raw_envelopes() {
        let mut decoder = WireStreamDecoder::new(FEATURE_PAYLOAD_ZSTD).unwrap();
        let message = b"client hello without outbound zstd encoder".to_vec();

        assert_eq!(
            decoder.decode_message(&message, FEATURE_NONE).unwrap(),
            message
        );
    }

    #[cfg(feature = "transport-compression-zstd")]
    #[test]
    fn zstd_stream_round_trips_multiple_message_boundaries() {
        let mut encoder = WireStreamEncoder::new(FEATURE_PAYLOAD_ZSTD).unwrap();
        let mut decoder = WireStreamDecoder::new(FEATURE_PAYLOAD_ZSTD).unwrap();
        let messages = [
            b"alpha alpha alpha".to_vec(),
            b"alpha alpha beta".to_vec(),
            b"alpha alpha gamma".to_vec(),
        ];

        for message in messages {
            let chunk = encoder.encode_message(&message).unwrap();
            let decoded = decoder
                .decode_message(&chunk, FEATURE_PAYLOAD_ZSTD)
                .unwrap();
            assert_eq!(decoded, message);
        }
    }

    #[cfg(feature = "transport-compression-lz4")]
    #[test]
    fn lz4_stream_round_trips_multiple_message_boundaries() {
        let mut encoder = WireStreamEncoder::new(FEATURE_PAYLOAD_LZ4).unwrap();
        let mut decoder = WireStreamDecoder::new(FEATURE_PAYLOAD_LZ4).unwrap();
        let messages = [
            b"alpha alpha alpha".to_vec(),
            b"alpha alpha beta".to_vec(),
            b"alpha alpha gamma".to_vec(),
        ];

        for message in messages {
            let chunk = encoder.encode_message(&message).unwrap();
            let decoded = decoder.decode_message(&chunk, FEATURE_PAYLOAD_LZ4).unwrap();
            assert_eq!(decoded, message);
        }
    }

    #[cfg(all(
        feature = "transport-compression-lz4",
        feature = "transport-compression-zstd"
    ))]
    #[test]
    fn synthetic_small_delta_streaming_compression_receipt() {
        jazz_benchmark_guard::refuse_contaminated_measurement();
        let shape_id = ShapeId(uuid::Uuid::from_bytes([0x22; 16]));
        let binding_id = BindingId(uuid::Uuid::from_bytes([0x33; 16]));
        let subscription = crate::protocol::SubscriptionKey {
            shape_id,
            binding_id,
            read_view: Default::default(),
        };
        let node = NodeUuid::from_bytes([0x44; 16]);
        let schema_version = SchemaVersionId::from_bytes([0x55; 16]);
        let messages = (0..300_u64)
            .map(|i| {
                let row = crate::ids::RowUuid(uuid::Uuid::from_u128(0x7000_0000_0000 + i as u128));
                let tx = TxId::new(TxTime(1_000_000 + i), node);
                let member =
                    crate::protocol::ResultMemberEntry::Row(crate::protocol::RealRowMemberEntry {
                        table: groove::Intern::new("res_l_child_3".to_owned()),
                        row_uuid: row,
                        occurrence_id: Some(crate::tools::OutputOccurrenceId::single_source(
                            crate::tools::ObjectId::from_uuid(row.0),
                        )),
                        content_tx: Some(tx),
                        layer: Default::default(),
                        deletion_tx: None,
                        source: Default::default(),
                        read_view: Default::default(),
                        schema_version: Some(schema_version),
                        branch_or_prefix: None,
                        row_digest: Some(vec![0xAB; 8]),
                        batch: Some(tx),
                        settle_position: Some(GlobalTime(10_000 + i)),
                    });
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    settled_through: GlobalTime(10_000 + i),
                    reset_result_set: false,
                    version_carriers: Vec::new(),
                    version_bundles: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_member_adds: vec![member],
                    result_member_removes: Vec::new(),
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                    terminal_operations: Vec::new(),
                })
            })
            .collect::<Vec<_>>();
        let mut raw = 0_u64;
        let mut per_message_zstd = 0_u64;
        let mut streaming_zstd = 0_u64;
        let mut streaming_lz4 = 0_u64;
        let mut zstd_encoder = WireStreamEncoder::new(FEATURE_PAYLOAD_ZSTD).unwrap();
        let mut zstd_decoder = WireStreamDecoder::new(FEATURE_PAYLOAD_ZSTD).unwrap();
        let mut lz4_encoder = WireStreamEncoder::new(FEATURE_PAYLOAD_LZ4).unwrap();
        let mut lz4_decoder = WireStreamDecoder::new(FEATURE_PAYLOAD_LZ4).unwrap();
        for message in &messages {
            let payload = encode_sync_message(message).unwrap();
            raw += payload.len() as u64;
            let (compressed, active) =
                compress_sync_payload(payload.clone(), FEATURE_PAYLOAD_ZSTD).unwrap();
            let decompressed = decompress_sync_payload(&compressed, active).unwrap();
            assert_eq!(decompressed, payload);
            per_message_zstd += compressed.len() as u64;

            let zstd_chunk = zstd_encoder.encode_message(&payload).unwrap();
            let zstd_decoded = zstd_decoder
                .decode_message(&zstd_chunk, FEATURE_PAYLOAD_ZSTD)
                .unwrap();
            assert_eq!(zstd_decoded, payload);
            streaming_zstd += zstd_chunk.len() as u64;

            let lz4_chunk = lz4_encoder.encode_message(&payload).unwrap();
            let lz4_decoded = lz4_decoder
                .decode_message(&lz4_chunk, FEATURE_PAYLOAD_LZ4)
                .unwrap();
            assert_eq!(lz4_decoded, payload);
            streaming_lz4 += lz4_chunk.len() as u64;
        }
        eprintln!(
            "SYNTHETIC_SMALL_DELTA_COMPRESSION raw={raw} per_message_zstd={per_message_zstd} streaming_zstd={streaming_zstd} streaming_lz4={streaming_lz4}"
        );
        assert!(streaming_zstd < per_message_zstd);
    }

    #[test]
    fn message_frame_round_trips_sync_message_payload_variants() {
        let node = NodeUuid::from_bytes([0x11; 16]);
        let tx_id = TxId::new(TxTime(12), node);
        let shape_id = ShapeId(uuid::Uuid::from_bytes([0x22; 16]));
        let binding_id = BindingId(uuid::Uuid::from_bytes([0x33; 16]));
        let schema_version = SchemaVersionId::from_bytes([0x44; 16]);
        let subscription = SubscriptionKey {
            shape_id,
            binding_id,
            read_view: Default::default(),
        };
        let messages = vec![
            SyncMessage::RegisterShape {
                shape_id,
                ast: ShapeAst::new(Query::from("todos"), schema_version),
                opts: RegisterShapeOptions::default(),
            },
            SyncMessage::Subscribe(Subscribe {
                shape_id,
                subscription,
                values: Vec::new(),
                known_state: None,
            }),
            SyncMessage::SubscribeRejected {
                subscription,
                reason: SubscribeRejectReason::UnsupportedShapeCapability {
                    detail: "SourceGap::BranchOverlay".to_owned(),
                },
            },
            SyncMessage::SubscribeRejected {
                subscription,
                reason: SubscribeRejectReason::ServerFailure {
                    code: crate::protocol::SubscribeServerFailureCode::TableNotFound,
                },
            },
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(7),
                reset_result_set: true,
                version_carriers: Vec::new(),
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                    authorization_progress: None,
                    opening_pending: false,
                },
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                terminal_operations: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            }),
            SyncMessage::CommitUnit {
                tx: Transaction {
                    tx_id,
                    kind: TxKind::Mergeable,
                    n_total_writes: 0,
                    made_by: AuthorSubject::for_test_bytes([0x55; 16]),
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    contribution_merge: None,
                },
                versions: Vec::new(),
            },
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(7)),
                durability: Some(DurabilityTier::Global),
            },
            SyncMessage::FetchRowVersions {
                requests: vec![crate::protocol::RowVersionRef::new(
                    "todos",
                    RowUuid::from_bytes([0x77; 16]),
                    tx_id,
                )],
            },
            SyncMessage::RowVersionPayloads {
                version_bundles: Vec::new(),
            },
        ];

        for message in messages {
            let payload = encode_sync_message(&message).unwrap();
            let frame = WireFrame::Message(WireEnvelope::new(
                WIRE_PROTOCOL_VERSION,
                FEATURE_SYNC_MESSAGE_PAYLOAD,
                payload,
            ));

            let decoded = decode_frame(&encode_frame(&frame).unwrap()).unwrap();
            let WireFrame::Message(envelope) = decoded else {
                panic!("expected message frame");
            };

            assert_eq!(decode_sync_message(&envelope.payload).unwrap(), message);
        }
    }

    #[test]
    fn view_update_result_entries_round_trip_interned_table_names() {
        let row = RowUuid::from_bytes([0x22; 16]);
        let tx_id = TxId::new(TxTime(21), NodeUuid::from_bytes([0x33; 16]));
        let entry: ResultRowEntry = (Intern::new("todos".to_owned()), row, tx_id);
        let message = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription: SubscriptionKey {
                shape_id: ShapeId(uuid::Uuid::from_bytes([0x44; 16])),
                binding_id: BindingId(uuid::Uuid::from_bytes([0x55; 16])),
                read_view: Default::default(),
            },
            settled_through: GlobalTime(7),
            reset_result_set: true,
            version_carriers: Vec::new(),
            version_bundles: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: vec![tx_id],
                authorization_progress: None,
                opening_pending: false,
            },
            result_member_adds: vec![entry.into()],
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        });

        let encoded = encode_sync_message(&message).unwrap();
        let decoded = decode_sync_message(&encoded).unwrap();

        assert_eq!(decoded, message);
    }

    #[test]
    fn negotiation_chooses_highest_common_version_and_feature_intersection() {
        let remote = WireHello {
            min_protocol_version: 1,
            max_protocol_version: 3,
            features: FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_SESSION_FRAME,
            role: WirePeerRole::Relay,
            authority: None,
        };

        let negotiated = negotiate_wire(
            &remote,
            2,
            4,
            FEATURE_SESSION_FRAME | FEATURE_STRUCTURED_ERRORS,
        )
        .unwrap();

        assert_eq!(
            negotiated,
            WireNegotiated {
                protocol_version: 3,
                features: FEATURE_SESSION_FRAME
            }
        );
    }

    #[test]
    fn negotiation_rejects_disjoint_versions() {
        let remote = WireHello {
            min_protocol_version: 1,
            max_protocol_version: 1,
            features: FEATURE_NONE,
            role: WirePeerRole::Core,
            authority: None,
        };

        let err = negotiate_wire(&remote, 2, 2, FEATURE_NONE).unwrap_err();

        assert_eq!(err.code, WireErrorCode::UnsupportedProtocolVersion);
        assert_eq!(err.retry, WireRetry::Never);
    }

    #[test]
    fn wire_v14_rejects_v13_without_compatibility_negotiation() {
        assert_eq!(WIRE_PROTOCOL_VERSION, 14);
        let remote = WireHello {
            min_protocol_version: 13,
            max_protocol_version: 13,
            features: FEATURE_SYNC_MESSAGE_PAYLOAD,
            role: WirePeerRole::Core,
            authority: None,
        };

        let error = negotiate_wire(
            &remote,
            WIRE_PROTOCOL_VERSION,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD,
        )
        .expect_err("current wire protocol must not negotiate with an old peer");

        assert_eq!(error.code, WireErrorCode::UnsupportedProtocolVersion);
        assert_eq!(error.retry, WireRetry::Never);
    }

    #[test]
    fn wire_v14_rejects_v12_peer_before_payload_decode() {
        let v12_peer = WireHello {
            min_protocol_version: 12,
            max_protocol_version: 12,
            features: current_wire_features(),
            role: WirePeerRole::Core,
            authority: None,
        };

        let error = negotiate_wire(
            &v12_peer,
            WIRE_PROTOCOL_VERSION,
            WIRE_PROTOCOL_VERSION,
            current_wire_features(),
        )
        .expect_err("v12 encoding must fail during the v14 handshake");

        assert_eq!(WIRE_PROTOCOL_VERSION, 14);
        assert_eq!(error.code, WireErrorCode::UnsupportedProtocolVersion);
        assert_eq!(error.retry, WireRetry::Never);
    }

    #[test]
    fn negotiation_keeps_directional_scope_capability_without_remote_authority() {
        let feature = FEATURE_AUTHORIZATION_SCOPE_RECEIPTS;
        let unbound = WireHello::current(WirePeerRole::Core, feature);
        assert_eq!(
            negotiate_wire(
                &unbound,
                WIRE_PROTOCOL_VERSION,
                WIRE_PROTOCOL_VERSION,
                feature
            )
            .unwrap()
            .features
                & feature,
            feature
        );
        let accepted = WireHello::current(WirePeerRole::Core, feature)
            .with_authority(NodeUuid::from_bytes([0x71; 16]), 9);
        assert_ne!(
            negotiate_wire(
                &accepted,
                WIRE_PROTOCOL_VERSION,
                WIRE_PROTOCOL_VERSION,
                feature,
            )
            .unwrap()
            .features
                & feature,
            0
        );
    }

    #[test]
    fn authorization_scope_semantics_fail_closed_without_negotiated_feature() {
        let subscription = SubscriptionKey {
            shape_id: ShapeId(uuid::Uuid::from_bytes([1; 16])),
            binding_id: BindingId(uuid::Uuid::from_bytes([2; 16])),
            read_view: Default::default(),
        };
        let message = SyncMessage::AuthorizationScopeSubscribe {
            subscribe: Subscribe {
                shape_id: subscription.shape_id,
                subscription,
                values: Vec::new(),
                known_state: None,
            },
            purpose: AuthorizationScopePurpose {
                action: PermissionAdviceAction::Read {
                    table: "todos".to_owned(),
                    row: RowUuid::from_bytes([7; 16]),
                },
            },
        };
        let old_features = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS;
        assert_eq!(
            encode_sync_message_for_features(&message, old_features)
                .unwrap_err()
                .code,
            WireErrorCode::UnsupportedFeature
        );

        let encoded = encode_sync_message_for_features(
            &message,
            old_features | FEATURE_AUTHORIZATION_SCOPE_RECEIPTS,
        )
        .unwrap();
        assert_eq!(
            decode_sync_message_for_features(&encoded, old_features)
                .unwrap_err()
                .code,
            WireErrorCode::UnsupportedFeature
        );
    }
}
