//! Versioned transport frames around Jazz sync semantics.
//!
//! The wire layer is intentionally thinner than [`crate::protocol`]: it owns
//! link/session negotiation, feature discovery, binary framing, and structured
//! protocol errors. The frame payload is opaque bytes for now so bindings and
//! server shells can adopt the envelope before the full [`crate::protocol::SyncMessage`]
//! encoder is frozen.

#[cfg(feature = "transport-compression-lz4")]
use std::io::Read;
#[cfg(any(
    feature = "transport-compression-lz4",
    feature = "transport-compression-zstd"
))]
use std::io::Write;

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::ids::AuthorId;
use crate::protocol::SyncMessage;
use crate::protocol_limits::{validate_sync_message_len, validate_wire_frame_len};

/// Current Jazz wire protocol version.
pub const WIRE_PROTOCOL_VERSION: u16 = 2;

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
}

impl WireHello {
    /// Construct a hello frame for the current implementation.
    pub fn current(role: WirePeerRole, features: WireFeatures) -> Self {
        Self {
            min_protocol_version: WIRE_PROTOCOL_VERSION,
            max_protocol_version: WIRE_PROTOCOL_VERSION,
            features,
            role,
        }
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireSession {
    /// Binding/server assigned resumable session id.
    pub session_id: String,
    /// Monotone session incarnation. Reconnects that abandon prior ordering use a new epoch.
    pub epoch: u64,
    /// Authenticated user identity for edge/client links, once admission succeeds.
    pub identity: Option<AuthorId>,
}

/// Metadata and payload for one semantic sync message.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
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
    from_bytes(bytes)
}

/// Serialize a semantic sync message with the canonical Jazz payload codec.
pub fn encode_sync_message(message: &SyncMessage) -> Result<Vec<u8>, postcard::Error> {
    to_allocvec(message)
}

/// Decode a semantic sync message serialized by [`encode_sync_message`].
pub fn decode_sync_message(bytes: &[u8]) -> Result<SyncMessage, postcard::Error> {
    if validate_sync_message_len(bytes.len()).is_err() {
        return Err(postcard::Error::DeserializeUnexpectedEnd);
    }
    from_bytes(bytes)
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
/// This remains available for measurement compatibility. Production peer links
/// use [`WireStreamEncoder`] so the dictionary/window lives for the whole
/// negotiated connection direction and resets only on reconnect.
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

#[cfg(feature = "transport-compression-lz4")]
fn compress_lz4(payload: &[u8]) -> Result<Vec<u8>, String> {
    Ok(lz4_flex::compress_prepend_size(payload))
}

#[cfg(not(feature = "transport-compression-lz4"))]
fn compress_lz4(_payload: &[u8]) -> Result<Vec<u8>, String> {
    Err("lz4 transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "transport-compression-lz4")]
fn decompress_lz4(payload: &[u8]) -> Result<Vec<u8>, String> {
    lz4_flex::decompress_size_prepended(payload)
        .map_err(|error| format!("failed to decompress lz4 payload: {error}"))
}

#[cfg(not(feature = "transport-compression-lz4"))]
fn decompress_lz4(_payload: &[u8]) -> Result<Vec<u8>, String> {
    Err("lz4 transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "transport-compression-zstd")]
fn compress_zstd(payload: &[u8]) -> Result<Vec<u8>, String> {
    zstd::bulk::compress(payload, 3)
        .map_err(|error| format!("failed to compress zstd payload: {error}"))
}

#[cfg(not(feature = "transport-compression-zstd"))]
fn compress_zstd(_payload: &[u8]) -> Result<Vec<u8>, String> {
    Err("zstd transport compression feature is not compiled in".to_owned())
}

#[cfg(any(
    feature = "transport-compression-zstd",
    feature = "transport-compression-ruzstd"
))]
fn decompress_zstd(payload: &[u8]) -> Result<Vec<u8>, String> {
    #[cfg(feature = "transport-compression-zstd")]
    {
        zstd::bulk::decompress(payload, crate::protocol_limits::MAX_SYNC_MESSAGE_BYTES)
            .map_err(|error| format!("failed to decompress zstd payload: {error}"))
    }
    #[cfg(all(
        not(feature = "transport-compression-zstd"),
        feature = "transport-compression-ruzstd"
    ))]
    {
        let mut decoder = ruzstd::decoding::FrameDecoder::new();
        let mut output = Vec::with_capacity(crate::protocol_limits::MAX_SYNC_MESSAGE_BYTES);
        decoder
            .decode_all_to_vec(payload, &mut output)
            .map_err(|error| format!("failed to decompress ruzstd payload: {error}"))?;
        Ok(output)
    }
}

#[cfg(not(any(
    feature = "transport-compression-zstd",
    feature = "transport-compression-ruzstd"
)))]
fn decompress_zstd(_payload: &[u8]) -> Result<Vec<u8>, String> {
    Err("zstd transport compression feature is not compiled in".to_owned())
}

/// Connection-direction compression state for sync message payloads.
///
/// Compressed links write `u32 little-endian length || sync_payload` records into
/// one codec stream. The state is intentionally per connection direction:
/// reconnect creates a fresh stream context, and known-state redelivery makes
/// that reset protocol-safe.
pub struct WireStreamEncoder {
    codec: WireCompression,
    inner: WireStreamEncoderInner,
}

enum WireStreamEncoderInner {
    None,
    #[cfg(feature = "transport-compression-lz4")]
    Lz4(lz4_flex::frame::FrameEncoder<Vec<u8>>),
    #[cfg(feature = "transport-compression-zstd")]
    Zstd(zstd::stream::write::Encoder<'static, Vec<u8>>),
}

impl WireStreamEncoder {
    /// Create encoder state for one outbound connection direction.
    pub fn new(features: WireFeatures) -> Result<Self, String> {
        let codec = outbound_wire_compression_from_features(features);
        let inner = match codec {
            WireCompression::None => WireStreamEncoderInner::None,
            WireCompression::Lz4 => new_lz4_stream_encoder()?,
            WireCompression::Zstd => new_zstd_stream_encoder()?,
        };
        Ok(Self { codec, inner })
    }

    /// Active feature bit carried by message envelopes for this stream.
    pub fn active_feature(&self) -> WireFeatures {
        self.codec.feature()
    }

    /// Encode one sync payload into the connection stream and return the bytes
    /// newly emitted by this message.
    pub fn encode_message(&mut self, payload: &[u8]) -> Result<Vec<u8>, String> {
        match &mut self.inner {
            WireStreamEncoderInner::None => Ok(payload.to_vec()),
            #[cfg(feature = "transport-compression-lz4")]
            WireStreamEncoderInner::Lz4(encoder) => {
                let start = encoder.get_ref().len();
                write_length_prefixed_payload(encoder, payload)?;
                encoder
                    .flush()
                    .map_err(|error| format!("failed to flush lz4 stream: {error}"))?;
                Ok(encoder.get_ref()[start..].to_vec())
            }
            #[cfg(feature = "transport-compression-zstd")]
            WireStreamEncoderInner::Zstd(encoder) => {
                let start = encoder.get_ref().len();
                write_length_prefixed_payload(encoder, payload)?;
                encoder
                    .flush()
                    .map_err(|error| format!("failed to flush zstd stream: {error}"))?;
                Ok(encoder.get_ref()[start..].to_vec())
            }
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

#[cfg(any(
    feature = "transport-compression-lz4",
    feature = "transport-compression-zstd"
))]
fn write_length_prefixed_payload(writer: &mut impl Write, payload: &[u8]) -> Result<(), String> {
    let len = u32::try_from(payload.len())
        .map_err(|_| "sync payload is too large for transport stream frame".to_owned())?;
    writer
        .write_all(&len.to_le_bytes())
        .and_then(|_| writer.write_all(payload))
        .map_err(|error| format!("failed to write transport stream payload: {error}"))
}

#[cfg(feature = "transport-compression-lz4")]
fn new_lz4_stream_encoder() -> Result<WireStreamEncoderInner, String> {
    Ok(WireStreamEncoderInner::Lz4(
        lz4_flex::frame::FrameEncoder::new(Vec::new()),
    ))
}

#[cfg(not(feature = "transport-compression-lz4"))]
fn new_lz4_stream_encoder() -> Result<WireStreamEncoderInner, String> {
    Err("lz4 transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "transport-compression-zstd")]
fn new_zstd_stream_encoder() -> Result<WireStreamEncoderInner, String> {
    zstd::stream::write::Encoder::new(Vec::new(), 3)
        .map(WireStreamEncoderInner::Zstd)
        .map_err(|error| format!("failed to create zstd stream encoder: {error}"))
}

#[cfg(not(feature = "transport-compression-zstd"))]
fn new_zstd_stream_encoder() -> Result<WireStreamEncoderInner, String> {
    Err("zstd transport compression feature is not compiled in".to_owned())
}

/// Connection-direction decompression state for sync message payloads.
pub struct WireStreamDecoder {
    codec: WireCompression,
    inner: WireStreamDecoderInner,
}

enum WireStreamDecoderInner {
    None,
    #[cfg(feature = "transport-compression-lz4")]
    Lz4 {
        compressed: Vec<u8>,
        plain_consumed: usize,
    },
    #[cfg(feature = "transport-compression-zstd")]
    Zstd {
        decoder: zstd::stream::write::Decoder<'static, Vec<u8>>,
        plain_consumed: usize,
    },
    #[cfg(all(
        not(feature = "transport-compression-zstd"),
        feature = "transport-compression-ruzstd"
    ))]
    ZstdRuzstd {
        compressed: Vec<u8>,
        plain: Vec<u8>,
        plain_consumed: usize,
    },
}

impl WireStreamDecoder {
    /// Create decoder state for one inbound connection direction.
    pub fn new(features: WireFeatures) -> Result<Self, String> {
        let codec = WireCompression::from_features(features);
        let inner = match codec {
            WireCompression::None => WireStreamDecoderInner::None,
            WireCompression::Lz4 => new_lz4_stream_decoder()?,
            WireCompression::Zstd => new_zstd_stream_decoder()?,
        };
        Ok(Self { codec, inner })
    }

    /// Decode one message's stream chunk into one semantic sync payload.
    pub fn decode_message(
        &mut self,
        payload: &[u8],
        envelope_features: WireFeatures,
    ) -> Result<Vec<u8>, String> {
        let active = envelope_features & FEATURE_PAYLOAD_COMPRESSION_MASK;
        if active.count_ones() > 1 {
            return Err("wire frame declares more than one payload compression codec".to_owned());
        }
        if active == FEATURE_NONE {
            return Ok(payload.to_vec());
        }
        if WireCompression::from_features(active) != self.codec {
            return Err("wire frame compression codec changed within one connection".to_owned());
        }
        match &mut self.inner {
            WireStreamDecoderInner::None => Ok(payload.to_vec()),
            #[cfg(feature = "transport-compression-lz4")]
            WireStreamDecoderInner::Lz4 {
                compressed,
                plain_consumed,
            } => {
                compressed.extend_from_slice(payload);
                let mut decoder = lz4_flex::frame::FrameDecoder::new(&compressed[..]);
                let mut plain = Vec::new();
                decoder
                    .read_to_end(&mut plain)
                    .map_err(|error| format!("failed to decompress lz4 stream: {error}"))?;
                read_next_stream_payload(&plain, plain_consumed)
            }
            #[cfg(feature = "transport-compression-zstd")]
            WireStreamDecoderInner::Zstd {
                decoder,
                plain_consumed,
            } => {
                decoder
                    .write_all(payload)
                    .and_then(|_| decoder.flush())
                    .map_err(|error| format!("failed to decompress zstd stream: {error}"))?;
                read_next_stream_payload(decoder.get_ref(), plain_consumed)
            }
            #[cfg(all(
                not(feature = "transport-compression-zstd"),
                feature = "transport-compression-ruzstd"
            ))]
            WireStreamDecoderInner::ZstdRuzstd {
                compressed,
                plain,
                plain_consumed,
            } => {
                compressed.extend_from_slice(payload);
                let mut decoder = ruzstd::decoding::FrameDecoder::new();
                plain.clear();
                plain.reserve(crate::protocol_limits::MAX_SYNC_MESSAGE_BYTES);
                decoder
                    .decode_all_to_vec(compressed, plain)
                    .map_err(|error| format!("failed to decompress ruzstd stream: {error}"))?;
                read_next_stream_payload(plain, plain_consumed)
            }
        }
    }
}

#[cfg(any(
    feature = "transport-compression-lz4",
    feature = "transport-compression-zstd",
    feature = "transport-compression-ruzstd"
))]
fn read_next_stream_payload(plain: &[u8], plain_consumed: &mut usize) -> Result<Vec<u8>, String> {
    let remaining = plain
        .get(*plain_consumed..)
        .ok_or_else(|| "transport stream consumed past available bytes".to_owned())?;
    let len_bytes: [u8; 4] = remaining
        .get(..4)
        .ok_or_else(|| "transport stream did not produce a complete length prefix".to_owned())?
        .try_into()
        .expect("slice length checked");
    let len = u32::from_le_bytes(len_bytes) as usize;
    if len > crate::protocol_limits::MAX_SYNC_MESSAGE_BYTES {
        return Err(format!(
            "transport stream payload exceeds max sync message bytes: {len}"
        ));
    }
    let start = *plain_consumed + 4;
    let end = start + len;
    let payload = plain
        .get(start..end)
        .ok_or_else(|| "transport stream did not produce a complete message".to_owned())?
        .to_vec();
    *plain_consumed = end;
    Ok(payload)
}

#[cfg(feature = "transport-compression-lz4")]
fn new_lz4_stream_decoder() -> Result<WireStreamDecoderInner, String> {
    Ok(WireStreamDecoderInner::Lz4 {
        compressed: Vec::new(),
        plain_consumed: 0,
    })
}

#[cfg(not(feature = "transport-compression-lz4"))]
fn new_lz4_stream_decoder() -> Result<WireStreamDecoderInner, String> {
    Err("lz4 transport compression feature is not compiled in".to_owned())
}

#[cfg(feature = "transport-compression-zstd")]
fn new_zstd_stream_decoder() -> Result<WireStreamDecoderInner, String> {
    zstd::stream::write::Decoder::new(Vec::new())
        .map(|decoder| WireStreamDecoderInner::Zstd {
            decoder,
            plain_consumed: 0,
        })
        .map_err(|error| format!("failed to create zstd stream decoder: {error}"))
}

#[cfg(all(
    not(feature = "transport-compression-zstd"),
    feature = "transport-compression-ruzstd"
))]
fn new_zstd_stream_decoder() -> Result<WireStreamDecoderInner, String> {
    Ok(WireStreamDecoderInner::ZstdRuzstd {
        compressed: Vec::new(),
        plain: Vec::new(),
        plain_consumed: 0,
    })
}

#[cfg(not(any(
    feature = "transport-compression-zstd",
    feature = "transport-compression-ruzstd"
)))]
fn new_zstd_stream_decoder() -> Result<WireStreamDecoderInner, String> {
    Err("zstd transport compression feature is not compiled in".to_owned())
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
    Ok(WireNegotiated {
        protocol_version: max,
        features: remote.features & local_features,
    })
}

#[cfg(test)]
mod tests {
    use groove::Intern;
    use serde_json::json;

    use super::*;
    use crate::ids::SchemaVersionId;
    use crate::ids::{NodeUuid, RowUuid};
    use crate::protocol::{
        RegisterShapeOptions, ResultRowEntry, ShapeAst, Subscribe, SubscribeRejectReason,
        SubscriptionKey,
    };
    use crate::protocol_limits::{MAX_SYNC_MESSAGE_BYTES, MAX_WIRE_FRAME_BYTES};
    use crate::query::{BindingId, Query, ShapeId};
    use crate::time::{GlobalSeq, TxTime};
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
                    "min_protocol_version": 2,
                    "max_protocol_version": 2,
                    "features": 5,
                    "role": "client"
                }
            })
        );
    }

    #[test]
    fn message_payload_round_trips_as_bytes() {
        let session = WireSession {
            session_id: "session-1".to_owned(),
            epoch: 3,
            identity: Some(AuthorId::from_bytes([0x42; 16])),
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
    fn oversized_sync_payload_rejects_before_message_decode() {
        let oversized = vec![0_u8; MAX_SYNC_MESSAGE_BYTES + 1];

        assert!(decode_sync_message(&oversized).is_err());
    }

    #[test]
    fn sync_message_round_trips_through_postcard_codec() {
        let tx_id = TxId::new(TxTime(12), NodeUuid::from_bytes([0x11; 16]));
        let message = SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::Cascade { root: tx_id }),
            global_seq: Some(GlobalSeq(7)),
            durability: Some(DurabilityTier::Global),
        };

        let encoded = encode_sync_message(&message).unwrap();
        let decoded = decode_sync_message(&encoded).unwrap();

        assert_eq!(decoded, message);
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
                        content_tx: Some(tx),
                        layer: Default::default(),
                        deletion_tx: None,
                        source: Default::default(),
                        read_view: Default::default(),
                        schema_version: Some(schema_version),
                        branch_or_prefix: None,
                        row_digest: Some(vec![0xAB; 8]),
                        batch: Some(tx),
                        settle_position: Some(GlobalSeq(10_000 + i)),
                    });
                SyncMessage::ViewUpdate {
                    subscription,
                    settled_through: GlobalSeq(10_000 + i),
                    reset_result_set: false,
                    version_bundles: Vec::new(),
                    peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                    result_member_adds: vec![member],
                    result_member_removes: Vec::new(),
                    program_fact_adds: Vec::new(),
                    program_fact_removes: Vec::new(),
                }
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
            SyncMessage::ViewUpdate {
                subscription,
                settled_through: GlobalSeq(7),
                reset_result_set: true,
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                },
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
            SyncMessage::ViewUpdateChunk {
                subscription,
                settled_through: GlobalSeq(7),
                reset_result_set: true,
                final_chunk: true,
                version_bundles: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                    complete_tx_payloads: vec![tx_id],
                },
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
            SyncMessage::CommitUnit {
                tx: Transaction {
                    tx_id,
                    kind: TxKind::Mergeable,
                    n_total_writes: 0,
                    made_by: AuthorId::from_bytes([0x55; 16]),
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    source_branch: None,
                    merge_strategy: None,
                },
                versions: Vec::new(),
            },
            SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_seq: Some(GlobalSeq(7)),
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
        let message = SyncMessage::ViewUpdate {
            subscription: SubscriptionKey {
                shape_id: ShapeId(uuid::Uuid::from_bytes([0x44; 16])),
                binding_id: BindingId(uuid::Uuid::from_bytes([0x55; 16])),
                read_view: Default::default(),
            },
            settled_through: GlobalSeq(7),
            reset_result_set: true,
            version_bundles: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: vec![tx_id],
            },
            result_member_adds: vec![entry.into()],
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        };

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
        };

        let err = negotiate_wire(&remote, 2, 2, FEATURE_NONE).unwrap_err();

        assert_eq!(err.code, WireErrorCode::UnsupportedProtocolVersion);
        assert_eq!(err.retry, WireRetry::Never);
    }
}
