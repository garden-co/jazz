//! Versioned transport frames around Jazz sync semantics.
//!
//! The wire layer is intentionally thinner than [`crate::protocol`]: it owns
//! link/session negotiation, feature discovery, binary framing, and structured
//! protocol errors. The frame payload is opaque bytes for now so bindings and
//! server shells can adopt the envelope before the full [`crate::protocol::SyncMessage`]
//! encoder is frozen.

use postcard::{from_bytes, to_allocvec};
use serde::{Deserialize, Serialize};

use crate::ids::AuthorId;
use crate::protocol::SyncMessage;
use crate::protocol_limits::{validate_sync_message_len, validate_wire_frame_len};

/// Current Jazz wire protocol version.
pub const WIRE_PROTOCOL_VERSION: u16 = 1;

/// No optional features.
pub const FEATURE_NONE: WireFeatures = 0;
/// Frame payloads contain encoded Jazz sync messages.
pub const FEATURE_SYNC_MESSAGE_PAYLOAD: WireFeatures = 1 << 0;
/// Frames may carry an explicit resumable session id and epoch.
pub const FEATURE_SESSION_FRAME: WireFeatures = 1 << 1;
/// Peers understand structured [`WireError`] frames.
pub const FEATURE_STRUCTURED_ERRORS: WireFeatures = 1 << 2;

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
                    "min_protocol_version": 1,
                    "max_protocol_version": 1,
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
                versions: Vec::new(),
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
