//! Persistent per-channel codec and reassembly state shared by wire adapters.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use jazz_compression::stream::{Codec, StreamDecoder, StreamEncoder};

use crate::protocol::SyncMessage;
use crate::wire::channels::{
    AUXILIARY_CHANNEL, ChannelClass, ChannelFrame, ChannelScheduler, MAX_CHANNEL_BUFFER_BYTES,
    MAX_CHANNEL_FRAME_PAYLOAD, MAX_CHANNELS,
};
use crate::wire::{
    FEATURE_PAYLOAD_LZ4, FEATURE_PAYLOAD_ZSTD, TransportError, WireChannelEnvelope,
    WireCompression, WireFrame, WireInboundContext, decode_sync_message_for_features, encode_frame,
    encode_sync_message_for_features,
};

/// Shared auxiliary state has one output owner after a lock-independent pump is
/// installed. Guards must never be held across asynchronous chunk reads.
pub type SharedAuxiliaryEndpoint = Arc<Mutex<AuxiliaryChannelEndpoint>>;

fn codec(features: u64) -> Result<Option<Codec>, String> {
    match features & (FEATURE_PAYLOAD_LZ4 | FEATURE_PAYLOAD_ZSTD) {
        0 => Ok(None),
        FEATURE_PAYLOAD_LZ4 => Ok(Some(Codec::Lz4)),
        FEATURE_PAYLOAD_ZSTD => Ok(Some(Codec::Zstd)),
        _ => Err("channel declares multiple compression codecs".into()),
    }
}

struct InboundChannel {
    generation: u64,
    sequence: u64,
    class: ChannelClass,
    active_features: u64,
    decoder: Option<StreamDecoder>,
    message_len: usize,
    payload: Vec<u8>,
}

/// Persistent state for a connection direction's bounded channel set.
pub(super) struct ChannelEndpoint {
    context: WireInboundContext,
    scheduler: ChannelScheduler,
    encoders: BTreeMap<u16, (u64, Option<StreamEncoder>)>,
    inbound: BTreeMap<u16, InboundChannel>,
    outbound_features: u64,
    reserved: usize,
    pending: Option<Vec<u8>>,
    pending_last: bool,
    failed: Option<String>,
}

impl ChannelEndpoint {
    pub(super) fn new(context: WireInboundContext) -> Result<Self, String> {
        let features = context.negotiated_features();
        // A decode-only browser explicitly emits uncompressed channel bytes;
        // it never substitutes independent per-message frames for a stream.
        let selected = match WireCompression::from_features(features) {
            WireCompression::Zstd if !cfg!(feature = "transport-compression-zstd") => 0,
            other => other.feature(),
        };
        let outbound_features =
            (features & !(FEATURE_PAYLOAD_LZ4 | FEATURE_PAYLOAD_ZSTD)) | selected;
        Ok(Self {
            context,
            scheduler: ChannelScheduler::default(),
            encoders: BTreeMap::new(),
            inbound: BTreeMap::new(),
            outbound_features,
            reserved: 0,
            pending: None,
            pending_last: false,
            failed: None,
        })
    }

    pub(super) fn enqueue(
        &mut self,
        channel: u16,
        generation: u64,
        class: ChannelClass,
        message: &SyncMessage,
        barrier: bool,
    ) -> Result<(), TransportError> {
        if let Some(error) = &self.failed {
            return Err(TransportError::Failed(error.clone()));
        }
        let payload = encode_sync_message_for_features(message, self.context.negotiated_features())
            .map_err(|error| {
                TransportError::Failed(format!("cannot encode channel message: {error:?}"))
            })?;
        self.scheduler
            .enqueue(channel, generation, class, payload, barrier)
            .map_err(|error| {
                if error.contains("backpressure") {
                    TransportError::Backpressure
                } else {
                    TransportError::Failed(error)
                }
            })
    }

    pub(super) fn is_idle(&self, channel: u16) -> bool {
        self.scheduler.is_idle(channel)
    }
    pub(super) fn reset_idle(&mut self, channel: u16, class: ChannelClass) -> Result<u64, String> {
        let generation = self.scheduler.reset_idle(channel, class)?;
        self.encoders.remove(&channel);
        Ok(generation)
    }
    pub(super) fn has_pending(&self) -> bool {
        self.scheduler.queued_messages() != 0
    }

    pub(super) fn peek_outbound(&mut self) -> Result<Option<Vec<u8>>, String> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if let Some(frame) = &self.pending {
            return Ok(Some(frame.clone()));
        }
        let Some(chunk) = self.scheduler.next_chunk() else {
            return Ok(None);
        };
        let codec = codec(self.outbound_features)?;
        if let std::collections::btree_map::Entry::Vacant(entry) =
            self.encoders.entry(chunk.channel)
        {
            entry.insert((chunk.generation, codec.map(StreamEncoder::new).transpose()?));
        }
        let (generation, encoder) = self.encoders.get_mut(&chunk.channel).unwrap();
        if *generation != chunk.generation {
            return Err("outbound codec generation mismatch".into());
        }
        let payload = if let Some(encoder) = encoder {
            let mut output = vec![0; MAX_CHANNEL_FRAME_PAYLOAD];
            let mut consumed = 0;
            let mut written = 0;
            while consumed < chunk.bytes.len() {
                let progress = encoder.encode(&chunk.bytes[consumed..], &mut output[written..])?;
                if progress.consumed == 0 && progress.written == 0 {
                    return Err("channel encoder exceeded physical extent bound".into());
                }
                consumed += progress.consumed;
                written += progress.written;
            }
            loop {
                let progress = encoder.flush(&mut output[written..])?;
                written += progress.written;
                if progress.finished {
                    break;
                }
                if progress.written == 0 {
                    return Err("channel flush exceeded physical extent bound".into());
                }
            }
            output.truncate(written);
            output
        } else {
            chunk.bytes.to_vec()
        };
        let extent = ChannelFrame {
            channel: chunk.channel,
            generation: chunk.generation,
            sequence: chunk.sequence,
            class: chunk.class,
            first: chunk.first,
            last: chunk.last,
            message_len: chunk.message_len,
            decoded_len: chunk.bytes.len() as u32,
            payload,
        };
        extent.validate()?;
        self.pending_last = extent.last;
        let frame = encode_frame(&WireFrame::Channel(WireChannelEnvelope {
            protocol_version: self.context.expected_protocol_version(),
            features: self.outbound_features,
            session: self.context.expected_session().cloned(),
            extent,
        }))
        .map_err(|error| error.to_string())?;
        self.pending = Some(frame.clone());
        Ok(Some(frame))
    }

    pub(super) fn accept_outbound(&mut self) -> Result<bool, String> {
        if self.pending.is_none() {
            return Err("no encoded channel frame to accept".into());
        }
        self.scheduler.accepted()?;
        self.pending = None;
        Ok(self.pending_last)
    }

    pub(super) fn receive(
        &mut self,
        frame: WireChannelEnvelope,
    ) -> Result<Option<SyncMessage>, String> {
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        let result = self.receive_inner(frame);
        if let Err(error) = &result {
            self.failed = Some(error.clone());
        }
        result
    }

    fn receive_inner(&mut self, frame: WireChannelEnvelope) -> Result<Option<SyncMessage>, String> {
        self.context
            .validate_channel_metadata(&frame)
            .map_err(|error| format!("{error:?}"))?;
        let extent = frame.extent;
        extent.validate()?;
        let active_features = frame.features & (FEATURE_PAYLOAD_LZ4 | FEATURE_PAYLOAD_ZSTD);
        let selected = codec(active_features)?;
        let reset = match self.inbound.get(&extent.channel) {
            None => {
                if extent.generation != 0 {
                    return Err("new channel must start at generation zero".into());
                }
                true
            }
            Some(state) if state.generation != extent.generation => {
                if state.generation.checked_add(1) != Some(extent.generation)
                    || state.message_len != 0
                {
                    return Err("stale or incomplete channel generation reset".into());
                }
                true
            }
            _ => false,
        };
        if reset {
            if !extent.first || extent.sequence != 0 {
                return Err("new channel generation must start at sequence zero".into());
            }
            if self.inbound.len() == MAX_CHANNELS && !self.inbound.contains_key(&extent.channel) {
                return Err("too many channel contexts".into());
            }
            self.inbound.insert(
                extent.channel,
                InboundChannel {
                    generation: extent.generation,
                    sequence: 0,
                    class: extent.class,
                    active_features,
                    decoder: selected.map(StreamDecoder::new).transpose()?,
                    message_len: 0,
                    payload: Vec::new(),
                },
            );
        }
        let state = self.inbound.get_mut(&extent.channel).unwrap();
        if state.sequence != extent.sequence
            || state.class != extent.class
            || state.active_features != active_features
        {
            return Err("channel sequence, class or compression changed".into());
        }
        if extent.first {
            if state.message_len != 0 {
                return Err("channel starts message before predecessor completes".into());
            }
            let size = extent.message_len as usize;
            if self.reserved + size > MAX_CHANNEL_BUFFER_BYTES {
                return Err("channel declared messages exceed aggregate budget".into());
            }
            self.reserved += size;
            state.message_len = size;
        } else if state.message_len == 0 {
            return Err("channel continuation has no message".into());
        }
        let expected = extent.decoded_len as usize;
        if state.payload.len() + expected > state.message_len {
            return Err("channel exceeds declared logical length".into());
        }
        let bytes = if let Some(decoder) = &mut state.decoder {
            let mut output = vec![0; expected + 1];
            let mut consumed = 0;
            let mut written = 0;
            loop {
                let progress =
                    decoder.decode(&extent.payload[consumed..], &mut output[written..])?;
                consumed += progress.consumed;
                written += progress.written;
                if written > expected || progress.finished {
                    return Err("channel codec ended or exceeded declared decoded extent".into());
                }
                if progress.consumed == 0 && progress.written == 0 {
                    break;
                }
            }
            if consumed != extent.payload.len() || written != expected {
                return Err("channel flush did not produce exact declared decoded extent".into());
            }
            output.truncate(written);
            output
        } else {
            if extent.payload.len() != expected {
                return Err("uncompressed channel extent size mismatch".into());
            }
            extent.payload
        };
        state
            .payload
            .try_reserve_exact(bytes.len())
            .map_err(|_| "channel reassembly allocation failed".to_owned())?;
        state.payload.extend_from_slice(&bytes);
        state.sequence = state
            .sequence
            .checked_add(1)
            .ok_or("channel sequence exhausted")?;
        if !extent.last {
            if state.payload.len() == state.message_len {
                return Err("channel omitted final extent flag".into());
            }
            return Ok(None);
        }
        if state.payload.len() != state.message_len {
            return Err("channel completed before declared logical length".into());
        }
        self.reserved -= state.message_len;
        state.message_len = 0;
        let payload = std::mem::take(&mut state.payload);
        let message =
            decode_sync_message_for_features(&payload, self.context.negotiated_features())
                .map_err(|error| format!("invalid channel semantic payload: {error:?}"))?;
        if message_class(&message).0 != state.class {
            return Err("semantic message does not belong to channel class".into());
        }
        Ok(Some(message))
    }
}

pub(super) fn message_class(message: &SyncMessage) -> (ChannelClass, bool) {
    use SyncMessage::*;
    match message {
        ChunkRequestBatch(_) | ChunkResponseBatch(_) => (ChannelClass::Auxiliary, false),
        SessionClaims { .. }
        | PublishSchema { .. }
        | PublishSchemaWithLens { .. }
        | PublishLens { .. }
        | CatalogueAck(_)
        | CatalogueSnapshot(_) => (ChannelClass::Control, true),
        CommitUnit { .. } | FateUpdate { .. } | AuthorityPublication(_) => {
            (ChannelClass::Writes, false)
        }
        RegisterShape { .. }
        | Subscribe(_)
        | Unsubscribe { .. }
        | FetchRowVersions { .. }
        | PermissionAdviceRequest { .. }
        | AuthorizationScopeSubscribe { .. }
        | AuthorizationScopeIntent { .. }
        | CurrentRowsRequest(_)
        | CurrentRowsCancel { .. } => (ChannelClass::Requests, false),
        ChunkUploadStart(_) | ChunkUploadNodes(_) | ChunkUploadResult(_) => {
            (ChannelClass::LargeValue, false)
        }
        _ => (ChannelClass::Delivery, false),
    }
}

/// The fixed lock-independent auxiliary channel. Queued semantic ownership is
/// retained until the final encoded extent is physically accepted.
pub struct AuxiliaryChannelEndpoint {
    endpoint: ChannelEndpoint,
    message: Option<SyncMessage>,
    pump_owned: bool,
    waker: Option<Waker>,
}

impl AuxiliaryChannelEndpoint {
    /// Create fresh state scoped to one admitted connection.
    pub fn new(context: WireInboundContext) -> Result<Self, String> {
        Ok(Self {
            endpoint: ChannelEndpoint::new(context)?,
            message: None,
            pump_owned: false,
            waker: None,
        })
    }
    /// Transfer physical output ownership to the independently driven pump.
    pub fn set_pump_owned(&mut self) {
        self.pump_owned = true;
    }
    /// Whether the semantic adapter must leave auxiliary output to its pump.
    pub fn pump_owned(&self) -> bool {
        self.pump_owned
    }
    /// Admit one immutable auxiliary message without advancing its codec.
    pub fn enqueue(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if message_class(&message).0 != ChannelClass::Auxiliary {
            return Err(TransportError::Failed(
                "canonical message attempted auxiliary bypass".into(),
            ));
        }
        if self.message.is_some() {
            return Err(TransportError::Backpressure);
        }
        self.endpoint.enqueue(
            AUXILIARY_CHANNEL,
            0,
            ChannelClass::Auxiliary,
            &message,
            false,
        )?;
        self.message = Some(message);
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }
    /// Return exactly the same encoded extent until accepted, without reencoding.
    pub fn peek_outbound(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.endpoint.peek_outbound()
    }
    /// Commit one physical extent; only the last releases the semantic obligation.
    pub fn accept_outbound(&mut self) -> Result<Option<SyncMessage>, String> {
        Ok(if self.endpoint.accept_outbound()? {
            self.message.take()
        } else {
            None
        })
    }
    /// Whether the pump still owns pending physical output.
    pub fn has_pending_outbound(&self) -> bool {
        self.endpoint.has_pending()
    }
    /// Register a lock-independent readiness wake for newly admitted output.
    pub fn poll_outbound_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.has_pending_outbound() {
            Poll::Ready(())
        } else {
            self.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    /// Admit only reserved-slot auxiliary extents, without acquiring a node lock.
    pub fn receive(&mut self, frame: WireChannelEnvelope) -> Result<Option<SyncMessage>, String> {
        if frame.extent.channel != AUXILIARY_CHANNEL
            || frame.extent.class != ChannelClass::Auxiliary
        {
            return Err("non-auxiliary frame attempted auxiliary bypass".into());
        }
        self.endpoint.receive(frame)
    }
}
