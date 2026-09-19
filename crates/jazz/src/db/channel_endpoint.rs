//! Persistent per-channel codec and reassembly state shared by wire adapters.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::protocol_limits::{
    MAX_ENCODED_MESSAGE_BYTES, MAX_FRAGMENT_REASSEMBLY_AGE_MS, MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
};
use jazz_compression::stream::{Codec, StreamDecoder, StreamEncoder};
use web_time::Instant;

use crate::protocol::SyncMessage;
use crate::wire::channel_credit::{ChannelCredits, SharedChannelCredits};
use crate::wire::channels::{
    AUXILIARY_CHANNEL, ChannelClass, ChannelFrame, ChannelScheduler, MAX_CHANNEL_BUFFER_BYTES,
    MAX_CHANNEL_FRAME_PAYLOAD, MAX_CHANNELS,
};
use crate::wire::{
    FEATURE_PAYLOAD_LZ4, FEATURE_PAYLOAD_ZSTD, TransportError, WireChannelEnvelope,
    WireCompression, WireError, WireFrame, WireInboundContext, encode_frame,
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
    encoded_len: usize,
    started: Option<Instant>,
    progressed: Option<Instant>,
    payload: Vec<u8>,
}

/// Persistent state for a connection direction's bounded channel set.
pub(super) struct ChannelEndpoint {
    context: WireInboundContext,
    credits: SharedChannelCredits,
    scheduler: ChannelScheduler,
    encoders: BTreeMap<u16, (u64, Option<StreamEncoder>)>,
    inbound: BTreeMap<u16, InboundChannel>,
    outbound_features: u64,
    reserved: usize,
    pending: Option<Vec<u8>>,
    pending_last: bool,
    pending_class: ChannelClass,
    failed: Option<String>,
    last_wire_error: Option<WireError>,
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
            credits: Arc::new(Mutex::new(ChannelCredits::new(context.clone()))),
            context,
            scheduler: ChannelScheduler::default(),
            encoders: BTreeMap::new(),
            inbound: BTreeMap::new(),
            outbound_features,
            reserved: 0,
            pending: None,
            pending_last: false,
            pending_class: ChannelClass::Control,
            failed: None,
            last_wire_error: None,
        })
    }

    pub(super) fn set_trusted_encoder(&mut self, trusted: bool) {
        self.context.set_trusted_encoder(trusted);
    }

    pub(super) fn last_wire_error(&self) -> Option<WireError> {
        self.last_wire_error.clone()
    }

    fn expire(&mut self) -> Result<(), String> {
        let expired = self.inbound.values().any(|state| {
            state.started.is_some_and(|at| {
                at.elapsed().as_millis() >= u128::from(MAX_FRAGMENT_REASSEMBLY_AGE_MS)
            }) || state.progressed.is_some_and(|at| {
                at.elapsed().as_millis() >= u128::from(MAX_FRAGMENT_REASSEMBLY_IDLE_MS)
            })
        });
        if expired {
            self.inbound.clear();
            self.reserved = 0;
            let error = "incomplete channel message expired; reconnect required".to_owned();
            self.failed = Some(error.clone());
            return Err(error);
        }
        Ok(())
    }

    #[cfg(test)]
    pub(super) fn set_elapsed_for_test(&mut self, elapsed_ms: u64) {
        let at = Instant::now() - std::time::Duration::from_millis(elapsed_ms);
        for state in self
            .inbound
            .values_mut()
            .filter(|state| state.message_len != 0)
        {
            state.started = Some(at);
            state.progressed = Some(at);
        }
    }

    pub(super) fn enqueue(
        &mut self,
        channel: u16,
        generation: u64,
        class: ChannelClass,
        message: &SyncMessage,
        barrier: bool,
    ) -> Result<(), TransportError> {
        self.expire().map_err(TransportError::Failed)?;
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

    pub(super) fn channel_credits(&self) -> SharedChannelCredits {
        Arc::clone(&self.credits)
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
        self.expire()?;
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        if let Some(frame) = &self.pending {
            return Ok(Some(frame.clone()));
        }
        let credits = self
            .credits
            .lock()
            .map_err(|_| "channel credits mutex poisoned")?;
        let Some(chunk) = self
            .scheduler
            .next_chunk_where(|class| credits.can_send(class))
        else {
            return Ok(None);
        };
        drop(credits);
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
        self.pending_class = extent.class;
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
        self.credits
            .lock()
            .map_err(|_| "channel credits mutex poisoned")?
            .charge(self.pending_class, self.pending.as_ref().unwrap().len())?;
        self.scheduler.accepted()?;
        self.pending = None;
        Ok(self.pending_last)
    }

    pub(super) fn receive(
        &mut self,
        frame: WireChannelEnvelope,
        encoded_len: usize,
    ) -> Result<Option<SyncMessage>, String> {
        self.expire()?;
        if let Some(error) = &self.failed {
            return Err(error.clone());
        }
        self.credits
            .lock()
            .map_err(|_| "channel credits mutex poisoned")?
            .consumed(frame.extent.class, encoded_len)?;
        let result = self.receive_inner(frame);
        if let Err(error) = &result {
            self.inbound.clear();
            self.reserved = 0;
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
                    encoded_len: 0,
                    started: None,
                    progressed: None,
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
            state.started = Some(Instant::now());
            state.encoded_len = 0;
        } else if state.message_len == 0 {
            return Err("channel continuation has no message".into());
        }
        state.encoded_len = state
            .encoded_len
            .checked_add(extent.payload.len())
            .ok_or("channel encoded length overflow")?;
        if state.encoded_len > MAX_ENCODED_MESSAGE_BYTES {
            return Err("channel encoded message exceeds size limit".into());
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
        state.progressed = Some(Instant::now());
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
        state.started = None;
        state.progressed = None;
        state.encoded_len = 0;
        let payload = std::mem::take(&mut state.payload);
        let message = self
            .context
            .decode_semantic_payload(&payload)
            .map_err(|error| {
                self.last_wire_error = Some(error.clone());
                format!("invalid channel semantic payload: {error:?}")
            })?;
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
        // A preceding delivery may introduce this transaction. Preserve that
        // dependency across independently scheduled delivery/write channels.
        FateUpdate { .. } => (ChannelClass::Writes, true),
        CommitUnit { .. } | AuthorityPublication(_) => (ChannelClass::Writes, false),
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
    pub(super) fn set_trusted_encoder(&mut self, trusted: bool) {
        self.endpoint.set_trusted_encoder(trusted);
    }
    pub(super) fn last_wire_error(&self) -> Option<WireError> {
        self.endpoint.last_wire_error()
    }
    /// Share canonical and auxiliary physical windows for this admitted link.
    pub fn set_channel_credits(&mut self, credits: SharedChannelCredits) {
        self.endpoint.credits = credits;
    }
    /// The lock-independent pump uses these balances and pending credit grants.
    pub fn channel_credits(&self) -> SharedChannelCredits {
        self.endpoint.channel_credits()
    }
    /// Distinguish queued output from output eligible under receiver credit.
    pub fn outbound_is_ready(&self) -> bool {
        self.has_pending_outbound()
            && self
                .endpoint
                .credits
                .lock()
                .is_ok_and(|credits| credits.can_send(ChannelClass::Auxiliary))
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
        if self.outbound_is_ready() {
            Poll::Ready(())
        } else {
            self.waker = Some(cx.waker().clone());
            if let Ok(mut credits) = self.endpoint.credits.lock() {
                credits.register_waker(cx);
            }
            Poll::Pending
        }
    }
    /// Admit only reserved-slot auxiliary extents, without acquiring a node lock.
    pub fn receive(
        &mut self,
        frame: WireChannelEnvelope,
        encoded_len: usize,
    ) -> Result<Option<SyncMessage>, String> {
        if frame.extent.channel != AUXILIARY_CHANNEL
            || frame.extent.class != ChannelClass::Auxiliary
        {
            return Err("non-auxiliary frame attempted auxiliary bypass".into());
        }
        self.endpoint.receive(frame, encoded_len)
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::db::{Transport, WireTransportAdapter};
    use crate::protocol::{
        ChunkResponse, ChunkResponseBatch, ChunkResponseEntry, PermissionAdvice,
        PermissionAdviceRequestId,
    };
    use crate::wire::{WIRE_PROTOCOL_VERSION, WireTransport, decode_frame};
    use std::collections::VecDeque;

    #[derive(Clone, Default)]
    struct TestWire {
        incoming: Arc<Mutex<VecDeque<Vec<u8>>>>,
        outgoing: Arc<Mutex<VecDeque<Vec<u8>>>>,
    }
    impl WireTransport for TestWire {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            self.outgoing.lock().unwrap().push_back(frame);
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.incoming.lock().unwrap().pop_front()
        }
    }
    fn pair() -> (TestWire, TestWire) {
        let left = TestWire::default();
        let right = TestWire {
            incoming: Arc::clone(&left.outgoing),
            outgoing: Arc::clone(&left.incoming),
        };
        (left, right)
    }

    // Internal wire tests are needed to assert physical interleaving and actual
    // compressed byte counts; row APIs intentionally hide those details.
    #[test]
    fn query_delivery_arrives_while_independent_large_chunk_is_still_in_progress() {
        let (left, right) = pair();
        let mut sender = WireTransportAdapter::current(left);
        let mut receiver = WireTransportAdapter::current(right);
        let bulk = SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
            responses: vec![ChunkResponseEntry {
                request_id: 1,
                result: ChunkResponse::Found(vec![7; 200_000]),
            }],
        });
        let query = SyncMessage::PermissionAdviceResponse {
            request_id: PermissionAdviceRequestId([1; 16]),
            advice: PermissionAdvice::Unknown,
        };
        sender.send(bulk.clone()).unwrap();
        sender.send(query.clone()).unwrap();
        assert_eq!(
            receiver.try_recv_result().unwrap(),
            Some(query),
            "small delivery must overtake unfinished auxiliary bytes"
        );
        let mut completed = None;
        for _ in 0..20 {
            assert!(sender.try_recv_result().unwrap().is_none());
            sender.poll_flush().unwrap();
            if let Some(message) = receiver.try_recv_result().unwrap() {
                completed = Some(message);
                break;
            }
        }
        assert_eq!(completed, Some(bulk));
    }

    pub(crate) fn compression_receipt(messages: &[SyncMessage], features: u64) -> u64 {
        let (left, right) = pair();
        let outgoing = Arc::clone(&left.outgoing);
        let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
        let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
        let mut encoded = 0;
        for message in messages {
            assert!(sender.try_recv_result().unwrap().is_none());
            sender.send(message.clone()).unwrap();
            for frame in outgoing.lock().unwrap().iter() {
                if let WireFrame::Channel(frame) = decode_frame(frame).unwrap() {
                    encoded += frame.extent.payload.len() as u64;
                }
            }
            assert_eq!(receiver.try_recv_result().unwrap(), Some(message.clone()));
        }
        encoded
    }
}
