//! Reliable ordered byte streams: framing, scheduling, compression and raw credits.
//! Logical routing and dependency epochs live above this opaque-byte backend.
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use crate::protocol_limits::{
    MAX_ENCODED_MESSAGE_BYTES, MAX_FRAGMENT_REASSEMBLY_AGE_MS, MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
};
use jazz_compression::stream::{Codec, StreamDecoder, StreamEncoder};
use web_time::Instant;

use crate::wire::channel_credit::{BufferLease, ChannelCredits, SharedChannelCredits};
use crate::wire::channels::{
    ChannelClass, ChannelFrame, ChannelScheduler, MAX_CHANNEL_BUFFER_BYTES,
    MAX_CHANNEL_FRAME_PAYLOAD, MAX_CHANNELS,
};
use crate::wire::{
    FEATURE_PAYLOAD_LZ4, FEATURE_PAYLOAD_ZSTD, TransportError, WireChannelEnvelope,
    WireCompression, WireFrame, WireInboundContext, encode_frame,
};

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
    lease: Option<BufferLease>,
}

pub(crate) struct ReceivedBytes {
    pub channel: u16,
    pub class: ChannelClass,
    pub payload: Vec<u8>,
    pub lease: BufferLease,
}

/// Persistent state for a connection direction's bounded channel set.
pub(crate) struct OrderedChannelBackend {
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
    idle_timeout_ms: u64,
    age_timeout_ms: u64,
}

impl OrderedChannelBackend {
    pub(crate) fn new(context: WireInboundContext) -> Result<Self, String> {
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
            idle_timeout_ms: MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
            age_timeout_ms: MAX_FRAGMENT_REASSEMBLY_AGE_MS,
        })
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn set_incomplete_receive_timeout_for_test(&mut self, timeout_ms: u64) {
        self.idle_timeout_ms = timeout_ms;
        self.age_timeout_ms = timeout_ms;
    }

    pub(crate) fn incomplete_receive_timeout_ms(&self) -> Option<u64> {
        self.inbound
            .values()
            .flat_map(|state| {
                [
                    (state.started, self.age_timeout_ms),
                    (state.progressed, self.idle_timeout_ms),
                ]
            })
            .filter_map(|(started, limit)| {
                started.map(|at| {
                    let remaining =
                        std::time::Duration::from_millis(limit).saturating_sub(at.elapsed());
                    u64::try_from(remaining.as_nanos().div_ceil(1_000_000)).unwrap_or(u64::MAX)
                })
            })
            .min()
    }

    pub(crate) fn expire(&mut self) -> Result<(), String> {
        let expired = self.inbound.values().any(|state| {
            state
                .started
                .is_some_and(|at| at.elapsed().as_millis() >= u128::from(self.age_timeout_ms))
                || state
                    .progressed
                    .is_some_and(|at| at.elapsed().as_millis() >= u128::from(self.idle_timeout_ms))
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
    pub(crate) fn set_elapsed_for_test(&mut self, elapsed_ms: u64) {
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

    pub(crate) fn enqueue(
        &mut self,
        channel: u16,
        generation: u64,
        class: ChannelClass,
        payload: Vec<u8>,
    ) -> Result<(), TransportError> {
        self.expire().map_err(TransportError::Failed)?;
        if let Some(error) = &self.failed {
            return Err(TransportError::Failed(error.clone()));
        }
        let mut credits = self
            .credits
            .lock()
            .map_err(|_| TransportError::Failed("credit mutex poisoned".into()))?;
        let size = payload.len();
        if !credits.can_reserve_message(class, size) {
            return Err(TransportError::Backpressure);
        }
        self.scheduler
            .enqueue(channel, generation, class, payload)
            .map_err(|error| {
                if error.contains("backpressure") {
                    TransportError::Backpressure
                } else {
                    TransportError::Failed(error)
                }
            })?;
        credits.reserve_message(class, size);
        drop(credits);
        if self
            .encoders
            .get(&channel)
            .is_some_and(|(old, _)| *old != generation)
        {
            self.encoders.remove(&channel);
        }
        Ok(())
    }

    pub(crate) fn set_channel_credits(&mut self, credits: SharedChannelCredits) {
        self.credits = credits;
    }

    pub(crate) fn channel_credits(&self) -> SharedChannelCredits {
        Arc::clone(&self.credits)
    }

    pub(crate) fn is_idle(&self, channel: u16) -> bool {
        self.scheduler.is_idle(channel)
    }
    pub(crate) fn next_idle_generation(&self, channel: u16) -> Result<u64, String> {
        self.scheduler.next_idle_generation(channel)
    }
    pub(crate) fn has_pending(&self) -> bool {
        self.scheduler.queued_messages() != 0
    }

    pub(crate) fn peek_outbound(&mut self) -> Result<Option<Vec<u8>>, String> {
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
            // Size the extent for this chunk rather than the largest possible
            // one, and grow only up to the unchanged physical bound when the
            // encoder stalls, so a 40-byte fate no longer zeroes ~70 KB.
            let mut output = vec![0; encoded_extent_capacity(chunk.bytes.len())];
            let mut consumed = 0;
            let mut written = 0;
            while consumed < chunk.bytes.len() {
                let progress = encoder.encode(&chunk.bytes[consumed..], &mut output[written..])?;
                if progress.consumed == 0 && progress.written == 0 {
                    if !grow_encoded_extent(&mut output) {
                        return Err("channel encoder exceeded physical extent bound".into());
                    }
                    continue;
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
                if progress.written == 0 && !grow_encoded_extent(&mut output) {
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

    pub(crate) fn accept_outbound(&mut self) -> Result<bool, String> {
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

    pub(crate) fn receive(
        &mut self,
        frame: WireChannelEnvelope,
        encoded_len: usize,
    ) -> Result<Option<ReceivedBytes>, String> {
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

    fn receive_inner(
        &mut self,
        frame: WireChannelEnvelope,
    ) -> Result<Option<ReceivedBytes>, String> {
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
                    lease: None,
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
            state.lease = Some(ChannelCredits::receive_message(
                &self.credits,
                extent.class,
                size,
            )?);
            // The declared length is already admitted by the budget above;
            // reserve it once rather than growing on every extent. The spare
            // byte is the decoder's overrun probe on the final extent.
            state
                .payload
                .try_reserve_exact(size + 1)
                .map_err(|_| "channel reassembly allocation failed".to_owned())?;
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
        if let Some(decoder) = &mut state.decoder {
            // Decode straight into the reassembly buffer. One spare byte still
            // detects a codec that overruns the declared decoded extent.
            let base = state.payload.len();
            state
                .payload
                .try_reserve_exact(expected + 1)
                .map_err(|_| "channel reassembly allocation failed".to_owned())?;
            state.payload.resize(base + expected + 1, 0);
            let decoded = (|| {
                let output = &mut state.payload[base..];
                let mut consumed = 0;
                let mut written = 0;
                loop {
                    let progress =
                        decoder.decode(&extent.payload[consumed..], &mut output[written..])?;
                    consumed += progress.consumed;
                    written += progress.written;
                    if written > expected || progress.finished {
                        return Err(
                            "channel codec ended or exceeded declared decoded extent".to_owned()
                        );
                    }
                    if progress.consumed == 0 && progress.written == 0 {
                        break;
                    }
                }
                if consumed != extent.payload.len() || written != expected {
                    return Err(
                        "channel flush did not produce exact declared decoded extent".to_owned(),
                    );
                }
                Ok(written)
            })();
            match decoded {
                Ok(written) => state.payload.truncate(base + written),
                Err(error) => {
                    state.payload.truncate(base);
                    return Err(error);
                }
            }
        } else {
            if extent.payload.len() != expected {
                return Err("uncompressed channel extent size mismatch".into());
            }
            state
                .payload
                .try_reserve_exact(extent.payload.len())
                .map_err(|_| "channel reassembly allocation failed".to_owned())?;
            state.payload.extend_from_slice(&extent.payload);
        }
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
        Ok(Some(ReceivedBytes {
            channel: extent.channel,
            class: state.class,
            payload,
            lease: state
                .lease
                .take()
                .expect("active receive owns buffer reservation"),
        }))
    }
}

impl Drop for OrderedChannelBackend {
    fn drop(&mut self) {
        if let Ok(mut credits) = self.credits.lock() {
            credits.close();
        }
    }
}

/// Initial output for compressing one chunk: the same slack formula as
/// `MAX_CHANNEL_FRAME_PAYLOAD`, applied to this chunk's length.
fn encoded_extent_capacity(chunk_len: usize) -> usize {
    (chunk_len + chunk_len / 10 + 64).min(MAX_CHANNEL_FRAME_PAYLOAD)
}

/// Double a stalled extent buffer up to the physical bound. Returns false
/// when it is already at the bound, which is the pre-existing error case.
fn grow_encoded_extent(output: &mut Vec<u8>) -> bool {
    if output.len() >= MAX_CHANNEL_FRAME_PAYLOAD {
        return false;
    }
    let grown = (output.len() * 2).min(MAX_CHANNEL_FRAME_PAYLOAD);
    output.resize(grown, 0);
    true
}
