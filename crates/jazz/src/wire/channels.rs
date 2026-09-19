//! Ordered logical channels carried by an ordered reliable link.
//!
//! This module owns byte framing and bounded scheduling, not authorization or
//! semantic dispatch. A channel is scoped to one admitted connection direction.
//! Hosts must apply semantic barriers before dispatching dependent messages.

use std::collections::{BTreeMap, VecDeque};

use crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES;

/// Maximum simultaneously retained channel slots per connection direction.
pub const MAX_CHANNELS: usize = 64;
/// Maximum uncompressed bytes submitted to a streaming codec in one turn.
pub const CHANNEL_CHUNK_BYTES: usize = 64 * 1024;
/// Maximum compressed chunk, including a codec's stream header/flush overhead.
pub const MAX_CHANNEL_FRAME_PAYLOAD: usize = CHANNEL_CHUNK_BYTES + CHANNEL_CHUNK_BYTES / 10 + 64;
/// Dedicated admission capacity for control traffic, unavailable to other flows.
pub const CONTROL_RESERVE_BYTES: usize = 1024 * 1024;
/// Capacity for bounded interactive messages beside one maximum legal bulk.
pub const INTERACTIVE_RESERVE_BYTES: usize = 8 * 1024 * 1024;
/// Aggregate decoded staging budget mirrored by both channel endpoints.
pub const MAX_CHANNEL_BUFFER_BYTES: usize =
    2 * MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES + CONTROL_RESERVE_BYTES;
/// Maximum retained logical messages, independently of their byte sizes.
pub const MAX_CHANNEL_QUEUED_MESSAGES: usize = 1024;
/// Slots unavailable to data traffic so tiny messages cannot starve control.
pub const CONTROL_RESERVE_MESSAGES: usize = 8;
/// Maximum postcard-v1 channel metadata overhead.
pub const CHANNEL_HEADER_BYTES: usize = 64;

/// Slot zero reserves independent control capacity.
pub const CONTROL_CHANNEL: u16 = 0;
/// Fixed auxiliary lane shared with the lock-independent chunk pump.
pub const AUXILIARY_CHANNEL: u16 = 63;
/// Reserved stream for bounded dependency-resolution traffic.
pub const PROGRESS_CHANNEL: u16 = 62;

/// Stable scheduling class. Its value is explicitly encoded as one byte.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[repr(u8)]
pub enum ChannelClass {
    /// Catalogue, session state and connection lifecycle.
    Control = 0,
    /// Shape registration followed by subscription requests.
    Requests = 1,
    /// One independent query/subscription or authorization-intent delivery.
    Delivery = 2,
    /// The initial single authored-write FIFO.
    Writes = 3,
    /// One independent immutable large-value transfer.
    LargeValue = 4,
    /// Immutable chunk lookup/response traffic without semantic authority.
    Auxiliary = 5,
    /// Replies needed to release buffers retained by ordinary traffic.
    Progress = 6,
}

/// Physical channel extent. `sequence` is contiguous within one generation;
/// receivers must never skip bytes in a stateful compressed stream.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ChannelFrame {
    /// Connection-direction slot.
    pub channel: u16,
    /// Fresh generation when a slot is reset.
    pub generation: u64,
    /// Contiguous frame ordinal within this generation.
    pub sequence: u64,
    /// Scheduling class fixed for the generation.
    pub class: ChannelClass,
    /// Whether this starts a logical message.
    pub first: bool,
    /// Whether this completes a logical message.
    pub last: bool,
    /// Exact decoded semantic size; nonzero only on the first extent.
    /// Total semantic bytes, or zero on continuation extents.
    pub message_len: u32,
    /// Exact decoded size produced by this codec flush, checked before decode.
    pub decoded_len: u32,
    /// The bounded flushed compressed bytes.
    #[serde(with = "serde_bytes")]
    pub payload: Vec<u8>,
}

impl ChannelFrame {
    pub(crate) fn validate(&self) -> Result<(), String> {
        if usize::from(self.channel) >= MAX_CHANNELS {
            return Err("channel slot exceeds connection limit".into());
        }
        if (self.channel == AUXILIARY_CHANNEL) != (self.class == ChannelClass::Auxiliary) {
            return Err("auxiliary class must use reserved auxiliary channel".into());
        }
        if (self.channel == PROGRESS_CHANNEL) != (self.class == ChannelClass::Progress) {
            return Err("progress class must use reserved progress channel".into());
        }
        if (self.channel == CONTROL_CHANNEL) != (self.class == ChannelClass::Control) {
            return Err("control class must use the reserved channel".into());
        }
        if self.decoded_len as usize > CHANNEL_CHUNK_BYTES
            || self.payload.len() > MAX_CHANNEL_FRAME_PAYLOAD
        {
            return Err("channel extent exceeds chunk limit".into());
        }
        if self.first {
            if self.message_len == 0 || self.message_len as usize > MAX_LOGICAL_MESSAGE_BYTES {
                return Err("channel message exceeds logical size limit".into());
            }
            if self.decoded_len > self.message_len {
                return Err("channel extent exceeds declared message size".into());
            }
        } else if self.message_len != 0 {
            return Err("continuation repeats logical size".into());
        }
        if self.decoded_len == 0 || self.payload.is_empty() {
            return Err("empty channel extent".into());
        }
        Ok(())
    }

    /// Encode the explicitly ordered fields with the postcard-v1 codec.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.validate()?;
        postcard::to_allocvec(self).map_err(|error| error.to_string())
    }

    /// Decode one exact postcard-v1 channel payload, rejecting trailing bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() > CHANNEL_HEADER_BYTES + MAX_CHANNEL_FRAME_PAYLOAD {
            return Err("channel frame exceeds size limit".into());
        }
        let (frame, remainder): (Self, _) =
            postcard::take_from_bytes(bytes).map_err(|error| error.to_string())?;
        if !remainder.is_empty() {
            return Err("trailing bytes after channel frame".into());
        }
        frame.validate()?;
        Ok(frame)
    }
}

struct Message {
    bytes: Box<[u8]>,
    offset: usize,
}

struct OutboundChannel {
    class: ChannelClass,
    generation: u64,
    sequence: u64,
    bytes: usize,
    messages: VecDeque<Message>,
}

/// A selected *uncompressed* extent. Compress only this extent, then retain the
/// resulting frame until the lower transport accepts it. Call `accepted` once.
#[derive(Debug)]
pub struct ScheduledChunk<'a> {
    /// Connection-direction slot.
    pub channel: u16,
    /// Fresh generation when a slot is reset.
    pub generation: u64,
    /// Contiguous frame ordinal within this generation.
    pub sequence: u64,
    /// Scheduling class fixed for the generation.
    pub class: ChannelClass,
    /// Whether this starts a logical message.
    pub first: bool,
    /// Whether this completes a logical message.
    pub last: bool,
    /// Total semantic bytes, or zero on continuation extents.
    pub message_len: u32,
    /// Uncompressed extent to encode during this turn.
    pub bytes: &'a [u8],
}

/// Bounded weighted round robin at physical-frame boundaries. Admission is
/// independent of lower-transport backpressure; codec state advances only when
/// the host encodes the selected extent, never while enqueuing a huge message.
///
/// Each stable stream is FIFO across codec generations. Cross-stream semantic
/// dependencies belong to the upper message layer, never this byte scheduler.
#[derive(Default)]
pub struct ChannelScheduler {
    channels: BTreeMap<u16, OutboundChannel>,
    round_cursor: usize,
    class_cursor: [u16; 7],
    selected: Option<u16>,
    bytes: usize,
    data_bytes: usize,
    messages: usize,
    data_messages: usize,
    bulk_bytes: usize,
    progress_bytes: usize,
    progress_messages: usize,
}

impl ChannelScheduler {
    /// Admission never mutates a codec. On rejection the caller retains the
    /// semantic message and may retry after another scheduling turn.
    pub fn enqueue(
        &mut self,
        channel: u16,
        generation: u64,
        class: ChannelClass,
        payload: Vec<u8>,
    ) -> Result<(), String> {
        if usize::from(channel) >= MAX_CHANNELS
            || (channel == CONTROL_CHANNEL) != (class == ChannelClass::Control)
            || (channel == AUXILIARY_CHANNEL) != (class == ChannelClass::Auxiliary)
            || (channel == PROGRESS_CHANNEL) != (class == ChannelClass::Progress)
        {
            return Err("invalid channel slot or class".into());
        }
        let len = payload.len();
        if len == 0 || len > MAX_LOGICAL_MESSAGE_BYTES {
            return Err("invalid logical message size".into());
        }
        if class == ChannelClass::Progress {
            if self.progress_messages >= CONTROL_RESERVE_MESSAGES
                || self.progress_bytes + len > MAX_LOGICAL_MESSAGE_BYTES
            {
                return Err("progress channel queue backpressure".into());
            }
        } else if self.messages - self.progress_messages >= MAX_CHANNEL_QUEUED_MESSAGES
            || (class != ChannelClass::Control
                && self.data_messages >= MAX_CHANNEL_QUEUED_MESSAGES - CONTROL_RESERVE_MESSAGES)
            || self.bytes - self.progress_bytes + len
                > MAX_CHANNEL_BUFFER_BYTES - MAX_LOGICAL_MESSAGE_BYTES
            || (class != ChannelClass::Control
                && self.data_bytes + len > MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES)
            || (len > CHANNEL_CHUNK_BYTES && self.bulk_bytes + len > MAX_LOGICAL_MESSAGE_BYTES)
        {
            return Err("channel queue backpressure".into());
        }
        if let Some(state) = self.channels.get(&channel) {
            if (state.generation != generation || state.class != class)
                && !(self.is_idle(channel) && state.generation.checked_add(1) == Some(generation))
            {
                return Err("channel generation or class changed without reset".into());
            }
            if state.bytes + len > MAX_LOGICAL_MESSAGE_BYTES {
                return Err("channel queue backpressure".into());
            }
        }
        let state = self
            .channels
            .entry(channel)
            .or_insert_with(|| OutboundChannel {
                class,
                generation,
                sequence: 0,
                bytes: 0,
                messages: VecDeque::new(),
            });
        if state.generation != generation {
            state.generation = generation;
            state.sequence = 0;
            state.class = class;
        }
        state.bytes += len;
        state.messages.push_back(Message {
            bytes: payload.into_boxed_slice(),
            offset: 0,
        });
        self.bytes += len;
        if class == ChannelClass::Progress {
            self.progress_bytes += len;
            self.progress_messages += 1;
        } else if len > CHANNEL_CHUNK_BYTES {
            self.bulk_bytes += len;
        }
        if !matches!(class, ChannelClass::Control | ChannelClass::Progress) {
            self.data_bytes += len;
            self.data_messages += 1;
        }
        self.messages += 1;
        Ok(())
    }

    /// Return the same selection until it is accepted, including first-frame
    /// backpressure. Newly admitted traffic participates at the next boundary.
    pub fn next_chunk(&mut self) -> Option<ScheduledChunk<'_>> {
        self.next_chunk_where(|_| true)
    }

    /// Select only classes with receive credit, before advancing any codec.
    pub fn next_chunk_where(
        &mut self,
        has_credit: impl Fn(ChannelClass) -> bool,
    ) -> Option<ScheduledChunk<'_>> {
        if self.selected.is_none() {
            let eligible = |c: &OutboundChannel| has_credit(c.class) && !c.messages.is_empty();
            // Weight classes, then round-robin within each class: a newly
            // admitted request waits at most one finite 18-frame class round,
            // independent of how many large transfers are already active.
            const ROUND: [ChannelClass; 20] = [
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Control,
                ChannelClass::Requests,
                ChannelClass::Requests,
                ChannelClass::Requests,
                ChannelClass::Requests,
                ChannelClass::Delivery,
                ChannelClass::Delivery,
                ChannelClass::Writes,
                ChannelClass::Writes,
                ChannelClass::LargeValue,
                ChannelClass::Auxiliary,
                ChannelClass::Progress,
                ChannelClass::Progress,
            ];
            for _ in 0..ROUND.len() {
                let class = ROUND[self.round_cursor];
                self.round_cursor = (self.round_cursor + 1) % ROUND.len();
                let after = self.class_cursor[class as usize];
                let candidate = self
                    .channels
                    .iter()
                    .filter(|(_, state)| state.class == class && eligible(state))
                    .map(|(&id, _)| id)
                    .find(|id| *id > after)
                    .or_else(|| {
                        self.channels
                            .iter()
                            .find(|(_, state)| state.class == class && eligible(state))
                            .map(|(&id, _)| id)
                    });
                if let Some(id) = candidate {
                    self.class_cursor[class as usize] = id;
                    self.selected = Some(id);
                    break;
                }
            }
        }
        let channel = self.selected?;
        let state = self.channels.get(&channel)?;
        let message = state.messages.front()?;
        let end = (message.offset + CHANNEL_CHUNK_BYTES).min(message.bytes.len());
        Some(ScheduledChunk {
            channel,
            generation: state.generation,
            sequence: state.sequence,
            class: state.class,
            first: message.offset == 0,
            last: end == message.bytes.len(),
            message_len: if message.offset == 0 {
                message.bytes.len() as u32
            } else {
                0
            },
            bytes: &message.bytes[message.offset..end],
        })
    }

    /// Advance exactly once after physical acceptance. Failure to reserve the
    /// sequence successor is terminal and leaves the selected extent intact.
    pub fn accepted(&mut self) -> Result<(), String> {
        let channel = self.selected.ok_or("no selected channel extent")?;
        let state = self.channels.get_mut(&channel).unwrap();
        let next_sequence = state
            .sequence
            .checked_add(1)
            .ok_or("channel sequence exhausted")?;
        let message = state.messages.front_mut().unwrap();
        let len = CHANNEL_CHUNK_BYTES.min(message.bytes.len() - message.offset);
        message.offset += len;
        state.sequence = next_sequence;
        // Retain accounting for the complete allocation until it is dropped.
        if message.offset == message.bytes.len() {
            let len = message.bytes.len();
            state.messages.pop_front();
            state.bytes -= len;
            self.bytes -= len;
            if state.class == ChannelClass::Progress {
                self.progress_bytes -= len;
                self.progress_messages -= 1;
            } else if len > CHANNEL_CHUNK_BYTES {
                self.bulk_bytes -= len;
            }
            if !matches!(state.class, ChannelClass::Control | ChannelClass::Progress) {
                self.data_bytes -= len;
                self.data_messages -= 1;
            }
            self.messages -= 1;
        }
        self.selected = None;
        Ok(())
    }

    /// Preview a drained slot's next generation without consuming it. Admission
    /// commits the reset only after every queue/size check has succeeded.
    pub fn next_idle_generation(&self, channel: u16) -> Result<u64, String> {
        if !self.is_idle(channel) {
            return Err("channel still owns queued or selected messages".into());
        }
        self.channels.get(&channel).map_or(Ok(0), |state| {
            state
                .generation
                .checked_add(1)
                .ok_or_else(|| "channel generation exhausted".into())
        })
    }

    /// A drained channel can be reassigned only after all accepted frames remain
    /// ordered ahead of the new generation on the lower reliable carrier.
    pub fn is_idle(&self, channel: u16) -> bool {
        self.selected != Some(channel)
            && self
                .channels
                .get(&channel)
                .is_none_or(|s| s.messages.is_empty())
    }

    /// Allocated semantic bytes retained across all queues.
    pub fn queued_bytes(&self) -> usize {
        self.bytes
    }
    /// Number of logical messages retained across all queues.
    pub fn queued_messages(&self) -> usize {
        self.messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Internal tests are necessary: fixed byte encoding, physical scheduling,
    // and allocation accounting are not observable through row-query APIs.
    #[test]
    fn channel_frame_has_explicit_byte_contract_and_rejects_declared_size_bombs() {
        let frame = ChannelFrame {
            channel: 2,
            generation: 1,
            sequence: 0,
            class: ChannelClass::Delivery,
            first: true,
            last: true,
            message_len: 3,
            decoded_len: 3,
            payload: b"abc".to_vec(),
        };
        let bytes = frame.encode().unwrap();
        assert_eq!(hex::encode(&bytes), "020100020101030303616263");
        assert_eq!(ChannelFrame::decode(&bytes).unwrap(), frame);
        for corrupt in [
            ChannelFrame {
                channel: MAX_CHANNELS as u16,
                ..frame.clone()
            },
            ChannelFrame {
                message_len: u32::MAX,
                ..frame.clone()
            },
            ChannelFrame {
                decoded_len: u32::MAX,
                ..frame.clone()
            },
        ] {
            let bytes = postcard::to_allocvec(&corrupt).unwrap();
            assert!(ChannelFrame::decode(&bytes).is_err());
        }
        let mut trailing = bytes;
        trailing.push(0);
        assert!(ChannelFrame::decode(&trailing).is_err());
    }

    #[test]
    fn small_delivery_completes_before_large_upload_and_backpressure_keeps_selection() {
        let mut scheduler = ChannelScheduler::default();
        scheduler
            .enqueue(
                4,
                0,
                ChannelClass::LargeValue,
                vec![7; CHANNEL_CHUNK_BYTES * 20],
            )
            .unwrap();
        let first = scheduler.next_chunk().unwrap();
        assert_eq!(
            (first.channel, first.sequence, first.first, first.last),
            (4, 0, true, false)
        );
        scheduler
            .enqueue(2, 0, ChannelClass::Delivery, vec![8; 3])
            .unwrap();
        // Lower transport rejected the first frame: retain it byte-for-byte.
        let retry = scheduler.next_chunk().unwrap();
        assert_eq!((retry.channel, retry.sequence), (4, 0));
        scheduler.accepted().unwrap();
        let next = scheduler.next_chunk().unwrap();
        assert_eq!((next.channel, next.last), (2, true));
        scheduler.accepted().unwrap();
        assert_eq!(scheduler.queued_messages(), 1);
        assert_eq!(scheduler.queued_bytes(), CHANNEL_CHUNK_BYTES * 20);
    }

    #[test]
    fn byte_scheduler_prioritizes_control_without_semantic_barrier_knowledge() {
        let mut scheduler = ChannelScheduler::default();
        scheduler
            .enqueue(3, 0, ChannelClass::Writes, vec![1; CHANNEL_CHUNK_BYTES + 1])
            .unwrap();
        scheduler
            .enqueue(0, 0, ChannelClass::Control, vec![2])
            .unwrap();
        scheduler
            .enqueue(1, 0, ChannelClass::Requests, vec![3])
            .unwrap();
        let mut order = Vec::new();
        while let Some(chunk) = scheduler.next_chunk() {
            order.push(chunk.channel);
            scheduler.accepted().unwrap();
        }
        assert_eq!(order, [0, 1, 3, 3]);
        assert_eq!(scheduler.queued_bytes(), 0);
    }

    #[test]
    fn late_request_has_bounded_latency_with_many_busy_bulk_channels() {
        let mut scheduler = ChannelScheduler::default();
        for slot in 3..AUXILIARY_CHANNEL {
            scheduler
                .enqueue(
                    slot,
                    0,
                    ChannelClass::LargeValue,
                    vec![1; CHANNEL_CHUNK_BYTES * 2],
                )
                .unwrap();
        }
        assert_eq!(
            scheduler.next_chunk().unwrap().class,
            ChannelClass::LargeValue
        );
        scheduler.accepted().unwrap();
        scheduler
            .enqueue(1, 0, ChannelClass::Requests, vec![2])
            .unwrap();
        let mut turns = 0;
        loop {
            let request = scheduler.next_chunk().unwrap().channel == 1;
            scheduler.accepted().unwrap();
            turns += 1;
            if request {
                break;
            }
        }
        assert!(turns <= 18, "new request waited {turns} frames");
    }

    #[test]
    fn credit_blocked_bulk_is_skipped_before_selecting_a_codec_extent() {
        let mut scheduler = ChannelScheduler::default();
        scheduler
            .enqueue(
                3,
                0,
                ChannelClass::LargeValue,
                vec![1; CHANNEL_CHUNK_BYTES * 2],
            )
            .unwrap();
        scheduler
            .enqueue(1, 0, ChannelClass::Requests, vec![2])
            .unwrap();
        let selected = scheduler
            .next_chunk_where(|class| class != ChannelClass::LargeValue)
            .unwrap();
        assert_eq!(selected.channel, 1);
        scheduler.accepted().unwrap();
        assert!(scheduler.next_chunk_where(|_| false).is_none());
        assert_eq!(scheduler.queued_messages(), 1);
    }

    #[test]
    fn spare_vec_capacity_is_not_retained_outside_the_byte_accounting() {
        let mut scheduler = ChannelScheduler::default();
        let mut payload = Vec::with_capacity(1024 * 1024);
        payload.push(1);
        scheduler
            .enqueue(1, 0, ChannelClass::Requests, payload)
            .unwrap();
        let allocation = &scheduler.channels[&1].messages[0].bytes;
        assert_eq!(
            std::mem::size_of_val(allocation.as_ref()),
            scheduler.queued_bytes()
        );
        assert_eq!(scheduler.queued_bytes(), 1);
    }

    #[test]
    fn tiny_data_messages_cannot_consume_control_reserve() {
        let mut scheduler = ChannelScheduler::default();
        for _ in 0..MAX_CHANNEL_QUEUED_MESSAGES - CONTROL_RESERVE_MESSAGES {
            scheduler
                .enqueue(3, 0, ChannelClass::Writes, vec![1])
                .unwrap();
        }
        assert!(
            scheduler
                .enqueue(3, 0, ChannelClass::Writes, vec![1])
                .is_err()
        );
        for _ in 0..CONTROL_RESERVE_MESSAGES {
            scheduler
                .enqueue(0, 0, ChannelClass::Control, vec![2])
                .unwrap();
        }
        assert!(
            scheduler
                .enqueue(0, 0, ChannelClass::Control, vec![2])
                .is_err()
        );
    }

    #[test]
    fn per_channel_fifo_and_channel_count_are_bounded() {
        let mut scheduler = ChannelScheduler::default();
        for value in [1, 2, 3] {
            scheduler
                .enqueue(3, 0, ChannelClass::Writes, vec![value])
                .unwrap();
        }
        assert!(
            scheduler
                .enqueue(MAX_CHANNELS as u16, 0, ChannelClass::Delivery, vec![1],)
                .is_err()
        );
        assert!(
            scheduler
                .enqueue(3, 1, ChannelClass::Writes, vec![1])
                .is_err()
        );
        for value in [1, 2, 3] {
            let chunk = scheduler.next_chunk().unwrap();
            assert_eq!(chunk.bytes, [value]);
            assert_eq!(chunk.sequence, u64::from(value - 1));
            scheduler.accepted().unwrap();
        }
        assert!(scheduler.next_chunk().is_none());
    }
}
