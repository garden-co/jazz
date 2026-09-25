//! Jazz message dependencies above opaque reliable ordered byte streams.
//!
//! Epochs order only semantic barriers. Ordinary streams in one epoch remain
//! independent. Admission means insertion in the single canonical FIFO, not
//! query completion, durability or remote transaction settlement.
use crate::protocol::SyncMessage;
use crate::wire::channel_credit::BufferLease;
use crate::wire::channels::{ChannelClass, MAX_CHANNEL_QUEUED_MESSAGES, MAX_CHANNELS};
use crate::wire::{WireError, WireInboundContext};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;

pub const ENVELOPE_VERSION: u8 = 1;
/// Bounded metadata allowance within the existing raw byte-message limit.
pub const MAX_ENVELOPE_OVERHEAD: usize = 1024;

#[derive(Debug, Serialize, Deserialize)]
struct Envelope<'a> {
    version: u8,
    epoch: u64,
    ordinal: u64,
    predecessors: Option<Vec<(u16, u64)>>,
    #[serde(borrow)]
    payload: &'a [u8],
}

fn decode_envelope(bytes: &[u8]) -> Result<Envelope<'_>, String> {
    let (envelope, rest) = postcard::take_from_bytes(bytes)
        .map_err(|error| format!("invalid routed envelope: {error}"))?;
    if !rest.is_empty() {
        return Err("trailing bytes after routed envelope".into());
    }
    Ok(envelope)
}

#[cfg(test)]
thread_local! {
    static ROUTED_PAYLOAD_LIMIT_FOR_TEST: std::cell::Cell<Option<usize>> =
        const { std::cell::Cell::new(None) };
}

/// Lower this thread's routed payload limit so a regression can cross it
/// without hundreds of MiB of rows. `None` restores the protocol limit.
#[cfg(test)]
pub(super) fn set_routed_payload_limit_for_test(limit: Option<usize>) {
    ROUTED_PAYLOAD_LIMIT_FOR_TEST.with(|cell| cell.set(limit));
}

/// Largest semantic payload one routed message admits: the logical-message
/// ceiling minus the bounded envelope allowance.
pub(super) fn max_routed_payload_bytes() -> usize {
    #[cfg(test)]
    if let Some(limit) = ROUTED_PAYLOAD_LIMIT_FOR_TEST.with(std::cell::Cell::get) {
        return limit;
    }
    crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES - MAX_ENVELOPE_OVERHEAD
}

/// The lease follows payload ownership through canonical and deferred queues.
/// Its last owner releases transport capacity; it has no authority semantics.
#[derive(Debug)]
pub struct ReceivedSyncMessage {
    /// The decoded canonical message.
    pub message: SyncMessage,
    pub(crate) lease: Option<BufferLease>,
    /// The checked (untrusted-encoder) wire decoder produced this message, so
    /// every version receipt it carries has already been validated.
    pub(crate) receipts_validated: bool,
}
impl ReceivedSyncMessage {
    pub(crate) fn unleased(message: SyncMessage) -> Self {
        Self {
            message,
            lease: None,
            receipts_validated: false,
        }
    }
}
struct Pending {
    channel: u16,
    epoch: u64,
    ordinal: u64,
    predecessors: Option<Vec<(u16, u64)>>,
    message: SyncMessage,
    lease: BufferLease,
    receipts_validated: bool,
}

pub(super) struct RoutedMessages {
    sent_epoch: u64,
    sent: [u64; MAX_CHANNELS],
    receive_epoch: u64,
    received: [u64; MAX_CHANNELS],
    admitted: [u64; MAX_CHANNELS],
    pending: VecDeque<Pending>,
    ready: VecDeque<ReceivedSyncMessage>,
}
impl Default for RoutedMessages {
    fn default() -> Self {
        Self {
            sent_epoch: 0,
            sent: [0; MAX_CHANNELS],
            receive_epoch: 0,
            received: [0; MAX_CHANNELS],
            admitted: [0; MAX_CHANNELS],
            pending: VecDeque::new(),
            ready: VecDeque::new(),
        }
    }
}
impl RoutedMessages {
    /// Preparation is read-only. Failed byte-queue admission cannot consume an
    /// epoch, ordinal, dependency watermark or buffer reservation.
    pub(super) fn prepare(
        &self,
        channel: u16,
        barrier: bool,
        payload: Vec<u8>,
    ) -> Result<Vec<u8>, String> {
        if channel as usize >= MAX_CHANNELS {
            return Err("invalid logical stream ID".into());
        }
        if payload.len() > max_routed_payload_bytes() {
            return Err("semantic message exceeds routed payload limit".into());
        }
        let ordinal = self.sent[channel as usize]
            .checked_add(1)
            .ok_or("logical ordinal exhausted")?;
        if barrier {
            self.sent_epoch
                .checked_add(1)
                .ok_or("logical epoch exhausted")?;
        }
        let predecessors = barrier.then(|| {
            self.sent
                .iter()
                .enumerate()
                .filter_map(|(slot, n)| (*n != 0).then_some((slot as u16, *n)))
                .collect()
        });
        postcard::to_allocvec(&Envelope {
            version: ENVELOPE_VERSION,
            epoch: self.sent_epoch,
            ordinal,
            predecessors,
            payload: &payload,
        })
        .map_err(|e| e.to_string())
    }
    pub(super) fn accepted(&mut self, channel: u16, barrier: bool) {
        self.sent[channel as usize] += 1;
        if barrier {
            self.sent_epoch += 1;
        }
    }
    pub(super) fn receive(
        &mut self,
        channel: u16,
        class: ChannelClass,
        bytes: Vec<u8>,
        lease: BufferLease,
        context: &WireInboundContext,
    ) -> Result<(), WireError> {
        self.receive_inner(channel, class, bytes, lease, context)
    }
    fn receive_inner(
        &mut self,
        channel: u16,
        class: ChannelClass,
        bytes: Vec<u8>,
        lease: BufferLease,
        context: &WireInboundContext,
    ) -> Result<(), WireError> {
        let malformed = |message: String| {
            crate::wire::WireError::new(
                crate::wire::WireErrorCode::MalformedFrame,
                crate::wire::WireRetry::Never,
                message,
            )
        };
        let envelope = decode_envelope(&bytes).map_err(malformed)?;
        let slot = channel as usize;
        if envelope.version != ENVELOPE_VERSION
            || envelope.epoch < self.receive_epoch
            || envelope.epoch.saturating_sub(self.receive_epoch)
                > MAX_CHANNEL_QUEUED_MESSAGES as u64
            || envelope.ordinal
                != self.received[slot]
                    .checked_add(1)
                    .ok_or_else(|| malformed("logical ordinal exhausted".into()))?
        {
            return Err(malformed("invalid routed epoch or stream ordinal".into()));
        }
        if let Some(dependencies) = &envelope.predecessors {
            if dependencies.len() > MAX_CHANNELS
                || dependencies.windows(2).any(|w| w[0].0 >= w[1].0)
                || dependencies
                    .iter()
                    .any(|(slot, ordinal)| *slot as usize >= MAX_CHANNELS || *ordinal == 0)
            {
                return Err(malformed("invalid barrier dependency vector".into()));
            }
        }
        if self.pending.len() + self.ready.len()
            >= MAX_CHANNEL_QUEUED_MESSAGES + crate::wire::channels::CONTROL_RESERVE_MESSAGES
        {
            return Err(malformed("routed message queue count exceeded".into()));
        }
        let message = context.decode_semantic_payload(envelope.payload)?;
        let (expected_class, barrier) = super::channel_endpoint::message_class(&message);
        if class != expected_class || barrier != envelope.predecessors.is_some() {
            return Err(malformed(
                "message routing metadata disagrees with semantics".into(),
            ));
        }
        self.received[slot] = envelope.ordinal;
        self.pending.push_back(Pending {
            channel,
            epoch: envelope.epoch,
            ordinal: envelope.ordinal,
            predecessors: envelope.predecessors,
            message,
            lease,
            receipts_validated: context.validates_receipts(),
        });
        loop {
            let eligible = self.pending.iter().position(|pending| {
                pending.epoch == self.receive_epoch
                    && pending.ordinal == self.admitted[pending.channel as usize] + 1
                    && pending.predecessors.as_ref().is_none_or(|dependencies| {
                        dependencies
                            .iter()
                            .all(|(slot, n)| self.admitted[*slot as usize] >= *n)
                    })
            });
            let Some(index) = eligible else { break };
            let pending = self.pending.remove(index).unwrap();
            self.admitted[pending.channel as usize] = pending.ordinal;
            if pending.predecessors.is_some() {
                self.receive_epoch = self
                    .receive_epoch
                    .checked_add(1)
                    .ok_or_else(|| malformed("logical epoch exhausted".into()))?;
            }
            self.ready.push_back(ReceivedSyncMessage {
                message: pending.message,
                lease: Some(pending.lease),
                receipts_validated: pending.receipts_validated,
            });
        }
        Ok(())
    }
    pub(super) fn pop(&mut self) -> Option<ReceivedSyncMessage> {
        self.ready.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_ordinary_router_queue_keeps_progress_capacity() {
        let message = SyncMessage::RowVersionPayloads {
            version_bundles: Vec::new(),
        };
        let mut receiver = RoutedMessages::default();
        for _ in 0..MAX_CHANNEL_QUEUED_MESSAGES {
            receiver
                .ready
                .push_back(ReceivedSyncMessage::unleased(message.clone()));
        }
        let mut sender = RoutedMessages::default();
        let context = WireInboundContext::new(
            crate::wire::WIRE_PROTOCOL_VERSION,
            crate::wire::FEATURE_SYNC_MESSAGE_PAYLOAD,
            None,
        );
        let credits = std::sync::Arc::new(std::sync::Mutex::new(
            crate::wire::channel_credit::ChannelCredits::new(context.clone()),
        ));
        for _ in 0..crate::wire::channels::CONTROL_RESERVE_MESSAGES {
            let bytes = sender
                .prepare(
                    crate::wire::channels::PROGRESS_CHANNEL,
                    false,
                    crate::wire::encode_sync_message(&message).unwrap(),
                )
                .unwrap();
            let lease = crate::wire::channel_credit::ChannelCredits::receive_message(
                &credits,
                ChannelClass::Progress,
                bytes.len(),
            )
            .unwrap();
            receiver
                .receive(
                    crate::wire::channels::PROGRESS_CHANNEL,
                    ChannelClass::Progress,
                    bytes,
                    lease,
                    &context,
                )
                .unwrap();
            sender.accepted(crate::wire::channels::PROGRESS_CHANNEL, false);
        }
        assert_eq!(
            receiver.ready.len(),
            MAX_CHANNEL_QUEUED_MESSAGES + crate::wire::channels::CONTROL_RESERVE_MESSAGES
        );
        assert!(
            crate::wire::channel_credit::ChannelCredits::receive_message(
                &credits,
                ChannelClass::Progress,
                1
            )
            .is_err()
        );
    }

    #[test]
    fn routed_envelope_v1_corpus_is_exact() {
        let mut router = RoutedMessages::default();
        let ordinary = router.prepare(3, false, vec![0xaa, 0xbb]).unwrap();
        assert_eq!(ordinary, [1, 0, 1, 0, 2, 0xaa, 0xbb]);
        router.accepted(3, false);
        let barrier = router.prepare(0, true, vec![0xcc]).unwrap();
        assert_eq!(barrier, [1, 0, 1, 1, 1, 3, 1, 1, 0xcc]);
        for bytes in [ordinary, barrier] {
            let envelope = decode_envelope(&bytes).unwrap();
            assert_eq!(postcard::to_allocvec(&envelope).unwrap(), bytes);
            let mut trailing = bytes.clone();
            trailing.push(0);
            assert!(
                decode_envelope(&trailing)
                    .unwrap_err()
                    .contains("trailing bytes")
            );
        }
    }

    #[test]
    fn barrier_preparation_does_not_consume_epoch_or_dependency_on_failed_admission() {
        let mut router = RoutedMessages::default();
        router.accepted(3, false);
        let first = router.prepare(0, true, vec![1]).unwrap();
        // The byte backend rejected this attempt. Retrying must be identical.
        assert_eq!(router.prepare(0, true, vec![1]).unwrap(), first);
        assert_eq!(router.sent_epoch, 0);
        assert_eq!(router.sent[0], 0);
        router.accepted(0, true);
        let next_bytes = router.prepare(1, false, vec![2]).unwrap();
        let next: Envelope = postcard::from_bytes(&next_bytes).unwrap();
        assert_eq!(next.epoch, 1);
        assert_eq!(next.ordinal, 1);
        let barrier: Envelope = postcard::from_bytes(&first).unwrap();
        assert_eq!(barrier.predecessors, Some(vec![(3, 1)]));
    }
}
