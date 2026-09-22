//! Receiver-consumption credits for bounded physical channel queues.
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll, Waker};

use super::channels::{
    CHANNEL_CHUNK_BYTES, CONTROL_RESERVE_BYTES, CONTROL_RESERVE_MESSAGES, ChannelClass,
    INTERACTIVE_RESERVE_BYTES, MAX_CHANNEL_FRAME_PAYLOAD, MAX_CHANNEL_QUEUED_MESSAGES,
};
use super::{
    WireChannelCredit, WireCreditKind, WireEnvelope, WireFrame, WireInboundContext, encode_frame,
};
use crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES;

/// A tiny physical frame still occupies a bounded queue slot.
pub const CHANNEL_FRAME_CREDIT_FLOOR: usize = 16 * 1024;
/// Charge shared by endpoint send windows and raw binding queue accounting.
pub fn channel_frame_credit_cost(encoded_len: usize) -> usize {
    encoded_len.max(CHANNEL_FRAME_CREDIT_FLOOR)
}

/// Connection-scoped flow-control state shared with the independent I/O pump.
pub type SharedChannelCredits = Arc<Mutex<ChannelCredits>>;

const WINDOWS: [usize; 6] = [
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
    1024 * 1024,
    1024 * 1024,
];
fn bucket(class: ChannelClass) -> usize {
    match class {
        ChannelClass::Control => 0,
        ChannelClass::Requests => 1,
        ChannelClass::Delivery => 2,
        ChannelClass::Writes | ChannelClass::LargeValue => 3,
        ChannelClass::Auxiliary => 4,
        ChannelClass::Progress => 5,
    }
}
fn bucket_class(index: usize) -> ChannelClass {
    // Keep the mapping scalar: optimized ARM64 code for a temporary indexed
    // enum array has been observed loading a stack slot before initializing it.
    match index {
        0 => ChannelClass::Control,
        1 => ChannelClass::Requests,
        2 => ChannelClass::Delivery,
        3 => ChannelClass::Writes,
        4 => ChannelClass::Auxiliary,
        5 => ChannelClass::Progress,
        _ => unreachable!("physical credit bucket out of range"),
    }
}

fn buffer_bucket(class: ChannelClass, bytes: usize) -> usize {
    match class {
        ChannelClass::Auxiliary => 3,
        ChannelClass::Progress => 5,
        ChannelClass::Control if bytes > CHANNEL_CHUNK_BYTES => 4,
        ChannelClass::Control => 0,
        _ if bytes > CHANNEL_CHUNK_BYTES => 2,
        _ => 1,
    }
}
fn grant_buffer_bucket(class: ChannelClass, bulk: bool) -> usize {
    buffer_bucket(class, if bulk { CHANNEL_CHUNK_BYTES + 1 } else { 1 })
}
#[derive(Clone, Copy, Default)]
struct BufferCost {
    bytes: usize,
    count: usize,
}

/// Ownership of one decoded byte message. Cloning shares the reservation;
/// capacity returns only when its last owner drops, including upper-layer staging.
#[derive(Clone)]
pub struct BufferLease(Arc<BufferLeaseInner>);
struct BufferLeaseInner {
    owner: Weak<Mutex<ChannelCredits>>,
    bucket: usize,
    bytes: usize,
}
impl std::fmt::Debug for BufferLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferLease")
            .field("bytes", &self.0.bytes)
            .finish()
    }
}
impl Drop for BufferLeaseInner {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let mut state = owner.lock().expect("channel credit mutex poisoned");
            state.received_buffers[self.bucket].bytes -= self.bytes;
            state.received_buffers[self.bucket].count -= 1;
            if !state.closed {
                state.buffer_grants[self.bucket].bytes += self.bytes;
                state.buffer_grants[self.bucket].count += 1;
                if let Some(waker) = state.waker.take() {
                    waker.wake();
                }
            }
        }
    }
}

/// Credits acknowledge raw-frame consumption, never semantic application or
/// authority. Separate class windows preserve query/control/auxiliary progress
/// while bulk bytes await the canonical consumer.
pub struct ChannelCredits {
    context: WireInboundContext,
    outstanding: [usize; 6],
    pending_grants: [usize; 6],
    next_sent: u64,
    next_received: u64,
    pending: Option<(Vec<u8>, usize, usize, WireCreditKind)>,
    sent_buffers: [BufferCost; 6],
    received_buffers: [BufferCost; 6],
    buffer_grants: [BufferCost; 6],
    closed: bool,
    waker: Option<Waker>,
}

impl ChannelCredits {
    /// Fresh connections receive fixed initial windows; reconnect never carries
    /// balances or grant sequence numbers from the previous connection.
    pub fn new(context: WireInboundContext) -> Self {
        Self {
            context,
            outstanding: [0; 6],
            pending_grants: [0; 6],
            next_sent: 0,
            next_received: 0,
            pending: None,
            sent_buffers: [BufferCost::default(); 6],
            received_buffers: [BufferCost::default(); 6],
            buffer_grants: [BufferCost::default(); 6],
            closed: false,
            waker: None,
        }
    }
    fn buffer_can_add(costs: &[BufferCost; 6], index: usize, bytes: usize) -> bool {
        if index == 3 || index == 5 {
            return costs[index]
                .bytes
                .checked_add(bytes)
                .is_some_and(|n| n <= MAX_LOGICAL_MESSAGE_BYTES)
                && costs[index].count
                    < if index == 5 {
                        CONTROL_RESERVE_MESSAGES
                    } else {
                        MAX_CHANNEL_QUEUED_MESSAGES
                    };
        }
        let canonical = [0, 1, 2, 4];
        let total_bytes: usize = canonical.iter().map(|i| costs[*i].bytes).sum();
        let total_count: usize = canonical.iter().map(|i| costs[*i].count).sum();
        let control = index == 0 || index == 4;
        total_bytes.checked_add(bytes).is_some_and(|n| {
            n <= MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES + CONTROL_RESERVE_BYTES
        }) && total_count < MAX_CHANNEL_QUEUED_MESSAGES
            && (control
                || (costs[1].bytes + costs[2].bytes + bytes
                    <= MAX_LOGICAL_MESSAGE_BYTES + INTERACTIVE_RESERVE_BYTES
                    && costs[1].count + costs[2].count
                        < MAX_CHANNEL_QUEUED_MESSAGES - CONTROL_RESERVE_MESSAGES))
            && ((index != 2 && index != 4)
                || costs[2].bytes + costs[4].bytes + bytes <= MAX_LOGICAL_MESSAGE_BYTES)
    }
    /// Check before semantic enqueue mutates routing, generation or codec state.
    pub(crate) fn can_reserve_message(&self, class: ChannelClass, bytes: usize) -> bool {
        !self.closed && Self::buffer_can_add(&self.sent_buffers, buffer_bucket(class, bytes), bytes)
    }
    pub(crate) fn reserve_message(&mut self, class: ChannelClass, bytes: usize) {
        assert!(self.can_reserve_message(class, bytes));
        let cost = &mut self.sent_buffers[buffer_bucket(class, bytes)];
        cost.bytes += bytes;
        cost.count += 1;
    }
    pub(crate) fn receive_message(
        owner: &SharedChannelCredits,
        class: ChannelClass,
        bytes: usize,
    ) -> Result<BufferLease, String> {
        let mut state = owner.lock().map_err(|_| "channel credit mutex poisoned")?;
        let index = buffer_bucket(class, bytes);
        if state.closed || !Self::buffer_can_add(&state.received_buffers, index, bytes) {
            return Err("decoded message receive window exceeded".into());
        }
        state.received_buffers[index].bytes += bytes;
        state.received_buffers[index].count += 1;
        Ok(BufferLease(Arc::new(BufferLeaseInner {
            owner: Arc::downgrade(owner),
            bucket: index,
            bytes,
        })))
    }
    /// Retire this connection; later lease drops cannot grant a new connection credit.
    pub fn close(&mut self) {
        self.closed = true;
        self.pending = None;
        self.pending_grants = [0; 6];
        self.buffer_grants = [BufferCost::default(); 6];
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
    }

    /// Eligibility is checked before selecting or advancing a channel codec.
    pub fn can_send(&self, class: ChannelClass) -> bool {
        let index = bucket(class);
        WINDOWS[index] - self.outstanding[index] >= MAX_CHANNEL_FRAME_PAYLOAD + 256
    }
    /// Charge exactly once after the lower queue accepts a physical frame.
    pub fn charge(&mut self, class: ChannelClass, encoded_len: usize) -> Result<(), String> {
        let index = bucket(class);
        let cost = channel_frame_credit_cost(encoded_len);
        let next = self.outstanding[index]
            .checked_add(cost)
            .ok_or("channel credit arithmetic overflow")?;
        if next > WINDOWS[index] {
            return Err("channel sender exceeded receive credit".into());
        }
        self.outstanding[index] = next;
        Ok(())
    }
    /// Return credit only after removing the raw physical frame from its queue.
    pub fn consumed(&mut self, class: ChannelClass, encoded_len: usize) -> Result<(), String> {
        let index = bucket(class);
        let cost = channel_frame_credit_cost(encoded_len);
        let next = self.pending_grants[index]
            .checked_add(cost)
            .ok_or("channel grant arithmetic overflow")?;
        if next > WINDOWS[index] {
            return Err("channel receiver exceeded ungranted window".into());
        }
        self.pending_grants[index] = next;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }
    /// Validate authenticated context, exact grant sequence and outstanding
    /// balance before making another outbound channel extent eligible.
    pub fn receive_credit(&mut self, grant: WireChannelCredit) -> Result<(), String> {
        self.context
            .validate_envelope_metadata(&WireEnvelope {
                protocol_version: grant.protocol_version,
                features: grant.features,
                session: grant.session,
                payload: Vec::new(),
            })
            .map_err(|error| format!("invalid credit context: {error:?}"))?;
        if grant.sequence != self.next_received {
            return Err("channel credit sequence mismatch".into());
        }
        let index = bucket(grant.class);
        let amount = usize::try_from(grant.consumed_bytes)
            .map_err(|_| "channel credit does not fit receiver")?;
        if amount == 0 {
            return Err("empty channel credit".into());
        }
        match grant.kind {
            WireCreditKind::Frames if amount > self.outstanding[index] => {
                return Err("channel credit exceeds outstanding balance".into());
            }
            WireCreditKind::Messages { count, bulk } => {
                let index = grant_buffer_bucket(grant.class, bulk);
                let cost = &self.sent_buffers[index];
                if count == 0 || count as usize > cost.count || amount > cost.bytes {
                    return Err("message credit exceeds outstanding balance".into());
                }
            }
            _ => {}
        }
        let successor = self
            .next_received
            .checked_add(1)
            .ok_or("channel credit sequence exhausted")?;
        match grant.kind {
            WireCreditKind::Frames => self.outstanding[index] -= amount,
            WireCreditKind::Messages { count, bulk } => {
                let index = grant_buffer_bucket(grant.class, bulk);
                self.sent_buffers[index].bytes -= amount;
                self.sent_buffers[index].count -= count as usize;
            }
        }
        self.next_received = successor;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }
    /// Retain the exact uncompressed grant frame through physical backpressure.
    pub fn peek_grant(&mut self) -> Result<Option<Vec<u8>>, String> {
        if let Some((bytes, _, _, _)) = &self.pending {
            return Ok(Some(bytes.clone()));
        }
        let (index, amount, class, kind) =
            if let Some(index) = self.buffer_grants.iter().position(|c| c.count != 0) {
                let cost = self.buffer_grants[index];
                (
                    index,
                    cost.bytes,
                    match index {
                        0 | 4 => ChannelClass::Control,
                        1 => ChannelClass::Requests,
                        2 => ChannelClass::Writes,
                        3 => ChannelClass::Auxiliary,
                        5 => ChannelClass::Progress,
                        _ => unreachable!("message credit bucket out of range"),
                    },
                    WireCreditKind::Messages {
                        count: cost.count as u32,
                        bulk: index == 2 || index == 4,
                    },
                )
            } else if let Some(index) = self.pending_grants.iter().position(|amount| *amount != 0) {
                (
                    index,
                    self.pending_grants[index],
                    bucket_class(index),
                    WireCreditKind::Frames,
                )
            } else {
                return Ok(None);
            };
        let frame = encode_frame(&WireFrame::ChannelCredit(WireChannelCredit {
            protocol_version: self.context.expected_protocol_version(),
            features: self.context.negotiated_features(),
            session: self.context.expected_session().cloned(),
            class,
            sequence: self.next_sent,
            consumed_bytes: amount as u64,
            kind,
        }))
        .map_err(|error| error.to_string())?;
        self.pending = Some((frame.clone(), index, amount, kind));
        Ok(Some(frame))
    }
    /// Commit one grant only after physical acceptance. Newly consumed bytes
    /// remain queued behind the exact retained grant snapshot.
    pub fn accept_grant(&mut self) -> Result<(), String> {
        let next = self
            .next_sent
            .checked_add(1)
            .ok_or("channel credit sequence exhausted")?;
        let (_, index, amount, kind) = self
            .pending
            .take()
            .ok_or("no channel credit grant pending")?;
        match kind {
            WireCreditKind::Frames => {
                self.pending_grants[index] = self.pending_grants[index]
                    .checked_sub(amount)
                    .ok_or("channel grant accounting underflow")?
            }
            WireCreditKind::Messages { count, .. } => {
                self.buffer_grants[index].bytes -= amount;
                self.buffer_grants[index].count -= count as usize;
            }
        }
        self.next_sent = next;
        Ok(())
    }
    /// Whether receiver consumption has queued a bounded grant frame.
    pub fn has_pending_grant(&self) -> bool {
        self.pending.is_some()
            || self.pending_grants.iter().any(|amount| *amount != 0)
            || self.buffer_grants.iter().any(|cost| cost.count != 0)
    }
    /// Readiness for either a pending grant or a channel waiting for new credit.
    pub fn poll_grant_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pending.is_some()
            || self.pending_grants.iter().any(|amount| *amount != 0)
            || self.buffer_grants.iter().any(|cost| cost.count != 0)
        {
            Poll::Ready(())
        } else {
            self.waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
    /// Register the same independent I/O wake while an auxiliary window is full.
    pub fn register_waker(&mut self, cx: &Context<'_>) {
        self.waker = Some(cx.waker().clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, decode_frame};
    fn context() -> WireInboundContext {
        WireInboundContext::new(WIRE_PROTOCOL_VERSION, FEATURE_SYNC_MESSAGE_PAYLOAD, None)
    }
    // Internal tests pin physical-byte credit and fixed postcard bytes, neither
    // of which can be asserted through the semantic row-query API.
    #[test]
    fn retained_view_capacity_cannot_block_bounded_progress_reply() {
        let credits = Arc::new(Mutex::new(ChannelCredits::new(context())));
        let view = ChannelCredits::receive_message(
            &credits,
            ChannelClass::Delivery,
            MAX_LOGICAL_MESSAGE_BYTES,
        )
        .unwrap();
        assert!(
            ChannelCredits::receive_message(
                &credits,
                ChannelClass::Delivery,
                CHANNEL_CHUNK_BYTES + 1
            )
            .is_err()
        );
        let repair = ChannelCredits::receive_message(
            &credits,
            ChannelClass::Progress,
            MAX_LOGICAL_MESSAGE_BYTES,
        )
        .unwrap();
        assert!(ChannelCredits::receive_message(&credits, ChannelClass::Progress, 1).is_err());
        drop(repair);
        assert!(
            ChannelCredits::receive_message(
                &credits,
                ChannelClass::Progress,
                MAX_LOGICAL_MESSAGE_BYTES
            )
            .is_ok()
        );
        credits.lock().unwrap().close();
        drop(view);
        assert_eq!(
            credits
                .lock()
                .unwrap()
                .received_buffers
                .iter()
                .map(|cost| cost.bytes)
                .sum::<usize>(),
            0
        );
        assert!(credits.lock().unwrap().peek_grant().unwrap().is_none());
    }

    #[test]
    fn credit_byte_fixture_retry_sequence_and_unearned_grants_are_checked() {
        let mut sender = ChannelCredits::new(context());
        let mut receiver = ChannelCredits::new(context());
        sender.charge(ChannelClass::Requests, 7).unwrap();
        receiver.consumed(ChannelClass::Requests, 7).unwrap();
        let bytes = receiver.peek_grant().unwrap().unwrap();
        assert_eq!(hex::encode(&bytes), "05030100010080800100");
        assert_eq!(receiver.peek_grant().unwrap().unwrap(), bytes);
        let WireFrame::ChannelCredit(grant) = decode_frame(&bytes).unwrap() else {
            panic!("credit frame")
        };
        sender.receive_credit(grant.clone()).unwrap();
        assert!(
            sender.receive_credit(grant.clone()).is_err(),
            "duplicate grant must not inflate a window"
        );
        receiver.accept_grant().unwrap();
        assert!(receiver.peek_grant().unwrap().is_none());
        let mut unearned = grant.clone();
        unearned.sequence = 1;
        assert!(sender.receive_credit(unearned).is_err());
        assert!(
            ChannelCredits::new(context())
                .receive_credit(grant)
                .is_err(),
            "old connection credits are not transferable"
        );
    }
    #[test]
    fn control_reserve_is_not_a_large_catalogue_cap_and_total_buffers_stay_bounded() {
        let mut credits = ChannelCredits::new(context());
        assert!(credits.can_reserve_message(ChannelClass::Control, MAX_LOGICAL_MESSAGE_BYTES));
        credits.reserve_message(ChannelClass::Control, MAX_LOGICAL_MESSAGE_BYTES);
        for _ in 0..INTERACTIVE_RESERVE_BYTES / CHANNEL_CHUNK_BYTES {
            credits.reserve_message(ChannelClass::Requests, CHANNEL_CHUNK_BYTES);
        }
        for _ in 0..CONTROL_RESERVE_BYTES / CHANNEL_CHUNK_BYTES {
            credits.reserve_message(ChannelClass::Control, CHANNEL_CHUNK_BYTES);
        }
        for class in [
            ChannelClass::Control,
            ChannelClass::Requests,
            ChannelClass::Delivery,
            ChannelClass::Writes,
        ] {
            assert!(!credits.can_reserve_message(class, 1));
        }
        assert!(
            credits.can_reserve_message(ChannelClass::Auxiliary, MAX_LOGICAL_MESSAGE_BYTES),
            "immutable chunk progress owns separate reserved capacity"
        );
    }

    #[test]
    fn decoded_buffer_credit_waits_for_last_owner_and_dies_with_connection() {
        let receiver = Arc::new(Mutex::new(ChannelCredits::new(context())));
        let mut sender = ChannelCredits::new(context());
        sender.reserve_message(ChannelClass::Requests, 100);
        let lease =
            ChannelCredits::receive_message(&receiver, ChannelClass::Requests, 100).unwrap();
        let staged = lease.clone();
        drop(lease);
        assert!(
            !receiver.lock().unwrap().has_pending_grant(),
            "moving into a deferred queue cannot return capacity"
        );
        drop(staged);
        let bytes = receiver.lock().unwrap().peek_grant().unwrap().unwrap();
        let WireFrame::ChannelCredit(grant) = decode_frame(&bytes).unwrap() else {
            panic!("credit")
        };
        assert!(matches!(
            grant.kind,
            WireCreditKind::Messages {
                count: 1,
                bulk: false
            }
        ));
        sender.receive_credit(grant).unwrap();
        receiver.lock().unwrap().accept_grant().unwrap();
        assert_eq!(sender.sent_buffers[1].bytes, 0);
        let lease =
            ChannelCredits::receive_message(&receiver, ChannelClass::Requests, 100).unwrap();
        receiver.lock().unwrap().close();
        drop(lease);
        assert_eq!(receiver.lock().unwrap().received_buffers[1].bytes, 0);
        assert!(
            !receiver.lock().unwrap().has_pending_grant(),
            "old leases cannot emit reconnect credits"
        );
    }

    #[test]
    fn exhausted_bulk_window_preserves_requests_deliveries_control_and_auxiliary() {
        let mut credits = ChannelCredits::new(context());
        for _ in 0..64 {
            credits.charge(ChannelClass::LargeValue, 64 * 1024).unwrap();
        }
        assert!(!credits.can_send(ChannelClass::LargeValue));
        assert!(!credits.can_send(ChannelClass::Writes));
        for class in [
            ChannelClass::Control,
            ChannelClass::Requests,
            ChannelClass::Delivery,
            ChannelClass::Auxiliary,
        ] {
            assert!(credits.can_send(class));
        }
        assert!(credits.charge(ChannelClass::Writes, 1).is_err());
        assert_eq!(channel_frame_credit_cost(1), 16 * 1024);
        assert_eq!(channel_frame_credit_cost(72 * 1024), 72 * 1024);
    }
    // Internal coverage is needed because row results do not expose which
    // physical or decoded-resource credit bucket a serialized grant replenishes.
    #[test]
    fn physical_grants_replenish_their_original_class_windows() {
        const BYTES: usize = 65_573;
        for class in [
            ChannelClass::Control,
            ChannelClass::Requests,
            ChannelClass::Delivery,
            ChannelClass::Writes,
            ChannelClass::LargeValue,
            ChannelClass::Auxiliary,
            ChannelClass::Progress,
        ] {
            let mut sender = ChannelCredits::new(context());
            let mut receiver = ChannelCredits::new(context());
            sender.charge(class, BYTES).unwrap();
            receiver.consumed(class, BYTES).unwrap();
            let bytes = receiver.peek_grant().unwrap().unwrap();
            let WireFrame::ChannelCredit(grant) = decode_frame(&bytes).unwrap() else {
                panic!("expected a credit frame");
            };
            let expected_class = match class {
                ChannelClass::LargeValue => ChannelClass::Writes,
                other => other,
            };
            assert_eq!(grant.class, expected_class, "charged class={class:?}");
            assert_eq!(grant.kind, WireCreditKind::Frames);
            assert_eq!(grant.consumed_bytes, BYTES as u64);
            if expected_class != ChannelClass::Control {
                let mut wrong_class = grant.clone();
                wrong_class.class = ChannelClass::Control;
                assert!(sender.receive_credit(wrong_class).is_err());
            }
            sender
                .receive_credit(grant)
                .expect("replenish the charged window");
            receiver.accept_grant().unwrap();
            assert!(receiver.peek_grant().unwrap().is_none());
        }
    }

    #[test]
    fn decoded_grants_preserve_every_resource_bucket() {
        for (index, class, bulk) in [
            (0, ChannelClass::Control, false),
            (1, ChannelClass::Requests, false),
            (2, ChannelClass::Writes, true),
            (3, ChannelClass::Auxiliary, false),
            (4, ChannelClass::Control, true),
            (5, ChannelClass::Progress, false),
        ] {
            let mut receiver = ChannelCredits::new(context());
            receiver.buffer_grants[index] = BufferCost {
                bytes: 65_573,
                count: 1,
            };
            let bytes = receiver.peek_grant().unwrap().unwrap();
            let WireFrame::ChannelCredit(grant) = decode_frame(&bytes).unwrap() else {
                panic!("expected decoded-resource credit");
            };
            assert_eq!(grant.class, class, "resource bucket={index}");
            assert_eq!(grant.kind, WireCreditKind::Messages { count: 1, bulk });
            assert_eq!(grant.consumed_bytes, 65_573);
            receiver.accept_grant().unwrap();
            assert!(receiver.peek_grant().unwrap().is_none());
        }
    }

    #[test]
    fn bulk_message_grants_preserve_resource_kind_without_physical_debits() {
        const BYTES: usize = 721_647;
        for class in [ChannelClass::Control, ChannelClass::Writes] {
            let receiver = Arc::new(Mutex::new(ChannelCredits::new(context())));
            let mut sender = ChannelCredits::new(context());
            sender.reserve_message(class, BYTES);
            let lease = ChannelCredits::receive_message(&receiver, class, BYTES).unwrap();
            drop(lease);
            let bytes = receiver.lock().unwrap().peek_grant().unwrap().unwrap();
            let WireFrame::ChannelCredit(grant) = decode_frame(&bytes).unwrap() else {
                panic!("expected a credit frame");
            };
            assert_eq!(grant.class, class);
            assert_eq!(grant.consumed_bytes, BYTES as u64);
            assert_eq!(grant.sequence, 0);
            assert_eq!(
                grant.kind,
                WireCreditKind::Messages {
                    count: 1,
                    bulk: true
                }
            );
            let mut false_frame_grant = grant.clone();
            false_frame_grant.kind = WireCreditKind::Frames;
            assert!(
                sender.receive_credit(false_frame_grant).is_err(),
                "decoded-buffer consumption must not fund physical frames"
            );
            sender
                .receive_credit(grant)
                .expect("release the reserved decoded buffer");
            receiver.lock().unwrap().accept_grant().unwrap();
            assert!(receiver.lock().unwrap().peek_grant().unwrap().is_none());
        }
    }
}
