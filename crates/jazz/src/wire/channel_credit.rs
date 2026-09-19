//! Receiver-consumption credits for bounded physical channel queues.
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use super::channels::{ChannelClass, MAX_CHANNEL_FRAME_PAYLOAD};
use super::{WireChannelCredit, WireEnvelope, WireFrame, WireInboundContext, encode_frame};

/// A tiny physical frame still occupies a bounded queue slot.
pub const CHANNEL_FRAME_CREDIT_FLOOR: usize = 16 * 1024;
/// Charge shared by endpoint send windows and raw binding queue accounting.
pub fn channel_frame_credit_cost(encoded_len: usize) -> usize {
    encoded_len.max(CHANNEL_FRAME_CREDIT_FLOOR)
}

/// Connection-scoped flow-control state shared with the independent I/O pump.
pub type SharedChannelCredits = Arc<Mutex<ChannelCredits>>;

const WINDOWS: [usize; 5] = [
    256 * 1024,
    512 * 1024,
    1024 * 1024,
    4 * 1024 * 1024,
    1024 * 1024,
];
fn bucket(class: ChannelClass) -> usize {
    match class {
        ChannelClass::Control => 0,
        ChannelClass::Requests => 1,
        ChannelClass::Delivery => 2,
        ChannelClass::Writes | ChannelClass::LargeValue => 3,
        ChannelClass::Auxiliary => 4,
    }
}
fn bucket_class(index: usize) -> ChannelClass {
    [
        ChannelClass::Control,
        ChannelClass::Requests,
        ChannelClass::Delivery,
        ChannelClass::Writes,
        ChannelClass::Auxiliary,
    ][index]
}

/// Credits acknowledge raw-frame consumption, never semantic application or
/// authority. Separate class windows preserve query/control/auxiliary progress
/// while bulk bytes await the canonical consumer.
pub struct ChannelCredits {
    context: WireInboundContext,
    outstanding: [usize; 5],
    pending_grants: [usize; 5],
    next_sent: u64,
    next_received: u64,
    pending: Option<(Vec<u8>, usize, usize)>,
    waker: Option<Waker>,
}

impl ChannelCredits {
    /// Fresh connections receive fixed initial windows; reconnect never carries
    /// balances or grant sequence numbers from the previous connection.
    pub fn new(context: WireInboundContext) -> Self {
        Self {
            context,
            outstanding: [0; 5],
            pending_grants: [0; 5],
            next_sent: 0,
            next_received: 0,
            pending: None,
            waker: None,
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
        if amount == 0 || amount > self.outstanding[index] {
            return Err("channel credit exceeds outstanding balance".into());
        }
        let successor = self
            .next_received
            .checked_add(1)
            .ok_or("channel credit sequence exhausted")?;
        self.outstanding[index] -= amount;
        self.next_received = successor;
        if let Some(waker) = self.waker.take() {
            waker.wake();
        }
        Ok(())
    }
    /// Retain the exact uncompressed grant frame through physical backpressure.
    pub fn peek_grant(&mut self) -> Result<Option<Vec<u8>>, String> {
        if let Some((bytes, _, _)) = &self.pending {
            return Ok(Some(bytes.clone()));
        }
        let Some(index) = self.pending_grants.iter().position(|amount| *amount != 0) else {
            return Ok(None);
        };
        let amount = self.pending_grants[index];
        let frame = encode_frame(&WireFrame::ChannelCredit(WireChannelCredit {
            protocol_version: self.context.expected_protocol_version(),
            features: self.context.negotiated_features(),
            session: self.context.expected_session().cloned(),
            class: bucket_class(index),
            sequence: self.next_sent,
            consumed_bytes: amount as u64,
        }))
        .map_err(|error| error.to_string())?;
        self.pending = Some((frame.clone(), index, amount));
        Ok(Some(frame))
    }
    /// Commit one grant only after physical acceptance. Newly consumed bytes
    /// remain queued behind the exact retained grant snapshot.
    pub fn accept_grant(&mut self) -> Result<(), String> {
        let next = self
            .next_sent
            .checked_add(1)
            .ok_or("channel credit sequence exhausted")?;
        let (_, index, amount) = self
            .pending
            .take()
            .ok_or("no channel credit grant pending")?;
        self.pending_grants[index] = self.pending_grants[index]
            .checked_sub(amount)
            .ok_or("channel grant accounting underflow")?;
        self.next_sent = next;
        Ok(())
    }
    /// Whether receiver consumption has queued a bounded grant frame.
    pub fn has_pending_grant(&self) -> bool {
        self.pending.is_some() || self.pending_grants.iter().any(|amount| *amount != 0)
    }
    /// Readiness for either a pending grant or a channel waiting for new credit.
    pub fn poll_grant_ready(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        if self.pending.is_some() || self.pending_grants.iter().any(|amount| *amount != 0) {
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
    fn credit_byte_fixture_retry_sequence_and_unearned_grants_are_checked() {
        let mut sender = ChannelCredits::new(context());
        let mut receiver = ChannelCredits::new(context());
        sender.charge(ChannelClass::Requests, 7).unwrap();
        receiver.consumed(ChannelClass::Requests, 7).unwrap();
        let bytes = receiver.peek_grant().unwrap().unwrap();
        assert_eq!(hex::encode(&bytes), "050301000100808001");
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
}
