//! Jazz routing and semantic payloads above independent ordered byte streams.
use crate::protocol::SyncMessage;
#[cfg(test)]
use crate::wire::WireFrame;
use crate::wire::channel_credit::SharedChannelCredits;
use crate::wire::channels::{AUXILIARY_CHANNEL, ChannelClass};
use crate::wire::stream_backend::OrderedChannelBackend;
use crate::wire::{
    TransportError, WireChannelEnvelope, WireError, WireInboundContext,
    encode_sync_message_for_features,
};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

/// Connection-local independently driven auxiliary endpoint.
pub type SharedAuxiliaryEndpoint = Arc<Mutex<AuxiliaryChannelEndpoint>>;

pub(super) struct ChannelEndpoint {
    routing: super::routed_messages::RoutedMessages,
    raw: OrderedChannelBackend,
    context: WireInboundContext,
    last_wire_error: Option<WireError>,
}
impl ChannelEndpoint {
    pub(super) fn new(context: WireInboundContext) -> Result<Self, String> {
        Ok(Self {
            routing: Default::default(),
            raw: OrderedChannelBackend::new(context.clone())?,
            context,
            last_wire_error: None,
        })
    }
    pub(super) fn set_trusted_encoder(&mut self, trusted: bool) {
        self.context.set_trusted_encoder(trusted);
    }
    pub(super) fn last_wire_error(&self) -> Option<WireError> {
        self.last_wire_error.clone()
    }
    pub(super) fn incomplete_receive_timeout_ms(&self) -> Option<u64> {
        self.raw.incomplete_receive_timeout_ms()
    }
    pub(super) fn expire(&mut self) -> Result<(), String> {
        self.raw.expire()
    }
    #[cfg(any(test, feature = "testing"))]
    pub(super) fn set_incomplete_receive_timeout_for_test(&mut self, timeout_ms: u64) {
        self.raw.set_incomplete_receive_timeout_for_test(timeout_ms);
    }
    #[cfg(test)]
    pub(super) fn set_elapsed_for_test(&mut self, elapsed_ms: u64) {
        self.raw.set_elapsed_for_test(elapsed_ms);
    }
    pub(super) fn enqueue(
        &mut self,
        channel: u16,
        generation: u64,
        class: ChannelClass,
        message: &SyncMessage,
        barrier: bool,
    ) -> Result<(), TransportError> {
        let bytes = encode_sync_message_for_features(message, self.context.negotiated_features())
            .map_err(|error| {
            TransportError::Failed(format!("cannot encode channel message: {error:?}"))
        })?;
        if class == ChannelClass::Auxiliary {
            return self.raw.enqueue(channel, generation, class, bytes);
        }
        let bytes = self
            .routing
            .prepare(channel, barrier, bytes)
            .map_err(TransportError::Failed)?;
        self.raw.enqueue(channel, generation, class, bytes)?;
        self.routing.accepted(channel, barrier);
        Ok(())
    }
    pub(super) fn channel_credits(&self) -> SharedChannelCredits {
        self.raw.channel_credits()
    }
    pub(super) fn is_idle(&self, channel: u16) -> bool {
        self.raw.is_idle(channel)
    }
    pub(super) fn next_idle_generation(&self, channel: u16) -> Result<u64, String> {
        self.raw.next_idle_generation(channel)
    }
    pub(super) fn has_pending(&self) -> bool {
        self.raw.has_pending()
    }
    pub(super) fn peek_outbound(&mut self) -> Result<Option<Vec<u8>>, String> {
        self.raw.peek_outbound()
    }
    pub(super) fn accept_outbound(&mut self) -> Result<bool, String> {
        self.raw.accept_outbound()
    }
    pub(super) fn pop(&mut self) -> Option<super::ReceivedSyncMessage> {
        self.routing.pop()
    }
    pub(super) fn receive(
        &mut self,
        frame: WireChannelEnvelope,
        encoded_len: usize,
    ) -> Result<Option<super::ReceivedSyncMessage>, String> {
        if let Some(received) = self.raw.receive(frame, encoded_len)? {
            if received.class == ChannelClass::Auxiliary {
                let message = self
                    .context
                    .decode_semantic_payload(&received.payload)
                    .map_err(|e| {
                        self.last_wire_error = Some(e.clone());
                        format!("invalid auxiliary payload: {e:?}")
                    })?;
                if message_class(&message).0 != ChannelClass::Auxiliary {
                    return Err("message is not auxiliary".into());
                }
                return Ok(Some(super::ReceivedSyncMessage {
                    message,
                    lease: Some(received.lease),
                }));
            }
            self.routing
                .receive(
                    received.channel,
                    received.class,
                    received.payload,
                    received.lease,
                    &self.context,
                )
                .map_err(|e| {
                    self.last_wire_error = Some(e.clone());
                    format!("invalid routed payload: {e:?}")
                })?;
        }
        Ok(self.routing.pop())
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
        CommitUnit { .. } => (ChannelClass::Writes, false),
        Reserved30(retired) => match *retired {},
        RegisterShape { .. }
        | Subscribe(_)
        | Unsubscribe { .. }
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
    /// Remaining time until the earliest incomplete receive expires.
    pub fn incomplete_receive_timeout_ms(&self) -> Option<u64> {
        self.endpoint.incomplete_receive_timeout_ms()
    }
    /// Fail and release incomplete receive buffers once their deadline passes.
    pub fn expire_incomplete_receive(&mut self) -> Result<(), String> {
        self.endpoint.expire()
    }
    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn set_incomplete_receive_timeout_for_test(&mut self, timeout_ms: u64) {
        self.endpoint
            .set_incomplete_receive_timeout_for_test(timeout_ms);
    }
    /// Share canonical and auxiliary physical windows for this admitted link.
    pub fn set_channel_credits(&mut self, credits: SharedChannelCredits) {
        self.endpoint.raw.set_channel_credits(credits);
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
                .channel_credits()
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
            if let Ok(mut credits) = self.endpoint.channel_credits().lock() {
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
        self.endpoint
            .receive(frame, encoded_len)
            .map(|message| message.map(|message| message.message))
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

#[cfg(test)]
mod dependency_tests {
    use super::*;
    use crate::ids::AuthorSubject;
    use crate::wire::{
        FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireFrame, decode_frame,
    };
    use std::collections::BTreeMap;
    fn endpoint() -> ChannelEndpoint {
        ChannelEndpoint::new(WireInboundContext::new(
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD,
            None,
        ))
        .unwrap()
    }
    fn ordinary() -> SyncMessage {
        SyncMessage::CurrentRowsCancel {
            request_id: crate::protocol::PermissionAdviceRequestId([7; 16]),
        }
    }
    fn barrier() -> SyncMessage {
        SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        }
    }
    #[test]
    fn rejected_barrier_enqueue_preserves_epoch_and_watermarks() {
        let mut sender = endpoint();
        for _ in 0..crate::wire::channels::MAX_CHANNEL_QUEUED_MESSAGES {
            sender
                .enqueue(0, 0, ChannelClass::Control, &barrier(), true)
                .unwrap();
        }
        let before = sender.routing.prepare(0, true, vec![1]).unwrap();
        assert!(matches!(
            sender.enqueue(0, 0, ChannelClass::Control, &barrier(), true),
            Err(TransportError::Backpressure)
        ));
        assert_eq!(sender.routing.prepare(0, true, vec![1]).unwrap(), before);
    }

    // This synthetic carrier preserves each stream FIFO but deliberately
    // delivers control and later epochs before their predecessor streams.
    #[test]
    fn cross_stream_reordering_preserves_dependencies_without_ordering_independent_inputs() {
        let mut sender = endpoint();
        sender
            .enqueue(1, 0, ChannelClass::Requests, &ordinary(), false)
            .unwrap();
        sender
            .enqueue(3, 0, ChannelClass::Requests, &ordinary(), false)
            .unwrap();
        sender
            .enqueue(0, 0, ChannelClass::Control, &barrier(), true)
            .unwrap();
        sender
            .enqueue(4, 0, ChannelClass::Requests, &ordinary(), false)
            .unwrap();
        let mut streams = BTreeMap::new();
        while let Some(bytes) = sender.peek_outbound().unwrap() {
            let WireFrame::Channel(frame) = decode_frame(&bytes).unwrap() else {
                panic!("channel")
            };
            assert!(frame.extent.first && frame.extent.last);
            streams.insert(frame.extent.channel, (frame, bytes.len()));
            sender.accept_outbound().unwrap();
        }
        let mut receiver = endpoint();
        for slot in [0, 4] {
            let (frame, len) = streams.remove(&slot).unwrap();
            assert!(receiver.receive(frame, len).unwrap().is_none());
        }
        // Stream3 is independent of stream1 and must not wait for it.
        let (frame, len) = streams.remove(&3).unwrap();
        assert!(matches!(
            receiver.receive(frame, len).unwrap().unwrap().message,
            SyncMessage::CurrentRowsCancel { .. }
        ));
        assert!(receiver.pop().is_none());
        let (frame, len) = streams.remove(&1).unwrap();
        assert!(matches!(
            receiver.receive(frame, len).unwrap().unwrap().message,
            SyncMessage::CurrentRowsCancel { .. }
        ));
        assert!(matches!(
            receiver.pop().unwrap().message,
            SyncMessage::SessionClaims { .. }
        ));
        assert!(matches!(
            receiver.pop().unwrap().message,
            SyncMessage::CurrentRowsCancel { .. }
        ));
        assert!(receiver.pop().is_none());
    }
}
