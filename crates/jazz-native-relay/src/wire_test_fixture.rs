use super::*;

pub(super) struct AuthorityWire {
    wire: NativeRelayWire,
    pending: VecDeque<Vec<u8>>,
}
impl AuthorityWire {
    pub(super) fn new(wire: NativeRelayWire) -> Self {
        Self {
            wire,
            pending: VecDeque::new(),
        }
    }
}
impl WireTransport for AuthorityWire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.wire
            .push_inbound_encoded(&frame)
            .map_err(|error| match error {
                RelayError::QueueCapacityExceeded { .. } => TransportError::Backpressure,
                error => TransportError::Failed(error.to_string()),
            })
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        if self.pending.is_empty() {
            self.pending.extend(
                self.wire
                    .take_outbound_encoded()
                    .expect("live authority wire"),
            );
        }
        self.pending.pop_front()
    }
}

// Tests place semantic frames directly into the native carrier to control the
// socket boundary. Admission still observes the production count/byte budget.
impl FrameQueue {
    pub(super) fn push(
        &mut self,
        message: SyncMessage,
        direction: &'static str,
    ) -> Result<(), RelayError> {
        let frame = encode_sync_message(&message).unwrap();
        if self.room().0 == 0 || frame.len() > self.room().1 {
            return Err(RelayError::QueueCapacityExceeded {
                direction,
                queued_messages: self.frames.len(),
            });
        }
        self.bytes += frame.len();
        self.frames.push_back(frame);
        Ok(())
    }
    pub(super) fn pop(&mut self) -> Option<SyncMessage> {
        let frame = self.frames.pop_front()?;
        self.bytes -= frame.len();
        Some(jazz::db::decode_message(&frame).unwrap())
    }
    pub(super) fn len(&self) -> usize {
        self.frames.len()
    }
}
impl NativeRelayWire {
    pub(super) fn push_inbound(&self, message: SyncMessage) -> Result<(), RelayError> {
        self.push_inbound_encoded(&encode_sync_message(&message).unwrap())
    }
}
