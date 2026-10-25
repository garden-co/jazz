//! Bounded native carrier for SQLite's existing semantic messages.
use super::*;

pub fn encode_sync_message(message: &SyncMessage) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(message)
}
pub fn decode_sync_message(bytes: &[u8]) -> Result<SyncMessage, postcard::Error> {
    jazz::postcard_exact::decode_postcard_exact(bytes)
}
#[derive(Debug)]
pub enum SendOutcome {
    Accepted,
    Rejected(Box<SyncMessage>),
}
/// The native connector has already admitted Hello; subsequent frames carry
/// SQLite messages. A rejected send retains the original message for retry.
pub struct NativeRelayTransport<W> {
    wire: W,
    failure: Option<String>,
    remote_error: Option<jazz::wire::WireError>,
}
impl<W: WireTransport> NativeRelayTransport<W> {
    pub fn current(wire: W) -> Self {
        Self {
            wire,
            failure: None,
            remote_error: None,
        }
    }
    pub fn remote_wire_error(&self) -> Option<&jazz::wire::WireError> {
        self.remote_error.as_ref()
    }
    pub fn try_send(&mut self, message: SyncMessage) -> Result<SendOutcome, TransportError> {
        if let Some(error) = &self.failure {
            return Err(TransportError::Failed(error.clone()));
        }
        let bytes =
            encode_sync_message(&message).map_err(|e| TransportError::Failed(e.to_string()))?;
        validate_logical_message_len(bytes.len()).map_err(TransportError::Failed)?;
        match self.wire.send_frame(bytes) {
            Ok(()) => Ok(SendOutcome::Accepted),
            Err(TransportError::Backpressure) => Ok(SendOutcome::Rejected(Box::new(message))),
            Err(error) => Err(error),
        }
    }
    pub fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.failure.is_some() {
            return None;
        }
        let bytes = self.wire.try_recv_frame()?;
        let decoded = validate_logical_message_len(bytes.len())
            .and_then(|()| decode_sync_message(&bytes).map_err(|e| e.to_string()));
        match decoded {
            Ok(message) => Some(message),
            Err(message) => {
                if let Ok(jazz::wire::WireFrame::Error(error)) = jazz::wire::decode_frame(&bytes) {
                    self.remote_error = Some(error);
                }
                self.failure = Some(message);
                None
            }
        }
    }
    pub(super) fn check(&self) -> Result<(), TransportError> {
        self.failure
            .as_ref()
            .map_or(Ok(()), |e| Err(TransportError::Failed(e.clone())))
    }
}
