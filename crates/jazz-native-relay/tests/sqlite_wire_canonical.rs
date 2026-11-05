//! Canonical semantic bytes are a carrier boundary, independently of row policy.
use jazz::db::{SyncMessage, TransportError};
use jazz::wire::WireTransport;
use jazz_native_relay::NativeRelayTransport;
use std::collections::VecDeque;

struct Frames(VecDeque<Vec<u8>>);
impl WireTransport for Frames {
    fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.0.pop_front()
    }
}

/// Mallory's alternate varint spelling must terminate the carrier rather than
/// become Alice's valid unsubscribe or allow a later frame to resume the stream.
#[test]
fn overlong_sqlite_varints_fail_the_carrier() {
    // Independent v3 header and canonical Unsubscribe(128) field bytes.
    let valid = vec![b'J', b'S', b'Q', b'L', 3, 1, 128, 1];
    let mut alice = NativeRelayTransport::current(Frames(VecDeque::from([valid.clone()])));
    assert!(matches!(
        alice.try_recv(),
        Some(SyncMessage::Unsubscribe { id: 128 })
    ));
    for malformed in [
        vec![b'J', b'S', b'Q', b'L', 3, 129, 0, 128, 1],
        vec![b'J', b'S', b'Q', b'L', 3, 1, 128, 129, 0],
        vec![b'J', b'S', b'Q', b'L', 2, 1, 128, 1], // Historical v2.
        vec![1, 128, 1],                            // Historical v1 is refused too.
    ] {
        let mut mallory =
            NativeRelayTransport::current(Frames(VecDeque::from([malformed, valid.clone()])));
        assert!(mallory.try_recv().is_none());
        assert!(mallory.try_recv().is_none());
        assert!(matches!(
            mallory.try_send(SyncMessage::Unsubscribe { id: 128 }),
            Err(TransportError::Failed(_))
        ));
    }
}
