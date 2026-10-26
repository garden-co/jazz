//! Public native carrier contract. These byte-level tests deliberately isolate
//! transport framing and backpressure from the separately tested DB lifecycle.
use jazz::db::{SyncMessage, TransportError};
use jazz::wire::{WireError, WireErrorCode, WireFrame, WireRetry, WireTransport};
use jazz_native_relay::{NativeRelayTransport, SendOutcome};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

#[derive(Default)]
struct State {
    full: bool,
    inbound: VecDeque<Vec<u8>>,
    outbound: Vec<Vec<u8>>,
}
struct Wire(Arc<Mutex<State>>);
impl WireTransport for Wire {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let mut state = self.0.lock().unwrap();
        if state.full {
            return Err(TransportError::Backpressure);
        }
        state.outbound.push(frame);
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.0.lock().unwrap().inbound.pop_front()
    }
}
fn pair() -> (NativeRelayTransport<Wire>, Arc<Mutex<State>>) {
    let state = Arc::new(Mutex::new(State::default()));
    (NativeRelayTransport::current(Wire(state.clone())), state)
}
fn frozen() -> Vec<u8> {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_native_relay_wire_v2.json")).unwrap();
    let s = fixture["unsubscribe_128"].as_str().unwrap();
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
        .collect()
}
/// Alice's carrier uses the frozen SQLite semantic frame and retains her exact
/// message while Bob's transport queue is full; retry emits it exactly once.
#[test]
fn sqlite_carrier_has_frozen_bytes_and_lossless_backpressure() {
    let (mut carrier, state) = pair();
    state.lock().unwrap().full = true;
    let SendOutcome::Rejected(message) = carrier
        .try_send(SyncMessage::Unsubscribe { id: 128 })
        .unwrap()
    else {
        panic!("full")
    };
    assert!(state.lock().unwrap().outbound.is_empty());
    state.lock().unwrap().full = false;
    assert!(matches!(
        carrier.try_send(*message).unwrap(),
        SendOutcome::Accepted
    ));
    assert_eq!(state.lock().unwrap().outbound, vec![frozen()]);
    state.lock().unwrap().inbound.push_back(frozen());
    assert!(matches!(
        carrier.try_recv(),
        Some(SyncMessage::Unsubscribe { id: 128 })
    ));
}
/// Mallory's malformed or oversized frame permanently fails that carrier; it
/// cannot silently skip the bad bytes and apply a later valid frame from Alice.
#[test]
fn sqlite_carrier_rejects_bad_frames_without_resuming_the_stream() {
    let mut trailing = frozen();
    trailing.push(0);
    for invalid in [
        vec![],
        vec![255],
        vec![1, 128, 1], // Historical SQLite v1 has no admitted v2 envelope.
        trailing,
        vec![0; jazz::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES + 1],
    ] {
        let (mut carrier, state) = pair();
        state.lock().unwrap().inbound.extend([invalid, frozen()]);
        assert!(carrier.try_recv().is_none());
        assert!(carrier.try_recv().is_none());
        assert!(matches!(
            carrier.try_send(SyncMessage::Unsubscribe { id: 1 }),
            Err(TransportError::Failed(_))
        ));
        assert!(state.lock().unwrap().outbound.is_empty());
    }
}
/// Bob's authority can refuse a connection with the existing control-error
/// frame; Alice retains its retry classification while stopping semantic input.
#[test]
fn sqlite_carrier_preserves_remote_retry_errors() {
    for retry in [WireRetry::Never, WireRetry::Later] {
        let (mut carrier, state) = pair();
        let error = WireError::new(WireErrorCode::Backpressure, retry, "authority queue full");
        state
            .lock()
            .unwrap()
            .inbound
            .push_back(jazz::wire::encode_frame(&WireFrame::Error(error.clone())).unwrap());
        assert!(carrier.try_recv().is_none());
        assert_eq!(carrier.remote_wire_error(), Some(&error));
        assert!(matches!(
            carrier.try_send(SyncMessage::Unsubscribe { id: 1 }),
            Err(TransportError::Failed(_))
        ));
    }
}
