//! Retry one atomic publication without duplicating accepted frame prefixes.
use jazz::db::{
    FrameJoiner, Image, SyncCell, SyncMessage, Transport, WireTransportAdapter, encode_frames,
};
use jazz::wire::{TransportError, WireTransport};
use std::{cell::RefCell, collections::VecDeque, rc::Rc};

#[derive(Default)]
struct State {
    allowance: usize,
    sent: Vec<Vec<u8>>,
    incoming: VecDeque<Vec<u8>>,
    fail: bool,
    calls: usize,
}
struct Gated(Rc<RefCell<State>>);
impl WireTransport for Gated {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        let mut state = self.0.borrow_mut();
        state.calls += 1;
        if state.fail {
            return Err(TransportError::Failed("bob disconnected".into()));
        }
        if state.allowance == 0 {
            return Err(TransportError::Backpressure);
        }
        state.allowance -= 1;
        state.sent.push(frame);
        Ok(())
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.0.borrow_mut().incoming.pop_front()
    }
}
fn setup() -> (WireTransportAdapter<Gated>, Rc<RefCell<State>>) {
    let state = Rc::new(RefCell::new(State::default()));
    (WireTransportAdapter::current(Gated(state.clone())), state)
}
fn publication() -> SyncMessage {
    SyncMessage::Rows {
        images: (1..=4)
            .map(|i| Image {
                table: "notes".into(),
                id: [i; 16],
                key: vec![],
                seq: 1,
                deleted: false,
                content: true,
                cells: vec![("body".into(), SyncCell::Blob(vec![i; 1_048_576]))],
            })
            .collect(),
    }
}

/// Alice's send is paused after each accepted frame. Bob sees each frame once,
/// in order, and receives one complete publication only after the terminal frame.
/// The gated byte transport is needed to pause inside the public adapter's send.
#[test]
fn partial_send_resumes_at_the_first_unsent_frame() {
    let message = publication();
    let expected = encode_frames(message.clone()).unwrap();
    assert!(expected.len() > 1);
    let (mut alice, state) = setup();
    let mut bob = FrameJoiner::default();
    for i in 0..expected.len() {
        state.borrow_mut().allowance = 1;
        let result = alice.send(message.clone());
        assert_eq!(state.borrow().sent.len(), i + 1);
        assert!(
            state.borrow().sent[i] == expected[i],
            "frame {i} was duplicated or changed"
        );
        let received = bob.push(&state.borrow().sent[i]).unwrap();
        if i + 1 == expected.len() {
            result.unwrap();
            match received {
                Some(SyncMessage::Rows { images }) => assert_eq!(images.len(), 4),
                other => panic!("{other:?}"),
            }
        } else {
            assert_eq!(result, Err(TransportError::Backpressure));
            assert!(received.is_none());
        }
    }
    assert_eq!(state.borrow().sent, expected);
}

/// While Alice's publication is partially sent, Bob's unrelated message cannot
/// interleave with it. Alice's exact retry finishes before Bob can send normally.
#[test]
fn different_message_cannot_interleave_with_pending_publication() {
    let message = publication();
    let expected = encode_frames(message.clone()).unwrap();
    let (mut alice, state) = setup();
    state.borrow_mut().allowance = 1;
    assert_eq!(
        alice.send(message.clone()),
        Err(TransportError::Backpressure)
    );
    state.borrow_mut().allowance = usize::MAX;
    let other = SyncMessage::Unsubscribe { id: 128 };
    assert_eq!(alice.send(other.clone()), Err(TransportError::Backpressure));
    assert_eq!(state.borrow().sent.len(), 1);
    alice.send(message).unwrap();
    assert_eq!(state.borrow().sent, expected);
    alice.send(other.clone()).unwrap();
    assert_eq!(
        state.borrow().sent.last().unwrap(),
        &encode_frames(other).unwrap()[0]
    );
}

/// Bob's transport failure after accepting a prefix is terminal for Alice's
/// adapter and is reported once. Reusing the adapter cannot send another prefix.
#[test]
fn failed_partial_send_closes_the_adapter_and_reports_once() {
    let message = publication();
    let (mut alice, state) = setup();
    state.borrow_mut().allowance = 1;
    assert_eq!(
        alice.send(message.clone()),
        Err(TransportError::Backpressure)
    );
    state.borrow_mut().fail = true;
    let failure = TransportError::Failed("bob disconnected".into());
    assert_eq!(alice.send(message.clone()), Err(failure.clone()));
    assert_eq!(alice.take_failure(), Some(failure));
    assert_eq!(alice.take_failure(), None);
    let calls = state.borrow().calls;
    state.borrow_mut().fail = false;
    state.borrow_mut().allowance = usize::MAX;
    assert!(matches!(
        alice.send(message),
        Err(TransportError::Failed(_))
    ));
    assert_eq!(state.borrow().calls, calls);
    assert_eq!(state.borrow().sent.len(), 1);
}

/// Mallory's malformed inbound frame closes Alice's adapter in both directions;
/// Bob cannot continue publishing over a connection whose decoder has failed.
#[test]
fn receive_failure_also_refuses_future_sends() {
    let (mut alice, state) = setup();
    state.borrow_mut().incoming.push_back(vec![255]);
    assert!(alice.try_recv().is_none());
    assert!(matches!(
        alice.take_failure(),
        Some(TransportError::Failed(_))
    ));
    assert_eq!(alice.take_failure(), None);
    state.borrow_mut().allowance = 1;
    assert!(matches!(
        alice.send(SyncMessage::Unsubscribe { id: 1 }),
        Err(TransportError::Failed(_))
    ));
    assert!(state.borrow().sent.is_empty());
    assert_eq!(state.borrow().calls, 0);
}

/// Alice's send refused before its first frame remains retryable. Once Bob has
/// capacity it sends once; after completion an identical new message is allowed.
#[test]
fn zero_progress_retry_and_subsequent_identical_send_are_distinct() {
    let (mut alice, state) = setup();
    let message = SyncMessage::Unsubscribe { id: 128 };
    assert_eq!(
        alice.send(message.clone()),
        Err(TransportError::Backpressure)
    );
    assert!(state.borrow().sent.is_empty());
    state.borrow_mut().allowance = 2;
    alice.send(message.clone()).unwrap();
    alice.send(message.clone()).unwrap();
    let frame = encode_frames(message).unwrap().pop().unwrap();
    assert_eq!(state.borrow().sent, vec![frame.clone(), frame]);
}

/// Bob refuses Alice's first frame without accepting any bytes. Alice can then
/// send a different message before retrying the original publication, as the
/// client's outbox may reorder messages between ticks. The public wire adapter
/// exposes the backpressure boundary without relying on host scheduling.
#[test]
fn zero_progress_allows_another_message_before_retry() {
    for message in [SyncMessage::Unsubscribe { id: 128 }, publication()] {
        let (mut alice, state) = setup();
        assert_eq!(
            alice.send(message.clone()),
            Err(TransportError::Backpressure)
        );
        assert!(state.borrow().sent.is_empty());
        state.borrow_mut().allowance = usize::MAX;
        let other = SyncMessage::Unsubscribe { id: 256 };
        alice.send(other.clone()).unwrap();
        alice.send(message.clone()).unwrap();
        let mut expected = encode_frames(other).unwrap();
        expected.extend(encode_frames(message).unwrap());
        assert_eq!(state.borrow().sent, expected);
    }
}
