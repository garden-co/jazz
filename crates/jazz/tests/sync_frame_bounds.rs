//! Wire transport limits apply across every frame of one split message.
//! Joe's bounds from #4367, carried onto transport parts: a part frame is
//! `0xFF` (more follow) or `0xFE` (the last) ahead of a slice of the
//! message's postcard bytes.
use jazz::db::{FrameJoiner, Image, SyncCell, SyncMessage, encode_frames};
use jazz::protocol_limits::{
    MAX_LOGICAL_MESSAGE_BYTES, MAX_WIRE_BATCH_FRAMES, MAX_WIRE_FRAME_BYTES,
};
const FRAME_BUDGET: usize = MAX_WIRE_FRAME_BYTES - 64;
const PART: u8 = 0xFF;
const LAST_PART: u8 = 0xFE;
fn image(actor: u8, bytes: usize) -> Image {
    Image {
        table: "notes".into(),
        id: [actor; 16],
        key: vec![],
        seq: 1,
        deleted: false,
        content: true,
        cells: vec![("body".into(), SyncCell::Blob(vec![actor; bytes]))],
    }
}
fn rows_message(images: Vec<Image>) -> SyncMessage {
    SyncMessage::Rows {
        images,
        tx_ids: vec![],
    }
}
fn raw(message: &SyncMessage) -> Vec<u8> {
    // The untrusted peer can bypass our encoder. Raw postcard is intentional
    // here so malformed or oversized inputs reach the public admission boundary.
    postcard::to_allocvec(message).unwrap()
}
fn part(marker: u8, bytes: &[u8]) -> Vec<u8> {
    [&[marker], bytes].concat()
}
fn rows(message: Option<SyncMessage>) -> Vec<Image> {
    match message {
        Some(SyncMessage::Rows { images, .. }) => images,
        other => panic!("expected Rows: {other:?}"),
    }
}
/// Bob's next whole message after a refusal is his own, with nothing of the
/// refused one before it.
fn bob_is_independent(joiner: &mut FrameJoiner) {
    let bob = image(3, 0);
    assert_eq!(
        rows(joiner.push(&raw(&rows_message(vec![bob.clone()]))).unwrap()),
        vec![bob]
    );
}

/// Mallory's well-formed but oversized frame is refused before Bob can publish
/// it, and the failed message must not leave Alice's prior part buffered.
#[test]
fn oversized_frame_is_rejected_and_discards_partial_parts() {
    let mut joiner = FrameJoiner::default();
    let alice = raw(&rows_message(vec![image(1, 0)]));
    assert!(joiner.push(&part(PART, &alice[..4])).unwrap().is_none());
    let oversized = raw(&rows_message(vec![image(2, MAX_WIRE_FRAME_BYTES)]));
    assert!(joiner.push(&oversized).is_err());
    bob_is_independent(&mut joiner);
}

/// Mallory's malformed frame aborts Alice's partial message. Bob's next
/// complete message must never inherit bytes retained from it.
#[test]
fn malformed_frame_discards_partial_parts() {
    let mut joiner = FrameJoiner::default();
    let alice = raw(&rows_message(vec![image(1, 0)]));
    assert!(joiner.push(&part(PART, &alice[..4])).unwrap().is_none());
    assert!(joiner.push(&part(LAST_PART, &[0xff; 3])).is_err());
    bob_is_independent(&mut joiner);
}

/// A split message may use exactly the frame-count budget, including its
/// last part. Mallory cannot retain an unbounded tiny-part sequence;
/// refusal releases the buffered state for Bob's next message.
#[test]
fn reassembly_counts_the_last_part_and_resets_after_refusal() {
    let bytes = raw(&SyncMessage::Unsubscribe { id: 7 });
    let mut joiner = FrameJoiner::default();
    for _ in 0..MAX_WIRE_BATCH_FRAMES - 1 {
        assert!(joiner.push(&part(PART, &[])).unwrap().is_none());
    }
    assert!(matches!(
        joiner.push(&part(LAST_PART, &bytes)).unwrap(),
        Some(SyncMessage::Unsubscribe { id: 7 })
    ));
    for _ in 0..MAX_WIRE_BATCH_FRAMES {
        assert!(joiner.push(&part(PART, &[])).unwrap().is_none());
    }
    assert!(joiner.push(&part(LAST_PART, &bytes)).is_err());
    bob_is_independent(&mut joiner);
}

/// Mallory cannot accumulate more than one joined-message budget through
/// individually valid parts, at the default ceiling or a session's own.
#[test]
fn reassembly_bounds_bytes_across_parts() {
    let slice = vec![0u8; FRAME_BUDGET - 1];
    let mut joiner = FrameJoiner::default();
    let count = MAX_LOGICAL_MESSAGE_BYTES / slice.len();
    assert!(count < MAX_WIRE_BATCH_FRAMES);
    for _ in 0..count {
        assert!(joiner.push(&part(PART, &slice)).unwrap().is_none());
    }
    assert!(joiner.push(&part(PART, &slice)).is_err());
    bob_is_independent(&mut joiner);

    let mut joiner = FrameJoiner::with_ceiling(2 * slice.len());
    for _ in 0..2 {
        assert!(joiner.push(&part(PART, &slice)).unwrap().is_none());
    }
    assert!(joiner.push(&part(LAST_PART, &[0])).is_err());
    bob_is_independent(&mut joiner);
}

/// Alice's ordinary large message still reaches Bob whole and in row
/// order after splitting; no part exposes a prefix.
#[test]
fn valid_split_message_retains_order_and_atomic_delivery() {
    let expected: Vec<_> = (1..=4)
        .map(|i| image(i, MAX_WIRE_FRAME_BYTES / 2))
        .collect();
    let frames = encode_frames(rows_message(expected.clone())).unwrap();
    assert!(frames.len() > 1);
    let mut joiner = FrameJoiner::default();
    for (index, frame) in frames.iter().enumerate() {
        assert!(frame.len() <= FRAME_BUDGET);
        let result = joiner.push(frame).unwrap();
        if index + 1 == frames.len() {
            assert_eq!(rows(result), expected);
        } else {
            assert!(result.is_none());
        }
    }
}

/// A single row too large for one frame travels in parts too: splitting is
/// below the message layer, so no message is indivisible short of the
/// joined-message ceiling.
#[test]
fn one_row_larger_than_a_frame_travels_in_parts() {
    let expected = vec![image(1, MAX_WIRE_FRAME_BYTES)];
    let frames = encode_frames(rows_message(expected.clone())).unwrap();
    assert_eq!(frames.len(), 2);
    let mut joiner = FrameJoiner::default();
    assert!(joiner.push(&frames[0]).unwrap().is_none());
    assert_eq!(rows(joiner.push(&frames[1]).unwrap()), expected);
}

/// Alice's partial message followed by Mallory's whole message is an
/// error. Bob's subsequent complete message cannot inherit the old prefix.
#[test]
fn whole_message_between_parts_ends_and_clears_partial_message() {
    let mut joiner = FrameJoiner::default();
    let alice = raw(&rows_message(vec![image(1, 0)]));
    joiner.push(&part(PART, &alice[..4])).unwrap();
    assert!(
        joiner
            .push(&raw(&SyncMessage::Unsubscribe { id: 2 }))
            .is_err()
    );
    bob_is_independent(&mut joiner);
}
