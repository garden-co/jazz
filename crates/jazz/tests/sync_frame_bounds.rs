//! Wire transport limits apply across every frame of one atomic publication.
use jazz::db::{FrameJoiner, Image, SyncCell, SyncMessage, encode_frames};
use jazz::protocol_limits::{
    MAX_LOGICAL_MESSAGE_BYTES, MAX_WIRE_BATCH_FRAMES, MAX_WIRE_FRAME_BYTES,
};
const FRAME_BUDGET: usize = MAX_WIRE_FRAME_BYTES - 64;
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
fn raw(message: &SyncMessage) -> Vec<u8> {
    // The untrusted peer can bypass our encoder. Raw postcard is intentional
    // here so malformed or oversized inputs reach the public admission boundary.
    [
        jazz::db::SYNC_ENCODING_PREFIX,
        postcard::to_allocvec(message).unwrap().as_slice(),
    ]
    .concat()
}
fn rows(message: Option<SyncMessage>) -> Vec<Image> {
    match message {
        Some(SyncMessage::Rows { images }) => images,
        other => panic!("expected Rows: {other:?}"),
    }
}

/// Alice cannot send an indivisible oversized row or ordinary message to Bob.
/// Direct frame APIs exercise the carrier boundary without storing fake rows.
#[test]
fn encoder_refuses_indivisible_oversized_messages() {
    for message in [
        SyncMessage::Rows {
            images: vec![image(1, MAX_WIRE_FRAME_BYTES)],
        },
        SyncMessage::Evict {
            table: "x".repeat(MAX_WIRE_FRAME_BYTES),
            id: [2; 16],
            key: vec![],
        },
    ] {
        assert!(encode_frames(message).is_err());
    }
}

/// Mallory's well-formed but oversized frame is refused before Bob can publish
/// it, and the failed message must not leave Alice's prior fragment buffered.
#[test]
fn oversized_frame_is_rejected_and_discards_partial_rows() {
    let mut joiner = FrameJoiner::default();
    assert!(
        joiner
            .push(&raw(&SyncMessage::RowsPart {
                images: vec![image(1, 0)]
            }))
            .unwrap()
            .is_none()
    );
    let oversized = raw(&SyncMessage::Rows {
        images: vec![image(2, MAX_WIRE_FRAME_BYTES)],
    });
    assert!(joiner.push(&oversized).is_err());
    let bob = image(3, 0);
    assert_eq!(
        rows(
            joiner
                .push(&raw(&SyncMessage::Rows {
                    images: vec![bob.clone()]
                }))
                .unwrap()
        ),
        vec![bob]
    );
}

/// Mallory's malformed frame aborts Alice's partial publication. Bob's next
/// complete message must never inherit rows retained from that failed message.
#[test]
fn malformed_frame_discards_partial_rows() {
    let mut joiner = FrameJoiner::default();
    assert!(
        joiner
            .push(&raw(&SyncMessage::RowsPart {
                images: vec![image(1, 0)]
            }))
            .unwrap()
            .is_none()
    );
    assert!(joiner.push(&[255]).is_err());
    let bob = image(2, 0);
    assert_eq!(
        rows(
            joiner
                .push(&raw(&SyncMessage::Rows {
                    images: vec![bob.clone()]
                }))
                .unwrap()
        ),
        vec![bob]
    );
}

/// Alice's logical publication may use exactly the frame-count budget,
/// including its terminal frame. Mallory cannot retain an unbounded tiny-frame
/// sequence; rejection releases the buffered state for Bob's next publication.
#[test]
fn reassembly_counts_the_terminal_frame_and_resets_after_refusal() {
    let part = raw(&SyncMessage::RowsPart { images: vec![] });
    let end = raw(&SyncMessage::Rows { images: vec![] });
    let mut joiner = FrameJoiner::default();
    for _ in 0..MAX_WIRE_BATCH_FRAMES - 1 {
        assert!(joiner.push(&part).unwrap().is_none());
    }
    assert!(rows(joiner.push(&end).unwrap()).is_empty());
    for _ in 0..MAX_WIRE_BATCH_FRAMES {
        assert!(joiner.push(&part).unwrap().is_none());
    }
    assert!(joiner.push(&end).is_err());
    assert!(rows(joiner.push(&end).unwrap()).is_empty());
}

/// Mallory cannot accumulate more than one logical-message byte budget through
/// individually valid frames. These direct carrier inputs exercise the real
/// limit (no runtime test hook); Alice sees no prefix and Bob's next message is
/// independent after refusal.
#[test]
fn reassembly_bounds_bytes_across_frames() {
    let part = raw(&SyncMessage::RowsPart {
        images: vec![image(1, MAX_WIRE_FRAME_BYTES / 2)],
    });
    assert!(part.len() < FRAME_BUDGET);
    let mut joiner = FrameJoiner::default();
    let count = MAX_LOGICAL_MESSAGE_BYTES / part.len();
    assert!(count < MAX_WIRE_BATCH_FRAMES);
    for _ in 0..count {
        assert!(joiner.push(&part).unwrap().is_none());
    }
    assert!(joiner.push(&part).is_err());
    let bob = image(2, 0);
    assert_eq!(
        rows(
            joiner
                .push(&raw(&SyncMessage::Rows {
                    images: vec![bob.clone()]
                }))
                .unwrap()
        ),
        vec![bob]
    );
}

/// Alice's ordinary large publication still reaches Bob atomically and in row
/// order after splitting; byte limits never expose an intermediate prefix.
#[test]
fn valid_split_publication_retains_order_and_atomic_delivery() {
    let expected: Vec<_> = (1..=4)
        .map(|i| image(i, MAX_WIRE_FRAME_BYTES / 2))
        .collect();
    let frames = encode_frames(SyncMessage::Rows {
        images: expected.clone(),
    })
    .unwrap();
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

/// Alice's partial publication followed by Mallory's unrelated message is an
/// error. Bob's subsequent complete publication cannot inherit the old prefix.
#[test]
fn unrelated_message_ends_and_clears_partial_publication() {
    let mut joiner = FrameJoiner::default();
    joiner
        .push(&raw(&SyncMessage::RowsPart {
            images: vec![image(1, 0)],
        }))
        .unwrap();
    assert!(
        joiner
            .push(&raw(&SyncMessage::Unsubscribe { id: 2 }))
            .is_err()
    );
    let bob = image(2, 0);
    assert_eq!(
        rows(
            joiner
                .push(&raw(&SyncMessage::Rows {
                    images: vec![bob.clone()]
                }))
                .unwrap()
        ),
        vec![bob]
    );
}
