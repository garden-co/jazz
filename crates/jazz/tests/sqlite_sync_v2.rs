//! Byte-level tests are needed here: row equality cannot detect a semantic tag
//! collision, noncanonical varint, or the absence of an encoding-version fence.
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, FileOptions, FrameJoiner, SyncMessage, block_on,
    decode_message, encode_message,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::SchemaBuilder;

fn bytes(hex: &str) -> Vec<u8> {
    hex::decode(hex).unwrap()
}

/// Alice's current peer reads independently specified v3 frames and writes
/// exactly the same bytes, including per-operation timestamps and writer IDs.
#[test]
fn sync_v2_has_independent_byte_fixtures() {
    let fixtures: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite-sync-v3/frames.json")).unwrap();
    assert_eq!(fixtures["encoding"], "jazz.sqlite.sync.v3");
    for case in fixtures["cases"].as_array().unwrap() {
        let frame = bytes(case["frame_hex"].as_str().unwrap());
        let expected: SyncMessage = serde_json::from_value(case["message"].clone()).unwrap();
        let decoded = decode_message(&frame).unwrap();
        assert_eq!(serde_json::to_value(decoded).unwrap(), case["message"]);
        assert_eq!(
            encode_message(&expected).unwrap(),
            frame,
            "{}",
            case["name"]
        );
    }
}

/// Mallory cannot make Alice interpret a v1 tag, a future version, an overlong
/// integer or trailing bytes as a current semantic frame.
#[test]
fn sync_v2_refuses_foreign_and_noncanonical_bytes() {
    for malformed in [
        "018001",             // Old SQLite Unsubscribe, without an envelope.
        "4a53514c01018001",   // Explicit older codec.
        "4a53514c04018001",   // Future codec.
        "4a53514c0301808100", // Noncanonical encoding of 128.
        "4a53514c0301800100", // Valid message with a suffix.
    ] {
        assert!(decode_message(&bytes(malformed)).is_err(), "{malformed}");
    }
}

/// Mallory's unfinished row publication cannot retain unbounded tiny frames,
/// and none of its partial rows is delivered to Alice's database.
#[test]
fn sync_v2_bounds_fragment_count_and_rejects_interleaving() {
    let part = bytes("4a53514c031400");
    let mut joiner = FrameJoiner::default();
    for _ in 0..jazz::protocol_limits::MAX_WIRE_BATCH_FRAMES {
        assert!(joiner.push(&part).unwrap().is_none());
    }
    assert!(joiner.push(&part).is_err());
    let mut joiner = FrameJoiner::default();
    assert!(joiner.push(&part).unwrap().is_none());
    assert!(joiner.push(&bytes("4a53514c030101")).is_err());
    let oversized = vec![0; jazz::protocol_limits::MAX_WIRE_FRAME_BYTES + 1];
    assert!(decode_message(&oversized).is_err());
}

/// Alice's older store is refused before journal configuration or schema
/// writes. The raw fixture deliberately isolates admission from old engine
/// behaviour, and byte equality proves the refusal did not mutate the file.
#[test]
fn sqlite_v2_refuses_v1_without_mutating_the_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.sqlite");
    {
        let old = rusqlite::Connection::open(&path).unwrap();
        old.execute_batch(
            "CREATE TABLE _jazz_format(version INTEGER NOT NULL); \
             INSERT INTO _jazz_format VALUES (1);",
        )
        .unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    let config = DbConfig::new(
        JazzSchema::new(&SchemaBuilder::new().build()).unwrap(),
        DbIdentity {
            node: NodeUuid::from_bytes([1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    let error = match block_on(Db::open_at(config, &path, FileOptions::default())) {
        Ok(_) => panic!("old storage was admitted"),
        Err(error) => error,
    };
    assert_eq!(error.code, ErrorCode::UnsupportedStorageCodecs);
    assert_eq!(std::fs::read(path).unwrap(), before);
}

/// Alice's bounded wire accepts only one fragment per turn. Bob receives one
/// whole publication with no duplicated prefix when Alice retries. A raw wire
/// is necessary to place backpressure between frames of the same message.
#[test]
fn fragmented_wire_retry_resumes_without_repeating_or_interleaving() {
    use jazz::db::{Image, SyncCell, Transport, WireTransportAdapter};
    use jazz::wire::{TransportError, WireTransport};
    use std::{
        cell::{Cell, RefCell},
        rc::Rc,
    };
    struct Bounded {
        frames: Rc<RefCell<Vec<Vec<u8>>>>,
        capacity: Rc<Cell<usize>>,
    }
    impl WireTransport for Bounded {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            if self.capacity.get() == 0 {
                return Err(TransportError::Backpressure);
            }
            self.capacity.set(self.capacity.get() - 1);
            self.frames.borrow_mut().push(frame);
            Ok(())
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }
    let images = (0..3)
        .map(|n| Image {
            table: "notes".into(),
            id: [n; 16],
            key: vec![],
            seq: 1,
            deleted: false,
            content: true,
            cells: vec![("text".into(), SyncCell::Text(vec![b'a' + n; 1_100_000]))],
        })
        .collect::<Vec<_>>();
    let message = SyncMessage::Rows {
        images: images.clone(),
    };
    let frames = Rc::new(RefCell::new(Vec::new()));
    let capacity = Rc::new(Cell::new(1));
    let mut sender = WireTransportAdapter::current(Bounded {
        frames: frames.clone(),
        capacity: capacity.clone(),
    });
    assert_eq!(
        sender.send(message.clone()),
        Err(TransportError::Backpressure)
    );
    assert_eq!(frames.borrow().len(), 1);
    capacity.set(1);
    assert_eq!(
        sender.send(SyncMessage::Unsubscribe { id: 7 }),
        Err(TransportError::Backpressure)
    );
    assert_eq!(
        frames.borrow().len(),
        1,
        "another message must not consume the pending retry"
    );
    assert_eq!(
        sender.send(message.clone()),
        Err(TransportError::Backpressure)
    );
    assert_eq!(frames.borrow().len(), 2);
    capacity.set(1);
    sender.send(message).unwrap();
    assert_eq!(frames.borrow().len(), 3);
    let mut joiner = FrameJoiner::default();
    for frame in &frames.borrow()[..2] {
        assert!(joiner.push(frame).unwrap().is_none());
    }
    let Some(SyncMessage::Rows { images: delivered }) = joiner.push(&frames.borrow()[2]).unwrap()
    else {
        panic!("whole row publication")
    };
    assert_eq!(delivered, images);
    capacity.set(1);
    sender.send(SyncMessage::Unsubscribe { id: 7 }).unwrap();
    assert!(matches!(
        joiner.push(&frames.borrow()[3]).unwrap(),
        Some(SyncMessage::Unsubscribe { id: 7 })
    ));
}
