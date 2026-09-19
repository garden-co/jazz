//! Framing, fragmentation, compression, and authenticated wire-session tests.

use super::*;

// These tests exercise physical FIFO/backpressure boundaries that a Db cannot
// expose directly. The actual adapter and persistent codecs remain in use.
fn receive_after_pumping<T: WireTransport, U: WireTransport>(
    sender: &mut WireTransportAdapter<T>,
    receiver: &mut WireTransportAdapter<U>,
) -> SyncMessage {
    for _ in 0..8192 {
        sender.poll_flush().unwrap();
        if let Some(message) = receiver.try_recv_result().unwrap() {
            return message;
        }
        assert!(
            sender.try_recv_result().unwrap().is_none(),
            "only consumption credits return"
        );
    }
    panic!("accepted channel message made no bounded progress");
}

struct RecordingChannelTransport {
    inner: ByteDuplexTransport,
    fail_on_call: Option<usize>,
    attempts: Rc<RefCell<Vec<Vec<u8>>>>,
}
impl WireTransport for RecordingChannelTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.attempts.borrow_mut().push(frame.clone());
        if self.fail_on_call == Some(self.attempts.borrow().len()) {
            return Err(TransportError::Backpressure);
        }
        self.inner.send_frame(frame)
    }
    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.inner.try_recv_frame()
    }
}

#[test]
fn logical_message_larger_than_frame_round_trips_in_channel_fifo() {
    let (left, right) = byte_duplex_raw();
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let mut sender = WireTransportAdapter::current(RecordingChannelTransport {
        inner: left,
        fail_on_call: None,
        attempts: Rc::clone(&attempts),
    });
    let mut receiver = WireTransportAdapter::current(right);
    let message = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0x71; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("fifo".repeat(MAX_WIRE_FRAME_BYTES / 4 + 200_000)),
        )]),
    };
    sender.send(message.clone()).unwrap();
    assert!(
        receiver.try_recv_result().unwrap().is_none(),
        "first extent is not a semantic message"
    );
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
    let frames = attempts.borrow();
    let channels: Vec<_> = frames
        .iter()
        .map(|bytes| {
            assert!(bytes.len() <= MAX_WIRE_FRAME_BYTES);
            let WireFrame::Channel(frame) = decode_frame(bytes).unwrap() else {
                panic!("mandatory channel frame")
            };
            frame.extent
        })
        .collect();
    assert!(channels.len() > 1);
    for (index, frame) in channels.iter().enumerate() {
        assert_eq!(frame.sequence, index as u64);
        assert_eq!(frame.first, index == 0);
        assert_eq!(frame.last, index + 1 == channels.len());
    }
    drop(frames);
    sender.send(message.clone()).unwrap();
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
    assert!(receiver.try_recv_result().unwrap().is_none());
}

#[test]
fn strict_bootstrap_receive_rejects_bad_physical_frame_before_later_valid_message() {
    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender
        .send(SyncMessage::SessionClaims {
            identity: AuthorSubject::SYSTEM,
            claims: BTreeMap::new(),
        })
        .expect("stage valid later message");
    staged.borrow_mut().push_front(vec![0xff]);

    let error = receiver
        .try_recv_strict()
        .expect_err("bootstrap must fail on the first malformed physical frame");
    assert_eq!(error.code, crate::wire::WireErrorCode::MalformedFrame);
    assert!(
        !staged.borrow().is_empty(),
        "later valid message must not erase the preceding bootstrap violation"
    );
}

#[test]
fn schema_lineage_publication_fragments_before_atomic_admission() {
    let base = schema();
    let authority = open_core(0x38, AuthorSubject::SYSTEM, &base);
    let large_default = Value::String("x".repeat(MAX_WIRE_FRAME_BYTES + 1024));
    let evolved_schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .column_with_default(
                    "large_default",
                    PublicColumnType::Text,
                    PublicValue::Text("x".repeat(MAX_WIRE_FRAME_BYTES + 1024)),
                ),
        ),
    );
    let evolved = crate::protocol::SchemaVersion::new(evolved_schema);
    let lens = crate::protocol::MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "large_default".to_owned(),
                default: large_default,
            }],
        }],
    )
    .expect("valid migration lens");
    let publication = authority
        .author_schema_lineage_publication(
            evolved.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    let message = SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq: 1,
        publication: Box::new(publication),
    };
    assert!(postcard::to_allocvec(&message).unwrap().len() > MAX_WIRE_FRAME_BYTES);

    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender.send(message.clone()).unwrap();
    let mut physical_frames = 0;
    let reassembled = (0..8192)
        .find_map(|_| {
            sender.poll_flush().unwrap();
            for frame in staged.borrow().iter() {
                assert!(frame.len() <= MAX_WIRE_FRAME_BYTES);
                assert!(matches!(
                    decode_frame(frame).unwrap(),
                    WireFrame::Channel(_)
                ));
                physical_frames += 1;
            }
            let next = receiver.try_recv_result().unwrap();
            assert!(
                !authority
                    .node()
                    .borrow()
                    .catalogue_schemas()
                    .contains_key(&evolved.id),
                "physical extents must not atomically admit a partial publication"
            );
            if next.is_some() {
                return next;
            }
            assert!(sender.try_recv_result().unwrap().is_none());
            None
        })
        .expect("bounded channel publication completes");
    assert!(physical_frames > 1);
    assert_eq!(reassembled, message);
    authority
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(reassembled)
        .unwrap();
    assert!(
        authority
            .node()
            .borrow()
            .catalogue_schemas()
            .contains_key(&evolved.id)
    );
}

#[test]
fn channel_reordering_and_duplicate_extents_fail_closed() {
    use crate::wire::channels::CHANNEL_CHUNK_BYTES;
    for duplicate in [false, true] {
        let (left, right) = byte_duplex_raw();
        let staged = Rc::clone(&right.inbound);
        let features = FEATURE_SYNC_MESSAGE_PAYLOAD;
        let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
        let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
        sender
            .send(SyncMessage::SessionClaims {
                identity: AuthorSubject::for_test_bytes([0x72; 16]),
                claims: BTreeMap::from([(
                    "large".to_owned(),
                    Value::String("q".repeat(2 * CHANNEL_CHUNK_BYTES)),
                )]),
            })
            .unwrap();
        sender.poll_flush().unwrap();
        let mut frames = staged.borrow_mut().drain(..).collect::<Vec<_>>();
        assert!(frames.len() > 1);
        if duplicate {
            frames.insert(1, frames[0].clone());
        } else {
            frames.swap(0, 1);
        }
        staged.borrow_mut().extend(frames);
        assert!(
            receiver.try_recv_result().is_err(),
            "stateful channel bytes must not be reordered or replayed"
        );
        assert!(
            receiver.try_recv_result().is_err(),
            "terminal state cannot skip ahead to later valid extents"
        );
    }
}

#[test]
fn fragment_admission_bounds_peer_state_and_rejects_conflicting_duplicates() {
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_MESSAGE_FRAGMENTATION;
    let fragment = |message_id, payload: u8| WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION,
        features,
        session: None,
        message_id,
        message_digest: [payload; 32],
        total_len: 2,
        offset: 0,
        payload: vec![payload],
    };
    let mut reassembler = LogicalMessageReassembler::default();
    assert_eq!(reassembler.push(fragment(1, 1), 0).unwrap(), None);
    assert!(
        reassembler
            .push(fragment(1, 2), 0)
            .unwrap_err()
            .contains("disagree")
    );
    reassembler.discard(1);
    for message_id in 0..MAX_INFLIGHT_LOGICAL_MESSAGES as u64 {
        assert_eq!(
            reassembler
                .push(fragment(message_id, message_id as u8), 0)
                .unwrap(),
            None
        );
    }
    assert!(
        reassembler
            .push(fragment(MAX_INFLIGHT_LOGICAL_MESSAGES as u64, 9), 0)
            .unwrap_err()
            .contains("too many incomplete")
    );
}

fn test_message_fragment(
    message_id: u64,
    message_digest: [u8; 32],
    total_len: u64,
    offset: u64,
    payload: Vec<u8>,
) -> WireMessageFragment {
    WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION,
        features: FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_MESSAGE_FRAGMENTATION,
        session: None,
        message_id,
        message_digest,
        total_len,
        offset,
        payload,
    }
}

struct ScriptedSendTransport {
    send_results: std::collections::VecDeque<Result<(), TransportError>>,
}

impl WireTransport for ScriptedSendTransport {
    fn send_frame(&mut self, _frame: Vec<u8>) -> Result<(), TransportError> {
        self.send_results
            .pop_front()
            .expect("send result was scripted")
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        None
    }
}

#[test]
fn receive_poll_reports_permanent_failure_while_flushing_accepted_backlog() {
    let message = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0x77; 16]),
        claims: BTreeMap::new(),
    };
    let mut adapter = WireTransportAdapter::new(
        ScriptedSendTransport {
            send_results: std::collections::VecDeque::from([
                Err(TransportError::Backpressure),
                Err(TransportError::Failed("wire closed".to_owned())),
                Err(TransportError::Failed("wire closed".to_owned())),
            ]),
        },
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_MESSAGE_FRAGMENTATION,
        None,
    );

    assert_eq!(
        adapter.send(message),
        Ok(()),
        "Backpressure accepts the logical message and retains its physical frame"
    );

    // Live peers use `Transport::try_recv_result` to observe receive failures.
    // Exercise the concrete adapter's fallible receive path here to ensure
    // terminal errors remain observable.
    let first = adapter
        .try_recv_strict()
        .expect_err("receive polling must report a permanent flush failure");
    assert_eq!(first.retry, WireRetry::Never);
    assert!(first.message.contains("wire closed"));

    let second = adapter
        .try_recv_strict()
        .expect_err("a failed adapter must not resume silently on a later poll");
    assert_eq!(second.retry, WireRetry::Never);
    assert!(second.message.contains("wire closed"));
}

#[test]
fn pending_outbound_backpressure_retains_a_bounded_fifo_queue() {
    use crate::wire::channels::MAX_CHANNEL_QUEUED_MESSAGES;
    assert_eq!(MAX_CHANNEL_QUEUED_MESSAGES, 1024);
    let mut adapter = WireTransportAdapter::new(
        ScriptedSendTransport {
            send_results: std::iter::repeat_n(
                Err(TransportError::Backpressure),
                MAX_CHANNEL_QUEUED_MESSAGES + 1,
            )
            .collect(),
        },
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD,
        None,
    );
    for _ in 0..MAX_CHANNEL_QUEUED_MESSAGES {
        assert_eq!(
            adapter.send(test_catalogue_ack()),
            Ok(()),
            "accepted logical ownership stays with the bounded channel queue"
        );
    }
    assert_eq!(
        adapter.send(test_catalogue_ack()),
        Err(TransportError::Backpressure),
        "the producer retains only the message beyond the queue bound"
    );
}

#[test]
fn stale_near_limit_staged_bytes_are_reclaimed_and_duplicates_do_not_extend_expiry() {
    let mut reassembler = LogicalMessageReassembler::with_staging_budget_for_test(8);
    let stale = test_message_fragment(1, [1; 32], 8, 0, vec![1; 7]);

    assert_eq!(reassembler.push(stale.clone(), 0).unwrap(), None);
    assert_eq!(
        reassembler
            .push(stale, MAX_FRAGMENT_REASSEMBLY_IDLE_MS - 1)
            .unwrap(),
        None
    );
    assert_eq!(
        reassembler
            .push(
                test_message_fragment(2, [2; 32], 9, 0, vec![2; 8]),
                MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
            )
            .unwrap(),
        None
    );
    assert_eq!(
        (
            reassembler.incomplete.len(),
            reassembler.staged_bytes,
            reassembler.incomplete.contains_key(&2),
        ),
        (1, 8, true)
    );
}

#[test]
fn four_stale_tiny_incomplete_ids_are_reclaimed_before_admitting_another() {
    let mut reassembler = LogicalMessageReassembler::default();
    for message_id in 0..MAX_INFLIGHT_LOGICAL_MESSAGES as u64 {
        assert_eq!(
            reassembler
                .push(
                    test_message_fragment(message_id, [message_id as u8; 32], 2, 0, vec![1]),
                    0,
                )
                .unwrap(),
            None
        );
    }

    assert_eq!(
        reassembler
            .push(
                test_message_fragment(
                    MAX_INFLIGHT_LOGICAL_MESSAGES as u64,
                    [9; 32],
                    2,
                    0,
                    vec![9],
                ),
                MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
            )
            .unwrap(),
        None
    );
    assert_eq!(
        (
            reassembler.incomplete.len(),
            reassembler.staged_bytes,
            reassembler
                .incomplete
                .contains_key(&(MAX_INFLIGHT_LOGICAL_MESSAGES as u64)),
        ),
        (1, 1, true)
    );
}

#[test]
fn actively_progressing_legal_fragmented_message_completes_before_maximum_age() {
    let payload = b"legal fragmented message".to_vec();
    let digest = *blake3::hash(&payload).as_bytes();
    let total_len = payload.len() as u64;
    let mut reassembler = LogicalMessageReassembler::default();

    assert_eq!(
        reassembler
            .push(
                test_message_fragment(7, digest, total_len, 0, payload[..5].to_vec()),
                0,
            )
            .unwrap(),
        None
    );
    assert_eq!(
        reassembler
            .push(
                test_message_fragment(7, digest, total_len, 5, payload[5..11].to_vec()),
                MAX_FRAGMENT_REASSEMBLY_IDLE_MS - 1,
            )
            .unwrap(),
        None
    );
    let envelope = reassembler
        .push(
            test_message_fragment(7, digest, total_len, 11, payload[11..].to_vec()),
            (MAX_FRAGMENT_REASSEMBLY_IDLE_MS - 1) * 2,
        )
        .unwrap()
        .expect("novel extents refresh inactivity without exceeding maximum age");

    assert_eq!(envelope.payload, payload);
}

#[test]
fn steady_progress_cannot_retain_an_incomplete_message_beyond_maximum_age() {
    let mut reassembler = LogicalMessageReassembler::default();
    let mut now_ms = 0;
    let mut offset = 0;
    while now_ms < MAX_FRAGMENT_REASSEMBLY_AGE_MS {
        assert_eq!(
            reassembler
                .push(
                    test_message_fragment(
                        8,
                        [8; 32],
                        MAX_LOGICAL_MESSAGE_BYTES as u64,
                        offset,
                        vec![8],
                    ),
                    now_ms,
                )
                .unwrap(),
            None
        );
        offset += 1;
        now_ms = now_ms.saturating_add(MAX_FRAGMENT_REASSEMBLY_IDLE_MS - 1);
    }

    reassembler.expire(MAX_FRAGMENT_REASSEMBLY_AGE_MS);

    assert_eq!(
        (reassembler.incomplete.len(), reassembler.staged_bytes),
        (0, 0)
    );
}

#[test]
fn active_reassembly_still_rejects_overlapping_extents() {
    let payload = b"abcd";
    let digest = *blake3::hash(payload).as_bytes();
    let mut reassembler = LogicalMessageReassembler::default();
    assert_eq!(
        reassembler
            .push(
                test_message_fragment(9, digest, payload.len() as u64, 0, payload[..2].to_vec()),
                0,
            )
            .unwrap(),
        None
    );

    let error = reassembler
        .push(
            test_message_fragment(9, digest, payload.len() as u64, 1, payload[1..3].to_vec()),
            1,
        )
        .unwrap_err();

    assert!(error.contains("overlapping logical message fragments"));
}

#[test]
fn active_reassembly_still_rejects_a_completed_payload_with_the_wrong_digest() {
    let mut reassembler = LogicalMessageReassembler::default();
    assert_eq!(
        reassembler
            .push(test_message_fragment(10, [0; 32], 2, 0, vec![1]), 0)
            .unwrap(),
        None
    );

    let error = reassembler
        .push(test_message_fragment(10, [0; 32], 2, 1, vec![2]), 1)
        .unwrap_err();

    assert!(error.contains("logical message digest mismatch"));
}

/// Verifies that Alice can finish an older fragmented message after her later
/// message completes first on a reordering transport.
///
/// ```text
/// alice message 1 extent 0 ─┐
/// alice message 2 complete ──┼──► receiver
/// alice message 1 extent 1 ─┘
/// ```
#[test]
fn active_lower_id_reassembly_completes_after_higher_id() {
    let mut reassembler = LogicalMessageReassembler::default();
    let lower = b"ab";
    assert_eq!(
        reassembler
            .push(
                test_message_fragment(
                    1,
                    *blake3::hash(lower).as_bytes(),
                    lower.len() as u64,
                    0,
                    lower[..1].to_vec(),
                ),
                0,
            )
            .unwrap(),
        None
    );
    let completed = vec![2];
    assert!(
        reassembler
            .push(
                test_message_fragment(2, *blake3::hash(&completed).as_bytes(), 1, 0, completed),
                1,
            )
            .unwrap()
            .is_some()
    );

    let envelope = reassembler
        .push(
            test_message_fragment(
                1,
                *blake3::hash(lower).as_bytes(),
                lower.len() as u64,
                1,
                lower[1..].to_vec(),
            ),
            1,
        )
        .unwrap()
        .expect("the older message remains active after the later completion");
    assert_eq!(envelope.payload, lower);
}

/// Verifies that Alice's expired lower message id can start fresh after her
/// higher id completed while physical delivery was reordered.
///
/// ```text
/// alice message 1 extent ─────► receiver ──idle expiry──► new message 1 extent
/// alice message 2 complete ───► receiver
/// ```
#[test]
fn expired_lower_id_restarts_after_higher_id_completion() {
    let mut reassembler = LogicalMessageReassembler::default();
    assert_eq!(
        reassembler
            .push(test_message_fragment(1, [1; 32], 2, 0, vec![1]), 0)
            .unwrap(),
        None
    );
    let completed = vec![2];
    assert!(
        reassembler
            .push(
                test_message_fragment(2, *blake3::hash(&completed).as_bytes(), 1, 0, completed),
                1,
            )
            .unwrap()
            .is_some()
    );

    assert_eq!(
        reassembler
            .push(
                test_message_fragment(1, [1; 32], 2, 0, vec![1]),
                MAX_FRAGMENT_REASSEMBLY_IDLE_MS,
            )
            .unwrap(),
        None
    );
    assert!(reassembler.incomplete.contains_key(&1));
}

#[test]
fn completed_replay_delivers_only_after_exact_recent_completion_eviction() {
    fn complete(
        reassembler: &mut LogicalMessageReassembler,
        message_id: u64,
    ) -> Option<WireEnvelope> {
        let payload = vec![message_id as u8];
        reassembler
            .push(
                test_message_fragment(
                    message_id,
                    *blake3::hash(&payload).as_bytes(),
                    payload.len() as u64,
                    0,
                    payload,
                ),
                0,
            )
            .unwrap()
    }

    assert_eq!(RECENT_COMPLETED_LOGICAL_MESSAGES, 64);
    let mut reassembler = LogicalMessageReassembler::default();
    assert!(complete(&mut reassembler, 0).is_some());
    for message_id in 1..RECENT_COMPLETED_LOGICAL_MESSAGES as u64 {
        assert!(complete(&mut reassembler, message_id).is_some());
    }

    assert_eq!(
        complete(&mut reassembler, 0),
        None,
        "the oldest completion remains deduplicated at the exact cache bound"
    );
    let first_message_after_horizon = RECENT_COMPLETED_LOGICAL_MESSAGES as u64;
    assert!(
        complete(&mut reassembler, first_message_after_horizon).is_some(),
        "the next completion evicts the oldest retained completion"
    );
    assert!(
        complete(&mut reassembler, 0).is_some(),
        "an exact old replay may deliver again only after its completion is evicted"
    );
}

#[test]
fn fragmented_message_survives_mid_send_backpressure_without_semantic_retry() {
    let (left, right) = byte_duplex_raw();
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let mut sender = WireTransportAdapter::current(RecordingChannelTransport {
        inner: left,
        fail_on_call: Some(2),
        attempts: Rc::clone(&attempts),
    });
    let mut receiver = WireTransportAdapter::current(right);
    let message = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0x73; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("compressible".repeat(300_000)),
        )]),
    };
    assert_eq!(sender.send(message.clone()), Ok(()));
    assert!(receiver.try_recv_result().unwrap().is_none());
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
    let frames = attempts.borrow();
    assert!(frames.len() > 2);
    assert_eq!(
        frames[2 - 1],
        frames[2],
        "retry preserves the exact already-compressed extent"
    );
    assert!(
        receiver.try_recv_result().unwrap().is_none(),
        "accepted semantic message is delivered once"
    );
}

#[test]
fn first_frame_backpressure_queues_compressed_logical_message_without_retry() {
    let (left, right) = byte_duplex_raw();
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let mut sender = WireTransportAdapter::current(RecordingChannelTransport {
        inner: left,
        fail_on_call: Some(1),
        attempts: Rc::clone(&attempts),
    });
    let mut receiver = WireTransportAdapter::current(right);
    let message = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0x73; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("compressible".repeat(300_000)),
        )]),
    };
    assert_eq!(sender.send(message.clone()), Ok(()));
    assert!(receiver.try_recv_result().unwrap().is_none());
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
    let frames = attempts.borrow();
    assert!(frames.len() > 1);
    assert_eq!(
        frames[1 - 1],
        frames[1],
        "retry preserves the exact already-compressed extent"
    );
    assert!(
        receiver.try_recv_result().unwrap().is_none(),
        "accepted semantic message is delivered once"
    );
}

#[test]
fn pending_backpressure_admits_later_receipt_in_bounded_channel_queue() {
    #[derive(Clone)]
    struct BlockedWireTransport {
        outbound: Rc<RefCell<std::collections::VecDeque<Vec<u8>>>>,
        blocked: Rc<Cell<bool>>,
    }

    impl WireTransport for BlockedWireTransport {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            if self.blocked.get() {
                return Err(TransportError::Backpressure);
            }
            self.outbound.borrow_mut().push_back(frame);
            Ok(())
        }

        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            None
        }
    }

    let staged = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let blocked = Rc::new(Cell::new(true));
    let mut sender = WireTransportAdapter::current(BlockedWireTransport {
        outbound: Rc::clone(&staged),
        blocked: Rc::clone(&blocked),
    });
    let mut receiver = WireTransportAdapter::current(ByteDuplexTransport {
        outbound: Rc::new(RefCell::new(std::collections::VecDeque::new())),
        inbound: Rc::clone(&staged),
    });
    let view = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0x76; 16]),
        claims: BTreeMap::from([("view".to_owned(), Value::String("pending".to_owned()))]),
    };
    let receipt = SyncMessage::FateUpdate {
        tx_id: TxId::new(
            crate::time::TxTime::from(76),
            NodeUuid::from_bytes([0x76; 16]),
        ),
        fate: Fate::Accepted,
        global_time: None,
        durability: Some(DurabilityTier::Edge),
    };

    assert_eq!(sender.send(view.clone()), Ok(()));
    assert_eq!(
        sender.send(receipt.clone()),
        Ok(()),
        "independent queued receipts are admitted without a semantic retry while space remains"
    );
    assert!(staged.borrow().is_empty());

    blocked.set(false);
    assert!(
        sender.try_recv().is_none(),
        "poll flushes the one already-accepted message"
    );
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), view);
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), receipt);
    assert!(receiver.try_recv().is_none());
}

#[test]
fn reconnect_discards_missing_fragments_and_replays_the_logical_message() {
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let message = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0x74; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("c".repeat(MAX_WIRE_FRAME_BYTES + 700_000)),
        )]),
    };

    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender.send(message.clone()).unwrap();
    staged.borrow_mut().truncate(1);
    assert!(receiver.try_recv().is_none());
    drop(receiver);

    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender.send(message.clone()).unwrap();
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
}

pub(super) fn byte_duplex_with_session(
    identity: AuthorSubject,
    epoch: u64,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (left, right) = byte_duplex_raw();
    let session = WireSession {
        session_id: "test-session".to_owned(),
        epoch,
        identity: Some(identity),
    };
    (
        Box::new(WireTransportAdapter::new(
            left,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS
                | FEATURE_MESSAGE_FRAGMENTATION,
            Some(session.clone()),
        )),
        Box::new(WireTransportAdapter::new(
            right,
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD
                | crate::wire::FEATURE_SESSION_FRAME
                | FEATURE_STRUCTURED_ERRORS
                | FEATURE_MESSAGE_FRAGMENTATION,
            Some(session),
        )),
    )
}

fn test_wire_session(identity: AuthorSubject, epoch: u64) -> WireSession {
    WireSession {
        session_id: "test-session".to_owned(),
        epoch,
        identity: Some(identity),
    }
}

fn test_catalogue_ack() -> SyncMessage {
    SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(1),
        schema: None,
        lens: None,
        applied: true,
    })
}

fn encode_test_message_frame(session: Option<WireSession>) -> Vec<u8> {
    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD
        | crate::wire::FEATURE_SESSION_FRAME
        | FEATURE_STRUCTURED_ERRORS;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, session);
    sender.send(test_catalogue_ack()).unwrap();
    staged.borrow_mut().pop_front().unwrap()
}

fn expect_auth_failed_frame(transport: &mut ByteDuplexTransport, retry: WireRetry, message: &str) {
    let error = transport.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    let WireFrame::Error(WireError {
        code,
        retry: actual_retry,
        message: actual_message,
    }) = frame
    else {
        panic!("expected error frame");
    };
    assert_eq!(code, WireErrorCode::AuthFailed);
    assert_eq!(actual_retry, retry);
    assert!(
        actual_message.contains(message),
        "expected {actual_message:?} to contain {message:?}"
    );
}

#[test]
fn wire_transport_adapter_carries_only_admitted_session_context() {
    let (left, _) = byte_duplex_raw();
    let context = ConnectionSessionContext {
        local: crate::wire::WireAuthorityEndpoint {
            node: NodeUuid::from_bytes([0x81; 16]),
            epoch: 17,
        },
        remote: Some(crate::wire::WireAuthorityEndpoint {
            node: NodeUuid::from_bytes([0x82; 16]),
            epoch: 19,
        }),
        link_identity: AuthorSubject::for_test_bytes([0x83; 16]),
        negotiated_features: crate::wire::FEATURE_AUTHORIZATION_SCOPE_RECEIPTS,
    };
    let adapter = WireTransportAdapter::new_with_session_context(
        left,
        WIRE_PROTOCOL_VERSION,
        context.negotiated_features,
        None,
        Some(context),
    );
    assert_eq!(adapter.connection_session_context(), Some(context));
}

#[test]
fn wire_transport_adapter_reports_malformed_frames() {
    let (left, mut right) = byte_duplex_raw();
    left.inbound.borrow_mut().push_back(vec![0xff, 0x00, 0x01]);

    let mut adapter = WireTransportAdapter::current(left);
    assert!(adapter.try_recv().is_none());

    let error = right.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    assert!(matches!(
        frame,
        WireFrame::Error(WireError {
            code: WireErrorCode::MalformedFrame,
            retry: WireRetry::Never,
            ..
        })
    ));
}

#[test]
fn wire_transport_adapter_reports_oversized_frame_without_decoding() {
    let (left, mut right) = byte_duplex_raw();
    left.inbound
        .borrow_mut()
        .push_back(vec![0_u8; MAX_WIRE_FRAME_BYTES + 1]);

    let mut adapter = WireTransportAdapter::current(left);
    assert!(adapter.try_recv().is_none());

    let error = right.try_recv_frame().expect("structured wire error");
    let frame = decode_frame(&error).unwrap();
    let WireFrame::Error(WireError { code, message, .. }) = frame else {
        panic!("expected error frame");
    };
    assert_eq!(code, WireErrorCode::MalformedFrame);
    assert!(
        message.contains("wire frame size"),
        "unexpected error message: {message}"
    );
}

#[test]
fn wire_transport_adapter_accepts_matching_session() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let session = test_wire_session(identity, 3);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(session.clone())));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(session),
    );

    assert_eq!(adapter.try_recv(), Some(test_catalogue_ack()));
    while let Some(bytes) = right.try_recv_frame() {
        assert!(
            matches!(decode_frame(&bytes).unwrap(), WireFrame::ChannelCredit(_)),
            "matching session emits only consumption credit"
        );
    }
}

#[test]
fn wire_transport_adapter_rejects_missing_session_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorSubject::for_test_bytes([0xa2; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(None));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "missing");
}

#[test]
fn channel_authentication_precedes_payload_admission() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorSubject::for_test_bytes([0xa5; 16]);
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD
        | crate::wire::FEATURE_SESSION_FRAME
        | FEATURE_STRUCTURED_ERRORS;
    let mut frame = decode_frame(&encode_test_message_frame(Some(test_wire_session(
        AuthorSubject::for_test_bytes([0xb5; 16]),
        3,
    ))))
    .unwrap();
    let WireFrame::Channel(envelope) = &mut frame else {
        panic!("channel fixture")
    };
    envelope.extent.message_len = MAX_LOGICAL_MESSAGE_BYTES as u32;
    envelope.extent.decoded_len = 1;
    envelope.extent.payload = vec![0xff]; // invalid semantic bytes must never precede auth validation
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&frame).unwrap());
    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        features,
        Some(test_wire_session(expected_identity, 3)),
    );
    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "identity");
    assert!(
        adapter.try_recv_result().is_err(),
        "authentication failure is terminal"
    );
}

#[test]
fn channel_negotiation_validation_precedes_payload_admission() {
    let (left, mut right) = byte_duplex_raw();
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD;
    let mut frame = decode_frame(&encode_test_message_frame(None)).unwrap();
    let WireFrame::Channel(envelope) = &mut frame else {
        panic!("channel fixture")
    };
    envelope.protocol_version = WIRE_PROTOCOL_VERSION + 1;
    envelope.features = features | crate::wire::FEATURE_PAYLOAD_LZ4;
    envelope.extent.message_len = MAX_LOGICAL_MESSAGE_BYTES as u32;
    envelope.extent.decoded_len = 1;
    envelope.extent.payload = vec![0xff];
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&frame).unwrap());
    let mut adapter = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    assert!(adapter.try_recv().is_none());
    let error = right.try_recv_frame().expect("structured wire error");
    assert!(matches!(
        decode_frame(&error).unwrap(),
        WireFrame::Error(WireError {
            code: WireErrorCode::UnsupportedProtocolVersion,
            ..
        })
    ));
    assert!(adapter.try_recv_result().is_err());
}

#[test]
fn wire_transport_adapter_rejects_wrong_identity_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorSubject::for_test_bytes([0xa3; 16]);
    let actual_identity = AuthorSubject::for_test_bytes([0xb3; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(test_wire_session(
            actual_identity,
            3,
        ))));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(expected_identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "identity");
}

#[test]
fn wire_transport_adapter_rejects_stale_epoch_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorSubject::for_test_bytes([0xa4; 16]);
    left.inbound
        .borrow_mut()
        .push_back(encode_test_message_frame(Some(test_wire_session(
            identity, 2,
        ))));

    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        Some(test_wire_session(identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    expect_auth_failed_frame(&mut right, WireRetry::AfterResume, "stale");
}

#[test]
fn wire_transport_adapter_preserves_message_order() {
    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::current(left);
    let mut receiver = WireTransportAdapter::current(right);
    for revision in [1, 2] {
        sender
            .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
                revision: Some(revision),
                schema: None,
                lens: None,
                applied: true,
            }))
            .unwrap();
    }
    for revision in [1, 2] {
        assert!(matches!(receive_after_pumping(&mut sender, &mut receiver),
            SyncMessage::CatalogueAck(crate::protocol::CatalogueAck { revision: Some(actual), .. }) if actual==revision));
    }
    assert!(receiver.try_recv_result().unwrap().is_none());
}

#[cfg(feature = "transport-compression-lz4")]
#[test]
fn wire_transport_adapter_lz4_compresses_payload_when_negotiated() {
    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_LZ4,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        right,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_LZ4,
        None,
    );
    let message = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(7),
        schema: None,
        lens: None,
        applied: true,
    });

    sender.send(message.clone()).unwrap();
    let raw = sender
        .into_inner()
        .outbound
        .borrow()
        .front()
        .cloned()
        .unwrap();
    let WireFrame::Channel(envelope) = decode_frame(&raw).unwrap() else {
        panic!("expected channel frame");
    };
    assert_eq!(
        envelope.features & crate::wire::FEATURE_PAYLOAD_LZ4,
        crate::wire::FEATURE_PAYLOAD_LZ4
    );
    assert_ne!(
        envelope.extent.payload,
        encode_sync_message(&message).unwrap()
    );
    assert_eq!(receiver.try_recv(), Some(message));
}

#[cfg(feature = "transport-compression-lz4")]
#[test]
fn lz4_fragmentation_round_trips_incompressible_payload_over_logical_limit() {
    // Keep an independent wasm32-safe mirror of the encoded-cap formula.
    const EXPECTED_MAX_ENCODED_MESSAGE_BYTES: usize =
        MAX_LOGICAL_MESSAGE_BYTES + MAX_LOGICAL_MESSAGE_BYTES / 10 + 24;
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let mut bytes = vec![0_u8; MAX_LOGICAL_MESSAGE_BYTES - 1024];
    for byte in &mut bytes {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        *byte = (state >> 56) as u8;
    }
    let message = SyncMessage::SessionClaims {
        identity: AuthorSubject::for_test_bytes([0xa1; 16]),
        claims: BTreeMap::from([("entropy".to_owned(), Value::Bytes(bytes))]),
    };
    let logical_len = encode_sync_message(&message)
        .expect("deterministic message encodes")
        .len();
    assert!(logical_len <= MAX_LOGICAL_MESSAGE_BYTES);
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_LZ4;
    let (left, right) = byte_duplex_raw();
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let mut sender = WireTransportAdapter::new(
        RecordingChannelTransport {
            inner: left,
            fail_on_call: None,
            attempts: Rc::clone(&attempts),
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    sender
        .send(message.clone())
        .expect("decoded payload fits logical cap");
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
    let encoded_len: usize = attempts
        .borrow()
        .iter()
        .map(|bytes| {
            let WireFrame::Channel(frame) = decode_frame(bytes).unwrap() else {
                panic!("channel extent")
            };
            assert_eq!(
                frame.features & crate::wire::FEATURE_PAYLOAD_LZ4,
                crate::wire::FEATURE_PAYLOAD_LZ4
            );
            frame.extent.payload.len()
        })
        .sum();
    assert!(
        encoded_len > MAX_LOGICAL_MESSAGE_BYTES,
        "stream E={encoded_len} exceeds D cap while D={logical_len} stays legal"
    );
    assert!(
        encoded_len <= EXPECTED_MAX_ENCODED_MESSAGE_BYTES,
        "stream E={encoded_len} stays within the independent encoded cap"
    );
    assert!(receiver.try_recv_result().unwrap().is_none());
}

#[cfg(feature = "transport-compression-lz4")]
#[test]
fn lz4_fragmentation_rejects_encoded_payload_over_encoded_cap_before_admitting() {
    const EXPECTED_MAX_ENCODED_MESSAGE_BYTES: usize =
        MAX_LOGICAL_MESSAGE_BYTES + MAX_LOGICAL_MESSAGE_BYTES / 10 + 24;
    // Reassembly is the private pre-allocation resource seam: use a one-byte
    // synthetic extent so this rejection proves no over-cap payload is staged
    // without allocating a payload near the encoded cap.
    let mut reassembler = LogicalMessageReassembler::default();
    let error = reassembler
        .push(
            WireMessageFragment {
                protocol_version: WIRE_PROTOCOL_VERSION,
                features: FEATURE_SYNC_MESSAGE_PAYLOAD
                    | crate::wire::FEATURE_PAYLOAD_LZ4
                    | FEATURE_MESSAGE_FRAGMENTATION,
                session: None,
                message_id: 91,
                message_digest: [9; 32],
                total_len: (EXPECTED_MAX_ENCODED_MESSAGE_BYTES + 1) as u64,
                offset: 0,
                payload: vec![9],
            },
            0,
        )
        .expect_err("encoded payload over the encoded cap must be rejected");
    assert!(error.contains("encoded message payload"));
    assert!(reassembler.incomplete.is_empty());
    assert_eq!(reassembler.staged_bytes, 0);
}

#[cfg(feature = "transport-compression-lz4")]
#[test]
fn lz4_decoder_rejects_decompressed_payload_over_logical_limit() {
    let mut decoder =
        WireStreamDecoder::new(crate::wire::FEATURE_PAYLOAD_LZ4).expect("lz4 decoder");
    let mut decompression_bomb = (MAX_LOGICAL_MESSAGE_BYTES as u32 + 1)
        .to_le_bytes()
        .to_vec();
    decompression_bomb.push(0);
    let error = decoder
        .decode_message(&decompression_bomb, crate::wire::FEATURE_PAYLOAD_LZ4)
        .expect_err("receiver must retain the decompressed-output bound");
    assert!(error.contains("exceeds max"));
}

#[cfg(feature = "transport-compression-zstd")]
#[test]
fn wire_transport_adapter_zstd_stream_preserves_message_order() {
    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_ZSTD,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        right,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_ZSTD,
        None,
    );
    let first = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(7),
        schema: None,
        lens: None,
        applied: true,
    });
    let second = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(8),
        schema: None,
        lens: None,
        applied: true,
    });

    sender.send(first.clone()).unwrap();
    sender.send(second.clone()).unwrap();

    assert_eq!(receiver.try_recv(), Some(first));
    assert_eq!(receiver.try_recv(), Some(second));
}

/// Alice's real shape registration/subscription produces Bob's stored row while
/// her requested immutable-node upload is still only partly sent on the same
/// adapter. This checks carrier scheduling and actual query service; it does
/// not stand in for the separate upload-before-row/settlement acceptance gates.
/// alice --upload root/frontier--> bob --Need(nodes)--> alice
/// alice --large nodes + query--> bob --ViewUpdate--> alice (nodes still pending)
#[test]
fn small_query_completes_while_requested_bulk_upload_is_in_flight() {
    use crate::protocol::{ChunkUploadNodes, ChunkUploadStart, ChunkUploadStatus};
    use crate::wire::channels::{CHANNEL_CHUNK_BYTES, ChannelClass};
    use groove::large_values::{LargeValueKind, prepare_streaming};
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xd1; 16]);
    let bob = open_core(0xd2, AuthorSubject::SYSTEM, &schema);
    bob.insert_with_id(
        "todos",
        row(0xd3),
        cells("small query result", false, alice),
    )
    .unwrap();
    let mut state = 0x9e37_79b9_u32;
    let bytes: Vec<_> = (0..8 * 1024 * 1024)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 17;
            state ^= state << 5;
            state as u8
        })
        .collect();
    let mut chunks = Vec::new();
    let (value_ref, _) = prepare_streaming(LargeValueKind::Bytes, bytes.as_slice(), |chunk| {
        chunks.push(chunk);
        Ok(())
    })
    .unwrap();
    let (client_bytes, server_bytes) = byte_duplex_raw();
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let mut client = WireTransportAdapter::current(RecordingChannelTransport {
        inner: client_bytes,
        fail_on_call: None,
        attempts: Rc::clone(&attempts),
    });
    let subscriber =
        bob.accept_subscriber(Box::new(WireTransportAdapter::current(server_bytes)), alice);
    client
        .send(SyncMessage::ChunkUploadStart(ChunkUploadStart {
            value_ref: value_ref.clone(),
        }))
        .unwrap();
    let bulk = (0..16)
        .find_map(|_| {
            let result = drive_subscriber_until_payload(&subscriber, &mut client);
            let SyncMessage::ChunkUploadResult(result) = result else {
                panic!("expected upload frontier, got {result:?}")
            };
            let ChunkUploadStatus::Need(nodes) = result.status else {
                panic!("expected missing immutable nodes")
            };
            let batch = SyncMessage::ChunkUploadNodes(ChunkUploadNodes {
                value_ref: value_ref.clone(),
                chunks: nodes
                    .iter()
                    .map(|node| {
                        chunks
                            .iter()
                            .find(|chunk| &chunk.node_ref == node)
                            .unwrap()
                            .clone()
                    })
                    .collect(),
            });
            if encode_sync_message(&batch).unwrap().len() > 16 * CHANNEL_CHUNK_BYTES {
                return Some(batch);
            }
            client.send(batch).unwrap();
            None
        })
        .expect("real upload reaches a bulk missing-node frontier");
    let bulk_start = attempts.borrow().len();
    client.send(bulk).unwrap();
    let query = Query::from("todos");
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    client
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: subscription.clone(),
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    let view = (0..128)
        .find_map(|_| {
            subscriber.borrow_mut().tick().unwrap();
            if let Some(message) = try_recv_subscriber_payload(&mut client) {
                if let SyncMessage::ViewUpdate(view) = message {
                    return Some(view);
                }
                panic!("query must finish before the bulk upload response: {message:?}");
            }
            None
        })
        .expect("small query completes within bounded channel turns");
    assert_eq!(view.subscription, subscription);
    assert!(
        view.supporting_rows
            .added_rows()
            .iter()
            .any(|fact| fact.row == row(0xd3))
    );
    let sent = attempts.borrow();
    let bulk_extents: Vec<_> = sent[bulk_start..]
        .iter()
        .filter_map(|bytes| match decode_frame(bytes).unwrap() {
            WireFrame::Channel(frame) if frame.extent.class == ChannelClass::LargeValue => {
                Some(frame.extent)
            }
            _ => None,
        })
        .collect();
    assert!(!bulk_extents.is_empty());
    assert!(bulk_extents[0].first);
    assert!(
        bulk_extents.iter().all(|extent| !extent.last),
        "query service must not wait for bulk completion"
    );
}

/// Alice cannot continue a partial stateful message after its idle deadline;
/// Bob rejects the connection instead of skipping expired dictionary bytes.
#[test]
fn partial_channel_expiry_requires_reconnect() {
    let (left, right) = byte_duplex_raw();
    let mut sender = WireTransportAdapter::current(left);
    let mut receiver = WireTransportAdapter::current(right);
    sender
        .send(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0xe1; 16]),
            claims: BTreeMap::from([(
                "large".to_owned(),
                Value::String("x".repeat(2 * crate::wire::channels::CHANNEL_CHUNK_BYTES)),
            )]),
        })
        .unwrap();
    assert!(receiver.try_recv_result().unwrap().is_none());
    receiver.set_reassembly_elapsed_for_test(MAX_FRAGMENT_REASSEMBLY_IDLE_MS);
    let error = receiver.try_recv_result().unwrap_err();
    assert!(format!("{error:?}").contains("expired"));
    assert!(receiver.try_recv_result().is_err());
}

/// A standalone adapter owns auxiliary deadlines too; no binding pump is
/// required to expose the deadline and retire its expired partial receive.
#[test]
fn standalone_auxiliary_partial_exposes_deadline_and_flush_expires_it() {
    use crate::wire::channels::{AUXILIARY_CHANNEL, ChannelClass, ChannelFrame};
    let (mut sender, receiver) = byte_duplex_raw();
    let mut receiver = WireTransportAdapter::current(receiver);
    receiver
        .shared_auxiliary_endpoint()
        .unwrap()
        .lock()
        .unwrap()
        .set_incomplete_receive_timeout_for_test(10);
    sender
        .send_frame(
            encode_frame(&WireFrame::Channel(crate::wire::WireChannelEnvelope {
                protocol_version: WIRE_PROTOCOL_VERSION,
                features: crate::wire::FEATURE_NONE,
                session: None,
                extent: ChannelFrame {
                    channel: AUXILIARY_CHANNEL,
                    generation: 0,
                    sequence: 0,
                    class: ChannelClass::Auxiliary,
                    first: true,
                    last: false,
                    message_len: 2,
                    decoded_len: 1,
                    payload: vec![0],
                },
            }))
            .unwrap(),
        )
        .unwrap();
    assert!(receiver.try_recv_result().unwrap().is_none());
    let remaining = receiver
        .incomplete_receive_timeout_ms()
        .expect("auxiliary partial arms host timer");
    std::thread::sleep(std::time::Duration::from_millis(remaining + 1));
    assert!(receiver.poll_flush().is_err());
    assert!(receiver.has_terminal_failure());
    assert!(receiver.incomplete_receive_timeout_ms().is_none());
}

/// Mallory sends a valid zstd stream dominated by empty raw blocks. Bob must
/// charge encoded bytes cumulatively even though each extent yields one byte.
/// This preserves #3027's independent encoded cap on the live channel path.
#[cfg(feature = "transport-compression-zstd")]
#[test]
fn channel_encoded_budget_is_enforced_independently_of_decoded_bytes() {
    use crate::wire::channels::{ChannelClass, ChannelFrame, MAX_CHANNEL_FRAME_PAYLOAD};
    const EXPECTED_ENCODED_CAP: usize =
        MAX_LOGICAL_MESSAGE_BYTES + MAX_LOGICAL_MESSAGE_BYTES / 10 + 24;
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | crate::wire::FEATURE_PAYLOAD_ZSTD;
    let (left, right) = byte_duplex_raw();
    let inbound = Rc::clone(&right.inbound);
    let mut bob = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    // Keep the credit-return sink alive; this test is a raw peer so it does not
    // need a sender-side codec or allocation proportional to declared length.
    let _mallory = left;
    let mut encoded = 0;
    for sequence in 0..=(EXPECTED_ENCODED_CAP / (MAX_CHANNEL_FRAME_PAYLOAD - 3) + 1) as u64 {
        let mut payload = if sequence == 0 {
            vec![0x28, 0xb5, 0x2f, 0xfd, 0, 0x30]
        } else {
            Vec::new()
        };
        payload.extend_from_slice(&[8, 0, 0, 0]); // one nonterminal raw byte
        while payload.len() + 3 <= MAX_CHANNEL_FRAME_PAYLOAD {
            payload.extend_from_slice(&[0, 0, 0]);
        }
        encoded += payload.len();
        let frame = WireFrame::Channel(crate::wire::WireChannelEnvelope {
            protocol_version: WIRE_PROTOCOL_VERSION,
            features,
            session: None,
            extent: ChannelFrame {
                channel: 2,
                generation: 0,
                sequence,
                class: ChannelClass::Writes,
                first: sequence == 0,
                last: false,
                message_len: if sequence == 0 {
                    MAX_LOGICAL_MESSAGE_BYTES as u32
                } else {
                    0
                },
                decoded_len: 1,
                payload,
            },
        });
        inbound
            .borrow_mut()
            .push_back(encode_frame(&frame).unwrap());
        let result = bob.try_recv_result();
        if encoded > EXPECTED_ENCODED_CAP {
            let error = result.expect_err("first byte beyond encoded cap is rejected");
            assert!(format!("{error:?}").contains("encoded message exceeds size limit"));
            assert!(bob.try_recv_result().is_err());
            return;
        }
        assert_eq!(result.unwrap(), None, "legal encoded prefix stays partial");
    }
    panic!("fixture must cross the independent encoded cap");
}

/// Mallory's first extent declares more than the logical cap; Bob rejects it
/// before decoding even a one-byte payload. A compressed bomb cannot use the
/// larger encoded budget to expand the decoded semantic budget (#3027).
#[test]
fn channel_decoded_budget_is_checked_before_payload_admission() {
    let (left, right) = byte_duplex_raw();
    let mut frame = decode_frame(&encode_test_message_frame(None)).unwrap();
    let WireFrame::Channel(envelope) = &mut frame else {
        panic!("channel fixture")
    };
    envelope.features = FEATURE_SYNC_MESSAGE_PAYLOAD;
    envelope.extent.message_len = MAX_LOGICAL_MESSAGE_BYTES as u32 + 1;
    envelope.extent.payload = vec![0xff];
    // Bypass the semantic sender's admission to model a malicious physical
    // peer; the receiving adapter must validate the declared logical length.
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&frame).unwrap());
    let mut bob = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD,
        None,
    );
    let error = bob.try_recv_result().unwrap_err();
    assert!(format!("{error:?}").contains("logical size limit"));
    assert!(bob.try_recv_result().is_err());
    drop(right);
}

// Receipt-validation counters are internal because public message equality cannot
// reveal whether an admitted trusted link redundantly revalidates every receipt.
#[test]
fn channel_adapter_propagates_locally_admitted_trusted_decoder_context() {
    let message = SyncMessage::RowVersionPayloads {
        version_bundles: transport_version_bundles(2),
    };
    for trusted in [false, true, false] {
        let (left, right) = byte_duplex_raw();
        let mut sender = WireTransportAdapter::current(left);
        let mut receiver = WireTransportAdapter::current(right);
        receiver.set_trusted_encoder(trusted);
        sender.send(message.clone()).unwrap();
        crate::protocol::RECEIPT_VALIDATIONS.with(|count| count.set(0));
        assert_eq!(receive_after_pumping(&mut sender, &mut receiver), message);
        let validations = crate::protocol::RECEIPT_VALIDATIONS.with(|count| count.get());
        if trusted {
            assert_eq!(
                validations, 0,
                "local admission propagates to channel decoder"
            );
        } else {
            assert!(
                validations >= 2,
                "untrusted transport validates every receipt"
            );
        }
    }
}

#[test]
fn channel_adapter_preserves_remote_typed_terminal_error() {
    let (left, mut right) = byte_duplex_raw();
    let expected = WireError {
        code: WireErrorCode::AuthFailed,
        retry: WireRetry::AfterAuth,
        message: "session expired".to_owned(),
    };
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&WireFrame::Error(expected.clone())).unwrap());
    let mut receiver = WireTransportAdapter::current(left);
    for _ in 0..2 {
        let actual = receiver.try_recv_strict().unwrap_err();
        assert_eq!(actual.code, expected.code);
        assert_eq!(actual.retry, expected.retry);
        assert_eq!(actual.message, expected.message);
    }
    assert!(
        right.try_recv_frame().is_none(),
        "received error must not be echoed"
    );
}
fn transport_version_bundles(count: usize) -> Vec<crate::protocol::VersionBundle> {
    use crate::protocol::{VersionBundle, VersionRecord};
    use crate::schema::ColumnSchema;
    use crate::time::{GlobalTime, TxTime};
    use crate::tx::{DurabilityTier, Fate, Transaction, TxId, TxKind};
    use groove::schema::ColumnType;
    let table = TableSchema::new("todos", [ColumnSchema::new("title", ColumnType::String)]);
    let schema_version = SchemaVersionId::from_bytes([0x44; 16]);
    let node = NodeUuid::from_bytes([0x11; 16]);
    let author = AuthorSubject::for_test_bytes([0x55; 16]);
    (0..count)
        .map(|index| {
            let tx_id = TxId::new(TxTime(1_000 + index as u64), node);
            VersionBundle {
                tx: Transaction {
                    tx_id,
                    kind: TxKind::Mergeable,
                    n_total_writes: 1,
                    made_by: author,
                    permission_subject: None,
                    base_snapshot: None,
                    row_read_set: None,
                    absent_read_set: None,
                    predicate_read_set: None,
                    user_metadata_json: None,
                    contribution_merge: None,
                },
                versions: vec![
                    VersionRecord::from_cells(
                        &table,
                        schema_version,
                        RowUuid::from_bytes([index as u8; 16]),
                        Vec::new(),
                        author,
                        1_000 + index as u64,
                        author,
                        1_000 + index as u64,
                        &BTreeMap::from([("title".to_owned(), format!("todo-{index}"))]),
                        None,
                    )
                    .unwrap(),
                ],
                scope: crate::protocol::VersionBundleScope::CompleteTransaction,
                fate: Fate::Accepted,
                global_time: Some(GlobalTime(10_000 + index as u64)),
                // A sequence is the global-authority receipt, and so its
                // companion durability is Global in every valid fixture.
                durability: DurabilityTier::Global,
            }
        })
        .collect()
}

// A standalone adapter owns both queues without the native pump. Physical
// frame inspection is required to pin its bounded 4:1 arbitration contract.
#[test]
fn standalone_adapter_services_auxiliary_after_four_canonical_extents() {
    use crate::protocol::{ChunkResponse, ChunkResponseBatch, ChunkResponseEntry};
    use crate::wire::channels::{CHANNEL_CHUNK_BYTES, ChannelClass};
    let (left, right) = byte_duplex_raw();
    let attempts = Rc::new(RefCell::new(Vec::new()));
    let mut sender = WireTransportAdapter::current(RecordingChannelTransport {
        inner: left,
        fail_on_call: None,
        attempts: Rc::clone(&attempts),
    });
    sender
        .send(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0xf1; 16]),
            claims: BTreeMap::from([(
                "bulk".to_owned(),
                Value::Bytes(vec![7; 16 * CHANNEL_CHUNK_BYTES]),
            )]),
        })
        .unwrap();
    sender
        .send(SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
            responses: vec![ChunkResponseEntry {
                request_id: 1,
                result: ChunkResponse::Found(vec![9; 32]),
            }],
        }))
        .unwrap();
    sender.poll_flush().unwrap();
    let frames: Vec<_> = attempts
        .borrow()
        .iter()
        .map(|bytes| {
            let WireFrame::Channel(frame) = decode_frame(bytes).unwrap() else {
                panic!("channel frame");
            };
            frame.extent
        })
        .collect();
    assert!(frames.len() >= 5);
    assert!(
        frames[..4]
            .iter()
            .all(|frame| frame.class != ChannelClass::Auxiliary && !frame.last)
    );
    assert_eq!(frames[4].class, ChannelClass::Auxiliary);
    assert!(frames[4].first && frames[4].last);
    drop(right);
}

// Review regression: physical generations must survive ordinary admission refusal.
// An internal seam is needed because row APIs hide transport queue capacity.
#[test]
fn rejected_dynamic_channel_admission_preserves_generation() {
    struct GatedWire {
        inner: ByteDuplexTransport,
        blocked: Rc<Cell<bool>>,
    }
    impl WireTransport for GatedWire {
        fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
            if self.blocked.get() {
                Err(TransportError::Backpressure)
            } else {
                self.inner.send_frame(frame)
            }
        }
        fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
            self.inner.try_recv_frame()
        }
    }
    let reply = |byte| SyncMessage::PermissionAdviceResponse {
        request_id: crate::protocol::PermissionAdviceRequestId([byte; 16]),
        advice: crate::protocol::PermissionAdvice::Unknown,
    };
    let (left, right) = byte_duplex_raw();
    let blocked = Rc::new(Cell::new(false));
    let mut sender = WireTransportAdapter::current(GatedWire {
        inner: left,
        blocked: Rc::clone(&blocked),
    });
    let mut receiver = WireTransportAdapter::current(right);
    sender.send(reply(1)).unwrap();
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), reply(1));
    // The received message's last owner has dropped. Deliver its buffer-credit
    // receipt before deliberately filling the entire new outstanding window.
    receiver.poll_flush().unwrap();
    assert!(sender.try_recv_result().unwrap().is_none());
    blocked.set(true);
    for _ in 0..crate::wire::channels::MAX_CHANNEL_QUEUED_MESSAGES {
        sender.send(test_catalogue_ack()).unwrap();
    }
    for byte in [2, 3] {
        assert_eq!(sender.send(reply(byte)), Err(TransportError::Backpressure));
    }
    blocked.set(false);
    for _ in 0..crate::wire::channels::MAX_CHANNEL_QUEUED_MESSAGES {
        assert_eq!(
            receive_after_pumping(&mut sender, &mut receiver),
            test_catalogue_ack()
        );
    }
    sender.send(reply(4)).unwrap();
    assert_eq!(receive_after_pumping(&mut sender, &mut receiver), reply(4));
}

/// A real server snapshot introduces a previously unknown transaction. Its
/// later fate arrives first on another ordered stream; canonical application
/// must still install the snapshot before attempting to apply that fate.
#[test]
fn reordered_delivery_and_fate_apply_in_dependency_order_to_real_client() {
    real_client_reordered_delivery_and_fate(false, false);
}

#[test]
fn repaired_wire_view_keeps_lease_and_defers_dependent_fate() {
    real_client_reordered_delivery_and_fate(true, false);
}

#[test]
fn disconnect_releases_retained_wire_repair_and_fate() {
    real_client_reordered_delivery_and_fate(true, true);
}

fn real_client_reordered_delivery_and_fate(repair: bool, detach: bool) {
    struct Tap {
        inner: WireTransportAdapter<ByteDuplexTransport>,
        sent: Rc<RefCell<Vec<SyncMessage>>>,
        omit_bodies: bool,
    }
    impl Transport for Tap {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            let mut wire_message = message.clone();
            if self.omit_bodies {
                if let SyncMessage::ViewUpdate(view) = &mut wire_message {
                    view.version_carriers.clear();
                }
            }
            self.inner.send(wire_message)?;
            self.sent.borrow_mut().push(message);
            Ok(())
        }
        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.inner.try_recv()
        }
        fn try_recv_owned_result(
            &mut self,
        ) -> Result<Option<crate::db::ReceivedSyncMessage>, TransportError> {
            self.inner.try_recv_owned_result()
        }
        fn poll_flush(&mut self) -> Result<crate::db::WireFlushStatus, TransportError> {
            self.inner.poll_flush()
        }
    }
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0xc2, AuthorSubject::SYSTEM, &schema);
    server
        .insert_with_id(
            "todos",
            row(0xc3),
            cells(
                &if repair {
                    "r".repeat(40 * 1024)
                } else {
                    "dependency ordered".to_owned()
                },
                false,
                alice,
            ),
        )
        .unwrap();
    if repair {
        server
            .insert_with_id(
                "todos",
                row(0xc5),
                cells(&"s".repeat(40 * 1024), false, alice),
            )
            .unwrap();
    }
    let client = open_db(0xc4, alice, &schema);
    let (client_bytes, server_bytes) = byte_duplex_raw();
    let inbound = Rc::clone(&client_bytes.inbound);
    let sent = Rc::new(RefCell::new(Vec::new()));
    let client_adapter = WireTransportAdapter::current(client_bytes);
    let receiver_credits = client_adapter
        .shared_auxiliary_endpoint()
        .unwrap()
        .lock()
        .unwrap()
        .channel_credits();
    let server_adapter = WireTransportAdapter::current(server_bytes);
    let sender_credits = server_adapter
        .shared_auxiliary_endpoint()
        .unwrap()
        .lock()
        .unwrap()
        .channel_credits();
    let upstream = block_on(client.connect_upstream(Box::new(client_adapter)));
    let subscriber = server.accept_subscriber(
        Box::new(Tap {
            inner: server_adapter,
            sent: Rc::clone(&sent),
            omit_bodies: repair,
        }),
        alice,
    );
    for _ in 0..8 {
        client.tick().unwrap();
        subscriber.borrow_mut().tick().unwrap();
    }
    let _subscription =
        prepared_subscribe(&client, &Query::from("todos"), global_subscribe_opts()).unwrap();
    client.tick().unwrap();
    let view = (0..32)
        .find_map(|_| {
            subscriber.borrow_mut().tick().unwrap();
            sent.borrow().iter().find_map(|message| match message {
                SyncMessage::ViewUpdate(view) => Some(view.clone()),
                _ => None,
            })
        })
        .expect("real query emits snapshot");
    let tx_id = view
        .version_carriers
        .iter()
        .flat_map(|carrier| carrier.expand().unwrap())
        .next()
        .expect("snapshot carries row transaction")
        .tx
        .tx_id;
    subscriber
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();
    subscriber.borrow_mut().transport.poll_flush().unwrap();
    // Reorder only across streams. Each stream's own physical FIFO is kept.
    let mut frames: Vec<_> = inbound.borrow_mut().drain(..).collect();
    frames.sort_by_key(|bytes| match decode_frame(bytes).unwrap() {
        WireFrame::Channel(frame)
            if frame.extent.class == crate::wire::channels::ChannelClass::Writes =>
        {
            0
        }
        _ => 1,
    });
    assert!(
        matches!(decode_frame(&frames[0]).unwrap(),WireFrame::Channel(frame) if frame.extent.class==crate::wire::channels::ChannelClass::Writes)
    );
    inbound.borrow_mut().extend(frames);
    block_on(async { upstream.lock().await.tick().await })
        .expect("fate cannot run before its snapshot introduces the transaction");
    client.tick().unwrap();
    if repair {
        {
            let upstream = upstream.borrow();
            let ConnectionLink::Upstream(state) = &upstream.link else {
                unreachable!()
            };
            assert_eq!(state.pending_row_version_repairs.len(), 1);
            assert!(
                state.pending_row_version_repairs[0].lease.is_some(),
                "retained view owns its reservation"
            );
            assert_eq!(state.deferred_repair_fates.len(), 1);
            assert!(
                state.deferred_repair_fates[0].lease.is_some(),
                "deferred fate owns its reservation"
            );
        }
        if detach {
            assert!(client.detach_connection(&upstream));
            let upstream = upstream.borrow();
            let ConnectionLink::Upstream(state) = &upstream.link else {
                unreachable!()
            };
            assert!(state.pending_row_version_repairs.is_empty());
            assert!(state.deferred_repair_fates.is_empty());
            assert!(upstream.staged_inbound.is_empty());
            return;
        }
        // Model another retained bulk view without allocating D bytes. The
        // real repair below exceeds one extent and must use progress capacity.
        use crate::wire::channel_credit::ChannelCredits;
        use crate::wire::channels::ChannelClass;
        let held_bulk = ChannelCredits::receive_message(
            &receiver_credits,
            ChannelClass::Delivery,
            crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES,
        )
        .unwrap();
        sender_credits.lock().unwrap().reserve_message(
            ChannelClass::Delivery,
            crate::protocol_limits::MAX_LOGICAL_MESSAGE_BYTES,
        );
        for _ in 0..32 {
            subscriber.borrow_mut().tick().unwrap();
            client.tick().unwrap();
        }
        assert!(
            sent.borrow().iter().any(|message| matches!(
                message,
                SyncMessage::RowVersionPayloads { .. }
            ) && crate::wire::encode_sync_message(message)
                .unwrap()
                .len()
                > crate::wire::channels::CHANNEL_CHUNK_BYTES),
            "repair genuinely needs bulk-sized progress capacity"
        );
        drop(held_bulk);
        let upstream = upstream.borrow();
        let ConnectionLink::Upstream(state) = &upstream.link else {
            unreachable!()
        };
        assert!(state.pending_row_version_repairs.is_empty());
        assert!(state.deferred_repair_fates.is_empty());
    }
    assert_eq!(
        row_ids(&prepared_read(&client, &Query::from("todos"))),
        if repair {
            vec![row(0xc3), row(0xc5)]
        } else {
            vec![row(0xc3)]
        }
    );
}
#[test]
fn deferred_view_retains_fate_dependency() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xb1; 16]);
    let server = open_core(0xb1, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xb2, alice, &schema);
    let row = RowUuid::from_bytes([0xb3; 16]);
    server
        .insert_with_id("todos", row, cells("0", false, alice))
        .unwrap();
    let (upstream, downstream, _requests, responses) = duplex_with_taps();
    let upstream = block_on(client.connect_upstream(upstream));
    let subscriber = server.accept_subscriber(downstream, alice);
    let mut stream =
        prepared_subscribe(&client, &Query::from("todos"), global_subscribe_opts()).unwrap();
    let mut held = Vec::new();
    let mut introduced = None;
    let mut snapshot = RelationSnapshot::default();
    for revision in 0..2 {
        if revision > 0 {
            server
                .update(
                    "todos",
                    row,
                    BTreeMap::from([("title".to_owned(), Value::String(revision.to_string()))]),
                )
                .unwrap();
        }
        for _ in 0..8 {
            subscriber.borrow_mut().tick().unwrap();
            // Only the first snapshot needs a body fetch. Later deltas carry
            // their own bodies but must wait behind that predecessor.
            responses.borrow_mut().retain_mut(|message| {
                match message {
                    SyncMessage::ViewUpdate(payload) if payload.supporting_rows.is_snapshot() => {
                        payload.version_carriers.clear();
                    }
                    SyncMessage::ViewUpdate(payload) => {
                        introduced = payload
                            .supporting_rows
                            .added_rows()
                            .first()
                            .map(|row| row.version.tx);
                    }
                    SyncMessage::RowVersionPayloads { .. } => {
                        held.push(message.clone());
                        return false;
                    }
                    _ => {}
                }
                true
            });
            client.tick().unwrap();
            while let Some(event) = stream.try_next_event() {
                apply_subscription_event(&mut snapshot, event);
            }
        }
    }
    responses.borrow_mut().push_back(SyncMessage::FateUpdate {
        tx_id: introduced.expect("later delta introduces transaction"),
        fate: Fate::Accepted,
        global_time: None,
        durability: Some(DurabilityTier::Edge),
    });
    client
        .tick()
        .expect("fate following an admitted but body-deferred view must remain valid");
    let queued = match &upstream.borrow().link {
        ConnectionLink::Upstream(state) => state.pending_row_version_repairs.len(),
        _ => unreachable!("client upstream"),
    };
    assert!(
        queued == 2,
        "snapshot and all dependent deltas must remain ordered: {queued}"
    );
    assert!(
        snapshot.rows.is_empty(),
        "no successor may install before its missing predecessor"
    );
    assert!(!held.is_empty(), "the first repair was actually delayed");
    responses.borrow_mut().extend(held);
    for _ in 0..24 {
        subscriber.borrow_mut().tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = stream.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
    }
    assert_eq!(snapshot.rows.len(), 1);
    assert_eq!(
        snapshot.rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("1".to_owned()))
    );
}
