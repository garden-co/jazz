//! Framing, fragmentation, compression, and authenticated wire-session tests.

use super::*;

#[test]
fn logical_message_larger_than_frame_round_trips_reordered_and_duplicated() {
    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    let body = (0..(MAX_WIRE_FRAME_BYTES + 700_000))
        .map(|index| ((index.wrapping_mul(31) % 251) as u8) as char)
        .collect::<String>();
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x71; 16]),
        claims: BTreeMap::from([("large".to_owned(), Value::String(body))]),
    };

    sender.send(message.clone()).unwrap();
    let mut frames = staged.borrow_mut().drain(..).collect::<Vec<_>>();
    assert!(frames.len() > 1);
    assert!(
        frames
            .iter()
            .all(|frame| frame.len() <= MAX_WIRE_FRAME_BYTES)
    );
    frames.push(frames[0].clone());
    frames.reverse();
    staged.borrow_mut().extend(frames);

    let repeated = message.clone();
    assert_eq!(receiver.try_recv(), Some(message));
    assert!(receiver.try_recv().is_none());

    sender.send(repeated.clone()).unwrap();
    assert_eq!(receiver.try_recv(), Some(repeated));
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
            identity: AuthorId::SYSTEM,
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
    let mut evolved_schema = base.clone();
    let large_default = Value::String("x".repeat(MAX_WIRE_FRAME_BYTES + 1024));
    evolved_schema.tables[0].columns.push(
        crate::schema::ColumnSchema::new("large_default", ColumnType::String)
            .with_default(large_default.clone()),
    );
    let evolved = crate::protocol::SchemaVersion::new(evolved_schema);
    let publication = crate::protocol::SchemaLineagePublication::new(
        evolved.clone(),
        crate::protocol::MigrationLens::new(
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
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    let message = SyncMessage::PublishSchemaWithLens {
        author: AuthorId::SYSTEM,
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
    let frames = staged.borrow_mut().drain(..).collect::<Vec<_>>();
    assert!(frames.len() > 1);
    assert!(
        frames
            .iter()
            .all(|frame| frame.len() <= MAX_WIRE_FRAME_BYTES)
    );

    let authority = open_core(0x38, AuthorId::SYSTEM, &base);
    for frame in &frames[..frames.len() - 1] {
        staged.borrow_mut().push_back(frame.clone());
        assert!(receiver.try_recv().is_none());
        assert!(
            !authority
                .node()
                .borrow()
                .catalogue_schemas()
                .contains_key(&evolved.id)
        );
    }
    staged
        .borrow_mut()
        .push_back(frames.last().unwrap().clone());
    let reassembled = receiver
        .try_recv()
        .expect("final fragment completes message");
    assert_eq!(reassembled, message);
    authority
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message(reassembled)
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
fn corrupt_fragment_never_admits_a_partial_logical_message() {
    let (left, right) = byte_duplex_raw();
    let staged = Rc::clone(&right.inbound);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);
    let mut receiver = WireTransportAdapter::new(right, WIRE_PROTOCOL_VERSION, features, None);
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x72; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("q".repeat(MAX_WIRE_FRAME_BYTES + 64)),
        )]),
    };

    sender.send(message).unwrap();
    {
        let mut staged = staged.borrow_mut();
        let encoded = staged
            .iter_mut()
            .find(|encoded| {
                matches!(
                    decode_frame(encoded),
                    Ok(WireFrame::MessageFragment(fragment))
                        if fragment.payload.contains(&b'q')
                )
            })
            .expect("encoded string body byte exists in a fragment");
        let mut frame = decode_frame(encoded).unwrap();
        let WireFrame::MessageFragment(fragment) = &mut frame else {
            unreachable!("selected a fragment frame")
        };
        let byte = fragment
            .payload
            .iter_mut()
            .find(|byte| **byte == b'q')
            .expect("string body byte exists");
        *byte = b'r';
        *encoded = encode_frame(&frame).unwrap();
    }
    assert!(receiver.try_recv().is_none());
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
    assert_eq!(reassembler.push(fragment(1, 1)).unwrap(), None);
    assert!(
        reassembler
            .push(fragment(1, 2))
            .unwrap_err()
            .contains("disagree")
    );
    reassembler.discard(1);
    for message_id in 0..MAX_INFLIGHT_LOGICAL_MESSAGES as u64 {
        assert_eq!(
            reassembler
                .push(fragment(message_id, message_id as u8))
                .unwrap(),
            None
        );
    }
    assert!(
        reassembler
            .push(fragment(MAX_INFLIGHT_LOGICAL_MESSAGES as u64, 9))
            .unwrap_err()
            .contains("too many incomplete")
    );
}

#[test]
fn fragmented_message_survives_mid_send_backpressure_without_semantic_retry() {
    let staged = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let receiver_inbound = Rc::clone(&staged);
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let mut sender = WireTransportAdapter::new(
        OneShotBackpressureTransport {
            outbound: staged,
            calls: 0,
            fail_on_call: 2,
            failed: false,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        ByteDuplexTransport {
            outbound: Rc::new(RefCell::new(std::collections::VecDeque::new())),
            inbound: receiver_inbound,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x73; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("b".repeat(MAX_WIRE_FRAME_BYTES + 700_000)),
        )]),
    };

    sender.send(message.clone()).unwrap();
    assert!(receiver.try_recv().is_none());
    assert!(
        sender.try_recv().is_none(),
        "poll flushes the accepted logical message"
    );
    assert_eq!(receiver.try_recv(), Some(message));
}

#[test]
fn first_frame_backpressure_queues_compressed_logical_message_without_retry() {
    let staged = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let receiver_inbound = Rc::clone(&staged);
    let features = current_wire_features();
    let mut sender = WireTransportAdapter::new(
        OneShotBackpressureTransport {
            outbound: staged,
            calls: 0,
            fail_on_call: 1,
            failed: false,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let mut receiver = WireTransportAdapter::new(
        ByteDuplexTransport {
            outbound: Rc::new(RefCell::new(std::collections::VecDeque::new())),
            inbound: receiver_inbound,
        },
        WIRE_PROTOCOL_VERSION,
        features,
        None,
    );
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x75; 16]),
        claims: BTreeMap::from([(
            "large".to_owned(),
            Value::String("compressible".repeat(300_000)),
        )]),
    };

    assert_eq!(sender.send(message.clone()), Ok(()));
    assert!(receiver.try_recv().is_none());
    assert!(
        sender.try_recv().is_none(),
        "poll flushes the accepted message"
    );
    assert_eq!(receiver.try_recv(), Some(message));
}

#[test]
fn reconnect_discards_missing_fragments_and_replays_the_logical_message() {
    let features =
        FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_STRUCTURED_ERRORS | FEATURE_MESSAGE_FRAGMENTATION;
    let message = SyncMessage::SessionClaims {
        identity: AuthorId::from_bytes([0x74; 16]),
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
    assert_eq!(receiver.try_recv(), Some(message));
}

pub(super) fn byte_duplex_with_session(
    identity: AuthorId,
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

fn test_wire_session(identity: AuthorId, epoch: u64) -> WireSession {
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
    let payload = encode_sync_message(&test_catalogue_ack()).unwrap();
    let mut envelope = WireEnvelope::new(
        WIRE_PROTOCOL_VERSION,
        FEATURE_SYNC_MESSAGE_PAYLOAD
            | crate::wire::FEATURE_SESSION_FRAME
            | FEATURE_STRUCTURED_ERRORS,
        payload,
    );
    if let Some(session) = session {
        envelope = envelope.with_session(session);
    }
    encode_frame(&WireFrame::Message(envelope)).unwrap()
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
        remote: crate::wire::WireAuthorityEndpoint {
            node: NodeUuid::from_bytes([0x82; 16]),
            epoch: 19,
        },
        link_identity: AuthorId::from_bytes([0x83; 16]),
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
    let identity = AuthorId::from_bytes([0xa1; 16]);
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
    assert!(right.try_recv_frame().is_none());
}

#[test]
fn wire_transport_adapter_rejects_missing_session_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let identity = AuthorId::from_bytes([0xa2; 16]);
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
fn fragment_authentication_precedes_reassembly_allocation() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorId::from_bytes([0xa5; 16]);
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD
        | crate::wire::FEATURE_SESSION_FRAME
        | FEATURE_STRUCTURED_ERRORS
        | FEATURE_MESSAGE_FRAGMENTATION;
    let fragment = WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION,
        features,
        session: Some(test_wire_session(AuthorId::from_bytes([0xb5; 16]), 3)),
        message_id: 41,
        message_digest: [7; 32],
        total_len: MAX_LOGICAL_MESSAGE_BYTES as u64,
        offset: 0,
        payload: vec![7],
    };
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&WireFrame::MessageFragment(fragment)).unwrap());
    let mut adapter = WireTransportAdapter::new(
        left,
        WIRE_PROTOCOL_VERSION,
        features,
        Some(test_wire_session(expected_identity, 3)),
    );

    assert!(adapter.try_recv().is_none());
    assert!(adapter.reassembler.incomplete.is_empty());
    assert_eq!(adapter.reassembler.staged_bytes, 0);
    expect_auth_failed_frame(&mut right, WireRetry::AfterAuth, "identity");
}

#[test]
fn fragment_negotiation_validation_precedes_reassembly_allocation() {
    let (left, mut right) = byte_duplex_raw();
    let features = FEATURE_SYNC_MESSAGE_PAYLOAD | FEATURE_MESSAGE_FRAGMENTATION;
    let fragment = WireMessageFragment {
        protocol_version: WIRE_PROTOCOL_VERSION + 1,
        features: features | crate::wire::FEATURE_PAYLOAD_LZ4,
        session: None,
        message_id: 42,
        message_digest: [8; 32],
        total_len: MAX_LOGICAL_MESSAGE_BYTES as u64,
        offset: 0,
        payload: vec![8],
    };
    left.inbound
        .borrow_mut()
        .push_back(encode_frame(&WireFrame::MessageFragment(fragment)).unwrap());
    let mut adapter = WireTransportAdapter::new(left, WIRE_PROTOCOL_VERSION, features, None);

    assert!(adapter.try_recv().is_none());
    assert!(adapter.reassembler.incomplete.is_empty());
    assert_eq!(adapter.reassembler.staged_bytes, 0);
    let error = right.try_recv_frame().expect("structured wire error");
    assert!(matches!(
        decode_frame(&error).unwrap(),
        WireFrame::Error(WireError {
            code: WireErrorCode::UnsupportedProtocolVersion,
            ..
        })
    ));
}

#[test]
fn wire_transport_adapter_rejects_wrong_identity_without_emitting_sync_message() {
    let (left, mut right) = byte_duplex_raw();
    let expected_identity = AuthorId::from_bytes([0xa3; 16]);
    let actual_identity = AuthorId::from_bytes([0xb3; 16]);
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
    let identity = AuthorId::from_bytes([0xa4; 16]);
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
    let (left, mut right) = byte_duplex_raw();
    let mut adapter = WireTransportAdapter::current(left);

    adapter
        .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            schema: None,
            lens: None,
            applied: true,
        }))
        .unwrap();
    adapter
        .send(SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(2),
            schema: None,
            lens: None,
            applied: true,
        }))
        .unwrap();

    let first = right.try_recv_frame().unwrap();
    let second = right.try_recv_frame().unwrap();
    let mut decoder = WireStreamDecoder::new(current_wire_features()).unwrap();
    let first = match decode_frame(&first).unwrap() {
        WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
        other => panic!("expected message frame, got {other:?}"),
    };
    let second = match decode_frame(&second).unwrap() {
        WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
        other => panic!("expected message frame, got {other:?}"),
    };

    assert!(matches!(
        first,
        SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(1),
            ..
        })
    ));
    assert!(matches!(
        second,
        SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
            revision: Some(2),
            ..
        })
    ));
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
    let mut message = SyncMessage::CatalogueAck(crate::protocol::CatalogueAck {
        revision: Some(7),
        schema: None,
        lens: None,
        applied: true,
    });

    sender.send(message.clone()).unwrap();
    let mut raw = sender
        .into_inner()
        .outbound
        .borrow()
        .front()
        .cloned()
        .unwrap();
    let WireFrame::Message(envelope) = decode_frame(&raw).unwrap() else {
        panic!("expected message frame");
    };
    assert_eq!(
        envelope.features & crate::wire::FEATURE_PAYLOAD_LZ4,
        crate::wire::FEATURE_PAYLOAD_LZ4
    );
    assert_ne!(envelope.payload, encode_sync_message(&message).unwrap());
    assert_eq!(receiver.try_recv(), Some(message));
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
