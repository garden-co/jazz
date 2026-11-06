//! Wire fixtures are assembled independently: row queries alone cannot pin
//! discriminants, UUID lengths or the order of paired provenance fields.
use jazz::db::{ReadSpan, SyncMessage, decode_message, encode_frames};
use jazz::ids::AuthorSubject;

/// Alice's request and Bob's empty/nonempty responses retain the named v1
/// encoding, including different state and retained-content writer origins.
#[test]
fn accepted_evidence_v1_has_independent_request_and_origin_fixtures() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/accepted_read_v1.json")).unwrap();
    let request = hex::decode(fixture["request_hex"].as_str().unwrap()).unwrap();
    let decoded = decode_message(&request).unwrap();
    assert!(
        matches!(&decoded, SyncMessage::Read { span: ReadSpan::AcceptedOnce { at: None, schema, scope: None }, .. } if schema == &[0x44;16])
    );
    assert_eq!(encode_frames(decoded).unwrap(), [request]);
    for (field, count, through) in [("empty_reply_hex", 0, 0), ("paired_reply_hex", 1, 9)] {
        let bytes = hex::decode(fixture[field].as_str().unwrap()).unwrap();
        let message = decode_message(&bytes).unwrap();
        let SyncMessage::Accepted { id, read } = &message else {
            panic!("accepted reply")
        };
        assert_eq!(*id, 1);
        assert_eq!(read.schema, [0x44; 16]);
        assert_eq!(read.through, through);
        assert_eq!(read.scope.author, AuthorSubject::SYSTEM);
        assert!(read.scope.claims.is_empty());
        assert_eq!(read.evidence.len(), count);
        let rows = jazz::binding_codec::decode_rows(&read.rows).unwrap();
        assert_eq!(
            rows.iter().map(|batch| batch.rows.len()).sum::<usize>(),
            count
        );
        if let Some(evidence) = read.evidence.first() {
            assert_eq!(evidence.row.0.as_bytes(), &[0x11; 16]);
            assert_eq!(evidence.state.transaction.time.0, 3);
            assert_eq!(evidence.state.transaction.node.as_bytes(), &[0x22; 16]);
            assert_eq!(evidence.state.sequence, 9);
            assert_eq!(evidence.content.transaction.time.0, 2);
            assert_eq!(evidence.content.transaction.node.as_bytes(), &[0x33; 16]);
            assert_eq!(evidence.content.sequence, 7);
            assert_eq!(
                hex::encode(&read.rows),
                fixture["paired_rows_hex"].as_str().unwrap()
            );
        }
        assert_eq!(encode_frames(message).unwrap(), [bytes]);
    }
}

/// Mallory cannot use the outer snapshot field to reinterpret authority
/// evidence as a local observation. Unsupported modes refuse before reading.
#[test]
fn accepted_evidence_refuses_non_current_outer_views() {
    use jazz::db::{ReadTier, block_on, duplex};
    use jazz::protocol::ReadViewSourceSpec;
    use jazz::query::Query;
    let server = crate::accepted_rpc_reads::open(9, AuthorSubject::SYSTEM, true, false);
    let snapshot = block_on(server.capture_snapshot()).unwrap();
    let (mut peer, down) = duplex();
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    peer.send(SyncMessage::Read {
        id: 11,
        query: Box::new(Query::from("notes")),
        binding: Default::default(),
        include_deleted: false,
        tier: ReadTier::Remote,
        wait: None,
        since: None,
        view: Box::new(ReadViewSourceSpec::Snapshot { snapshot }),
        span: ReadSpan::AcceptedOnce {
            at: None,
            schema: *crate::accepted_rpc_reads::schema(false)
                .version_id()
                .as_bytes(),
            scope: None,
        },
    })
    .unwrap();
    block_on(server.tick()).unwrap();
    let mut refused = false;
    while let Some(message) = peer.try_recv() {
        if let SyncMessage::Refused { id: 11, code, .. } = message {
            assert_eq!(code, jazz::db::ErrorCode::Query);
            refused = true;
        }
    }
    assert!(refused);
}

/// Bob's malformed or mismatched response cannot satisfy Alice's request.
/// A protocol peer is necessary to inject evidence an authority cannot emit.
#[test]
fn malformed_evidence_and_mismatched_schema_coordinates_are_refused() {
    use jazz::db::{block_on, duplex};
    use jazz::query::Query;
    use std::{
        future::Future,
        task::{Context, Poll, Waker},
    };
    for mutation in 0..5 {
        let client =
            crate::accepted_rpc_reads::open(7, crate::accepted_rpc_reads::alice(), false, false);
        let (up, mut peer) = duplex();
        block_on(client.connect_upstream(up));
        let query = client.prepare_query(&Query::from("notes")).unwrap();
        let mut future = Box::pin(client.accepted_query(&query, Some(9), false, None));
        assert!(
            future
                .as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        block_on(client.tick()).unwrap();
        let mut requested = None;
        while let Some(message) = peer.try_recv() {
            if let SyncMessage::Read {
                id,
                span: ReadSpan::AcceptedOnce { .. },
                ..
            } = message
            {
                requested = Some(id);
            }
        }
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../fixtures/accepted_read_v1.json")).unwrap();
        let bytes = hex::decode(fixture["paired_reply_hex"].as_str().unwrap()).unwrap();
        let SyncMessage::Accepted { mut read, .. } = decode_message(&bytes).unwrap() else {
            panic!("fixture")
        };
        read.schema = *crate::accepted_rpc_reads::schema(false)
            .version_id()
            .as_bytes();
        match mutation {
            0 => read.schema = [0x55; 16],
            1 => read.through = 10,
            2 => read.evidence.clear(),
            3 => read.evidence[0].state.sequence = 10,
            4 => read.evidence[0].content.sequence = 10,
            _ => unreachable!(),
        }
        peer.send(SyncMessage::Accepted {
            id: requested.unwrap(),
            read,
        })
        .unwrap();
        block_on(client.tick()).unwrap();
        let Poll::Ready(Err(error)) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("malformed reply accepted or left pending")
        };
        assert_eq!(error.code, jazz::db::ErrorCode::Protocol);
        drop(future);
        assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    }
}
