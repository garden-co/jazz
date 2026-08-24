use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use groove::chunks::{ChunkProvider, ChunkStorage, MissingChunkResolver};

use super::super::*;

#[test]
fn auxiliary_pump_completes_a_suspended_groove_chunk_read_without_a_semantic_tick() {
    crate::db::block_on(async {
        let source = Rc::new(groove::chunks::MemoryChunkStorage::new());
        let destination = Rc::new(groove::chunks::MemoryChunkStorage::new());
        let schema = groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new());
        let mut source_database = groove::db::Database::new(
            schema.clone(),
            groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
        )
        .await
        .unwrap();
        source_database.set_chunk_storage(source.clone());
        let mut destination_database = groove::db::Database::new(
            schema,
            groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
        )
        .await
        .unwrap();
        destination_database.set_chunk_storage(destination.clone());
        let prepared = groove::large_values::prepare(
            groove::large_values::LargeValueKind::Bytes,
            &vec![7; 32 * 1024],
            |_| groove::large_values::Locator(uuid::Uuid::new_v4().as_bytes().to_vec()),
        )
        .unwrap();
        source.stage(prepared.staged_chunks.clone()).await.unwrap();

        let resolver = PeerChunkResolver::default();
        let downstream = PeerIoPump::new(
            resolver.clone(),
            destination_database.local_chunk_reader(),
            1,
            PeerIoPumpRole::Upstream,
        );
        let upstream = PeerIoPump::new(
            resolver.clone(),
            source_database.local_chunk_reader(),
            2,
            PeerIoPumpRole::Subscriber,
        );
        let provider = groove::chunks::StorageChunkProvider::with_resolver(
            destination.clone(),
            Rc::new(resolver),
        );
        let request = groove::chunks::ChunkRequest {
            object_hash: prepared.value_ref.root.object_hash.0,
            locator: prepared.value_ref.root.locator.0.clone(),
        };
        let mut read = provider.get(request.clone());
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut read).poll(&mut context),
            Poll::Pending
        ));

        let features = crate::wire::current_wire_features();
        assert!(
            upstream
                .route_incoming_wire_frame(
                    downstream
                        .take_outbound_wire_frame(
                            crate::wire::WIRE_PROTOCOL_VERSION,
                            features,
                            None,
                        )
                        .unwrap()
                        .unwrap(),
                    features,
                )
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            downstream
                .route_incoming_wire_frame(
                    upstream
                        .take_outbound_wire_frame(
                            crate::wire::WIRE_PROTOCOL_VERSION,
                            features,
                            None,
                        )
                        .unwrap()
                        .unwrap(),
                    features,
                )
                .await
                .unwrap()
                .is_none()
        );

        let bytes = read.await.unwrap();
        assert!(!bytes.is_empty());
        assert!(
            destination
                .get(
                    request.locator,
                    groove::large_values::ContentHash(request.object_hash),
                )
                .await
                .is_ok()
        );
    });
}

#[test]
fn subscriber_auxiliary_responses_are_bounded_to_one_chunk_per_wire_frame() {
    let resolver = PeerChunkResolver::default();
    let schema = groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new());
    let database = crate::db::block_on(groove::db::Database::new(
        schema,
        groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
    ))
    .unwrap();
    let subscriber = PeerIoPump::new(
        resolver.clone(),
        database.local_chunk_reader(),
        23,
        PeerIoPumpRole::Subscriber,
    );
    let response_count = 10;
    let chunk_bytes = vec![0x5a; groove::large_values::LEAF_MAX_BYTES];
    resolver.state.borrow_mut().relay_responses.insert(
        23,
        (0..response_count)
            .map(|request_id| ChunkResponseEntry {
                request_id,
                result: ChunkResponse::Found(chunk_bytes.clone()),
            })
            .collect(),
    );

    let features = crate::wire::current_wire_features();
    let mut observed_request_ids = Vec::new();
    for _ in 0..response_count {
        let frame = subscriber
            .take_outbound_wire_frame(crate::wire::WIRE_PROTOCOL_VERSION, features, None)
            .unwrap()
            .expect("each queued response has its own wire frame");
        assert!(
            frame.len() <= crate::protocol_limits::MAX_WIRE_FRAME_BYTES,
            "one auxiliary frame stays below the wire allocation limit"
        );
        let crate::wire::WireFrame::Message(envelope) = crate::wire::decode_frame(&frame).unwrap()
        else {
            panic!("auxiliary output is a complete message frame");
        };
        let payload =
            crate::wire::decompress_sync_payload(&envelope.payload, envelope.features).unwrap();
        let SyncMessage::ChunkResponseBatch(batch) =
            crate::wire::decode_sync_message_for_features(&payload, features).unwrap()
        else {
            panic!("subscriber emits chunk responses");
        };
        assert_eq!(batch.responses.len(), 1, "the requested bound is honored");
        observed_request_ids.push(batch.responses[0].request_id);
    }
    assert_eq!(
        observed_request_ids,
        (0..response_count).collect::<Vec<_>>()
    );
    assert!(
        subscriber
            .take_outbound_wire_frame(crate::wire::WIRE_PROTOCOL_VERSION, features, None)
            .unwrap()
            .is_none()
    );
}

#[test]
fn dropping_the_last_suspended_consumer_cancels_unsent_chunk_demand() {
    let resolver = PeerChunkResolver::default();
    let schema = groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new());
    let database = crate::db::block_on(groove::db::Database::new(
        schema,
        groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
    ))
    .unwrap();
    let pump = PeerIoPump::new(
        resolver.clone(),
        database.local_chunk_reader(),
        9,
        PeerIoPumpRole::Upstream,
    );
    let request = groove::chunks::ChunkRequest {
        object_hash: [4; 32],
        locator: vec![5; 16],
    };
    let mut pending = resolver.resolve(request);
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Pending
    ));
    drop(pending);
    assert!(pump.take_outbound(64).is_none());
}

#[test]
fn a_late_response_from_a_disconnected_upstream_cannot_complete_reassigned_demand() {
    crate::db::block_on(async {
        let resolver = PeerChunkResolver::default();
        let schema = groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new());
        let database = groove::db::Database::new(
            schema,
            groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
        )
        .await
        .unwrap();
        let first = PeerIoPump::new(
            resolver.clone(),
            database.local_chunk_reader(),
            10,
            PeerIoPumpRole::Upstream,
        );
        let successor = PeerIoPump::new(
            resolver.clone(),
            database.local_chunk_reader(),
            11,
            PeerIoPumpRole::Upstream,
        );
        let request = groove::chunks::ChunkRequest {
            object_hash: [6; 32],
            locator: vec![7; 16],
        };
        let mut pending = resolver.resolve(request);
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(
            Pin::new(&mut pending).poll(&mut context),
            Poll::Pending
        ));
        let request_id = match first.take_outbound(64).unwrap() {
            SyncMessage::ChunkRequestBatch(batch) => batch.requests[0].request_id,
            _ => unreachable!(),
        };

        first.disconnect();
        assert!(matches!(
            successor.take_outbound(64),
            Some(SyncMessage::ChunkRequestBatch(_))
        ));
        first
            .route_incoming(SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
                responses: vec![ChunkResponseEntry {
                    request_id,
                    result: ChunkResponse::Found(vec![1]),
                }],
            }))
            .await
            .unwrap();
        assert!(matches!(
            Pin::new(&mut pending).poll(&mut context),
            Poll::Pending
        ));

        successor
            .route_incoming(SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
                responses: vec![ChunkResponseEntry {
                    request_id,
                    result: ChunkResponse::Found(vec![2]),
                }],
            }))
            .await
            .unwrap();
        assert_eq!(pending.await.unwrap().as_ref(), &[2]);
    });
}
