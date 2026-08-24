use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};

use groove::chunks::{ChunkKvStorage, ChunkProvider, ChunkStorage, MissingChunkResolver};

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
            locator: prepared.value_ref.root.locator,
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
        locator: groove::large_values::Locator::random(),
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

// This stays at the auxiliary-pump boundary because the batch split is a
// hop-local wire-protocol contract, not a user-visible database operation.
#[test]
fn five_concurrent_chunk_demands_are_delivered_in_two_decodable_batches() {
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

        let requests = (0_u8..5)
            .map(|index| groove::chunks::ChunkRequest {
                object_hash: groove::large_values::object_hash(&[index]).0,
                locator: groove::large_values::Locator::random(),
            })
            .collect::<Vec<_>>();
        for (index, request) in requests.iter().enumerate() {
            source
                .put_if_absent(
                    request.locator.clone(),
                    groove::large_values::ContentHash(request.object_hash),
                    bytes::Bytes::from(vec![index as u8]),
                )
                .await
                .unwrap();
        }

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
        let provider =
            groove::chunks::StorageChunkProvider::with_resolver(destination, Rc::new(resolver));
        let mut reads = requests
            .iter()
            .cloned()
            .map(|request| provider.get(request))
            .collect::<Vec<_>>();
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        for read in &mut reads {
            assert!(matches!(Pin::new(read).poll(&mut context), Poll::Pending));
        }

        for expected_batch_len in [4, 1] {
            let request_batch = downstream.take_outbound(64).unwrap();
            let SyncMessage::ChunkRequestBatch(batch) = request_batch else {
                unreachable!();
            };
            assert_eq!(batch.requests.len(), expected_batch_len);
            let encoded =
                crate::wire::encode_sync_message(&SyncMessage::ChunkRequestBatch(batch.clone()))
                    .unwrap();
            assert!(crate::wire::decode_sync_message(&encoded).is_ok());
            upstream
                .route_incoming(SyncMessage::ChunkRequestBatch(batch))
                .await
                .unwrap();
            let response_batch = upstream.take_outbound(64).unwrap();
            downstream.route_incoming(response_batch).await.unwrap();
        }
        assert!(downstream.take_outbound(64).is_none());

        for (index, read) in reads.into_iter().enumerate() {
            assert_eq!(read.await.unwrap().as_ref(), &[index as u8]);
        }
    });
}

#[test]
fn retryable_chunk_response_preserves_retry_delay_and_allows_a_later_fulfillment() {
    crate::db::block_on(async {
        let resolver = PeerChunkResolver::default();
        let schema = groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new());
        let database = groove::db::Database::new(
            schema,
            groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
        )
        .await
        .unwrap();
        let upstream = PeerIoPump::new(
            resolver.clone(),
            database.local_chunk_reader(),
            41,
            PeerIoPumpRole::Upstream,
        );
        let request = groove::chunks::ChunkRequest {
            object_hash: [0x41; 32],
            locator: vec![0x42; 16],
        };

        let first = resolver.resolve(request.clone());
        let first_id = match upstream.take_outbound(1).unwrap() {
            SyncMessage::ChunkRequestBatch(batch) => batch.requests[0].request_id,
            _ => unreachable!(),
        };
        upstream
            .route_incoming(SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
                responses: vec![ChunkResponseEntry {
                    request_id: first_id,
                    result: ChunkResponse::Retryable {
                        retry_after_ms: 10_000,
                    },
                }],
            }))
            .await
            .unwrap();
        assert_eq!(
            first.await,
            Err(groove::chunks::ChunkError::Retryable {
                retry_after_ms: 10_000
            }),
            "the binding must distinguish a retry instruction from permanent unavailability"
        );

        let second = resolver.resolve(request);
        let second_id = match upstream.take_outbound(1).unwrap() {
            SyncMessage::ChunkRequestBatch(batch) => batch.requests[0].request_id,
            _ => unreachable!(),
        };
        upstream
            .route_incoming(SyncMessage::ChunkResponseBatch(ChunkResponseBatch {
                responses: vec![ChunkResponseEntry {
                    request_id: second_id,
                    result: ChunkResponse::Found(vec![0x99]),
                }],
            }))
            .await
            .unwrap();
        assert_eq!(second.await.unwrap().as_ref(), &[0x99]);
    });
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
            locator: groove::large_values::Locator::random(),
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
