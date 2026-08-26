use std::cell::RefCell;
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use groove::chunks::{ChunkKvStorage, ChunkProvider, ChunkStorage, MissingChunkResolver};

use super::super::*;
use super::{duplex, open_db, schema};

#[derive(Default)]
struct DeferredChunkStorage {
    backend: groove::chunks::MemoryChunkStorage,
    outcome: RefCell<Option<Result<bytes::Bytes, groove::chunks::ChunkStorageError>>>,
    waiter: RefCell<Option<Waker>>,
}

impl DeferredChunkStorage {
    fn release(&self, outcome: Result<bytes::Bytes, groove::chunks::ChunkStorageError>) {
        *self.outcome.borrow_mut() = Some(outcome);
        if let Some(waiter) = self.waiter.borrow_mut().take() {
            waiter.wake();
        }
    }
}

impl ChunkStorage for DeferredChunkStorage {
    fn get(
        &self,
        _locator: groove::large_values::Locator,
        _expected_hash: groove::large_values::ContentHash,
    ) -> groove::chunks::ChunkFuture<'_, Result<bytes::Bytes, groove::chunks::ChunkStorageError>>
    {
        Box::pin(std::future::poll_fn(|context| {
            if let Some(outcome) = self.outcome.borrow_mut().take() {
                Poll::Ready(outcome)
            } else {
                *self.waiter.borrow_mut() = Some(context.waker().clone());
                Poll::Pending
            }
        }))
    }

    fn stage(
        &self,
        chunks: Vec<groove::large_values::StagedChunk>,
    ) -> groove::chunks::ChunkFuture<
        '_,
        Result<groove::large_values::StagedLargeValueAccounting, groove::chunks::ChunkStorageError>,
    > {
        self.backend.stage(chunks)
    }

    fn delete(
        &self,
        locator: groove::large_values::Locator,
        expected_hash: groove::large_values::ContentHash,
    ) -> groove::chunks::ChunkFuture<'_, Result<(), groove::chunks::ChunkStorageError>> {
        self.backend.delete(locator, expected_hash)
    }
}

fn deferred_local_chunk_reader() -> (groove::chunks::LocalChunkReader, Rc<DeferredChunkStorage>) {
    let storage = Rc::new(DeferredChunkStorage::default());
    let mut database = crate::db::block_on(groove::db::Database::new(
        groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new()),
        groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
    ))
    .unwrap();
    database.set_chunk_storage(storage.clone());
    (database.local_chunk_reader(), storage)
}

fn retained_relay_obligations(state: &ChunkDemandState) -> usize {
    state.relay_chunk_obligations
}

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
    {
        let mut state = resolver.state.borrow_mut();
        state.relay_responses.insert(
            23,
            (0..response_count)
                .map(|request_id| ChunkResponseEntry {
                    request_id,
                    result: ChunkResponse::Found(chunk_bytes.clone()),
                })
                .collect(),
        );
        state.relay_chunk_obligations = response_count as usize;
    }

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
fn bounded_auxiliary_drain_keeps_large_response_batches_fifo_and_within_bytes() {
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
        24,
        PeerIoPumpRole::Subscriber,
    );
    let response_count = 32_u64;
    let chunk_bytes = vec![0x3c; groove::large_values::LEAF_MAX_BYTES];
    {
        let mut state = resolver.state.borrow_mut();
        state.relay_responses.insert(
            24,
            (0..response_count)
                .map(|request_id| ChunkResponseEntry {
                    request_id,
                    result: ChunkResponse::Found(chunk_bytes.clone()),
                })
                .collect(),
        );
        state.relay_chunk_obligations = response_count as usize;
    }

    let features = crate::wire::current_wire_features();
    let byte_budget = groove::large_values::LEAF_MAX_BYTES * 4;
    let mut observed_request_ids = Vec::new();
    loop {
        let frames = subscriber
            .take_outbound_wire_frames(
                crate::wire::WIRE_PROTOCOL_VERSION,
                features,
                None,
                8,
                byte_budget,
            )
            .unwrap();
        if frames.is_empty() {
            break;
        }
        assert!(
            frames.len() <= 8,
            "frame-count budget bounds every host batch"
        );
        assert!(
            frames.iter().map(Vec::len).sum::<usize>() <= byte_budget,
            "byte budget bounds every host batch"
        );
        for frame in frames {
            let crate::wire::WireFrame::Message(envelope) =
                crate::wire::decode_frame(&frame).unwrap()
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
            assert_eq!(batch.responses.len(), 1, "per-frame bound remains intact");
            observed_request_ids.push(batch.responses[0].request_id);
        }
    }
    assert_eq!(
        observed_request_ids,
        (0..response_count).collect::<Vec<_>>(),
        "each bounded drain preserves the queued response order and reaches the tail"
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

#[test]
fn failed_send_restore_keeps_its_relay_reservation_across_later_admission() {
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
        40,
        PeerIoPumpRole::Subscriber,
    );
    let shared = ChunkRequestEntry {
        request_id: 0,
        locator: groove::large_values::Locator::random(),
        expected_hash: [40; 32],
        remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
    };
    for request_id in 0..MAX_RELAY_CHUNK_OBLIGATIONS as u64 - 1 {
        resolver.enqueue_relay(
            40,
            ChunkRequestEntry {
                request_id,
                ..shared.clone()
            },
        );
    }
    resolver.enqueue_relay(
        40,
        ChunkRequestEntry {
            request_id: MAX_RELAY_CHUNK_OBLIGATIONS as u64,
            remaining_hops: 0,
            ..shared.clone()
        },
    );
    let handed_out = subscriber
        .take_outbound(1)
        .expect("response is handed to send");
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_RELAY_CHUNK_OBLIGATIONS,
        "a send in progress keeps its reservation"
    );

    resolver.enqueue_relay(
        40,
        ChunkRequestEntry {
            request_id: MAX_RELAY_CHUNK_OBLIGATIONS as u64 + 1,
            ..shared
        },
    );
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_RELAY_CHUNK_OBLIGATIONS,
        "a later relay request cannot consume the failed send's slot"
    );
    subscriber.restore_outbound(handed_out);
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_RELAY_CHUNK_OBLIGATIONS,
        "restoring an unsent batch transfers no extra reservation"
    );
}

#[test]
fn partial_drain_then_disconnect_releases_only_that_connections_obligations() {
    let resolver = PeerChunkResolver::default();
    let schema = groove::schema::DatabaseSchema::new(Vec::<groove::schema::TableSchema>::new());
    let database = crate::db::block_on(groove::db::Database::new(
        schema,
        groove::storage::MemoryStorage::new(&[groove::db::LARGE_VALUE_METADATA_CF]),
    ))
    .unwrap();
    let first = PeerIoPump::new(
        resolver.clone(),
        database.local_chunk_reader(),
        41,
        PeerIoPumpRole::Subscriber,
    );
    let second = PeerIoPump::new(
        resolver.clone(),
        database.local_chunk_reader(),
        42,
        PeerIoPumpRole::Subscriber,
    );
    for (connection, request_id) in [(41, 1), (41, 2), (42, 3), (42, 4)] {
        resolver.enqueue_relay(
            connection,
            ChunkRequestEntry {
                request_id,
                locator: groove::large_values::Locator::random(),
                expected_hash: [41; 32],
                remaining_hops: 0,
            },
        );
    }
    let handed_out = first.take_outbound(1).expect("partial response drain");
    first.disconnect();
    let state = resolver.state.borrow();
    assert_eq!(retained_relay_obligations(&state), 2);
    assert!(state.relay_responses.get(&41).is_none());
    assert!(state.inflight_relay_responses.get(&41).is_none());
    drop(state);
    let sent = second.take_outbound(1).expect("other connection is intact");
    second.acknowledge_outbound(&sent);
    assert_eq!(retained_relay_obligations(&resolver.state.borrow()), 1);
    drop(handed_out);
}

#[test]
fn mixed_waiter_and_immediate_response_saturation_never_exceeds_the_shared_cap() {
    let resolver = PeerChunkResolver::default();
    let request = ChunkRequestEntry {
        request_id: 0,
        locator: groove::large_values::Locator::random(),
        expected_hash: [43; 32],
        remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
    };
    for request_id in 0..MAX_RELAY_CHUNK_OBLIGATIONS as u64 - 2 {
        resolver.enqueue_relay(
            43,
            ChunkRequestEntry {
                request_id,
                ..request.clone()
            },
        );
    }
    resolver.enqueue_relay(
        43,
        ChunkRequestEntry {
            request_id: 9_000,
            remaining_hops: 0,
            ..request.clone()
        },
    );
    resolver.enqueue_relay(
        43,
        ChunkRequestEntry {
            request_id: 9_001,
            ..request.clone()
        },
    );
    resolver.enqueue_relay(
        43,
        ChunkRequestEntry {
            request_id: 9_002,
            remaining_hops: 0,
            ..request
        },
    );
    let state = resolver.state.borrow();
    assert_eq!(
        retained_relay_obligations(&state),
        MAX_RELAY_CHUNK_OBLIGATIONS
    );
    assert_eq!(
        state.relay_responses.get(&43).map_or(0, Vec::len),
        1,
        "both immediate unavailable and overload retry responses obey the same cap"
    );
}

#[test]
fn completion_transfers_a_relay_reservation_until_the_response_is_acknowledged() {
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
        46,
        PeerIoPumpRole::Subscriber,
    );
    resolver.enqueue_relay(
        46,
        ChunkRequestEntry {
            request_id: 46,
            locator: groove::large_values::Locator::random(),
            expected_hash: [46; 32],
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        },
    );
    let upstream_id = resolver.take_outbound(1)[0].request_id;
    assert_eq!(retained_relay_obligations(&resolver.state.borrow()), 1);
    resolver.complete(ChunkResponseEntry {
        request_id: upstream_id,
        result: ChunkResponse::Found(vec![46]),
    });
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        1,
        "completion transfers rather than releases the relay reservation"
    );
    let response = subscriber
        .take_outbound(1)
        .expect("completed response is queued");
    assert_eq!(retained_relay_obligations(&resolver.state.borrow()), 1);
    subscriber.acknowledge_outbound(&response);
    assert_eq!(retained_relay_obligations(&resolver.state.borrow()), 0);
}

#[test]
fn disconnect_mid_batch_stops_later_lookup_and_drops_the_resumed_result() {
    let schema = schema();
    let server = open_db(0x44, AuthorSubject::SYSTEM, &schema);
    let (_client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let resolver = server.node.chunk_resolver.clone();
    let (local_chunks, storage) = deferred_local_chunk_reader();
    let pump = PeerIoPump::new(
        resolver.clone(),
        local_chunks,
        subscriber.borrow().connection_epoch,
        PeerIoPumpRole::Subscriber,
    );
    subscriber.borrow_mut().auxiliary_pump = pump.clone();
    let requests = [44_u64, 45]
        .into_iter()
        .map(|request_id| ChunkRequestEntry {
            request_id,
            locator: groove::large_values::Locator::random(),
            expected_hash: [0x44; 32],
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        })
        .collect();
    let mut routing = Box::pin(pump.route_incoming(SyncMessage::ChunkRequestBatch(
        ChunkRequestBatch { requests },
    )));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(routing.as_mut().poll(&mut context), Poll::Pending));
    assert!(server.detach_connection(&subscriber));
    assert!(pump.is_disconnected());
    storage.release(Err(groove::chunks::ChunkStorageError::Unavailable));
    assert!(matches!(
        routing.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    let state = resolver.state.borrow();
    assert_eq!(retained_relay_obligations(&state), 0);
    assert!(state.relay_responses.get(&pump.connection).is_none());
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
            locator: groove::large_values::Locator::random(),
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

/// A chunk read sent by `alice` remains live when her upstream disconnects
/// before `bob`'s replacement link registers; `bob` receives the same demand
/// and only his response completes it.
///
/// ```text
/// reader ──request──► alice ──disconnect──► bob ──response──► reader
/// ```
#[test]
fn a_later_registered_upstream_retries_demand_drained_by_a_disconnected_predecessor() {
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
            12,
            PeerIoPumpRole::Upstream,
        );
        let request = groove::chunks::ChunkRequest {
            object_hash: [7; 32],
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
        let successor = PeerIoPump::new(
            resolver.clone(),
            database.local_chunk_reader(),
            13,
            PeerIoPumpRole::Upstream,
        );
        let retried_id = match successor.take_outbound(64).unwrap() {
            SyncMessage::ChunkRequestBatch(batch) => batch.requests[0].request_id,
            _ => unreachable!(),
        };
        assert_eq!(
            retried_id, request_id,
            "reconnect keeps the demand correlation id"
        );

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
                    request_id: retried_id,
                    result: ChunkResponse::Found(vec![2]),
                }],
            }))
            .await
            .unwrap();
        assert_eq!(pending.await.unwrap().as_ref(), &[2]);
    });
}
