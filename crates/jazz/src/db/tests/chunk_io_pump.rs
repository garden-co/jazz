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
    state
        .pending_by_chunk
        .values()
        .flat_map(|pending| &pending.waiters)
        .filter(|waiter| matches!(waiter, ChunkDemandWaiter::Relay { .. }))
        .count()
        + state.relay_responses.values().map(Vec::len).sum::<usize>()
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
    resolver.enqueue_relay_responses(
        23,
        (0..response_count).map(|request_id| ChunkResponseEntry {
            request_id,
            result: ChunkResponse::Found(chunk_bytes.clone()),
        }),
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
    resolver.enqueue_relay_responses(
        24,
        (0..response_count).map(|request_id| ChunkResponseEntry {
            request_id,
            result: ChunkResponse::Found(chunk_bytes.clone()),
        }),
    );

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
fn completed_relay_responses_hold_capacity_until_drained() {
    let resolver = PeerChunkResolver::default();
    let connection = 30;
    let request = groove::chunks::ChunkRequest {
        object_hash: [0x30; 32],
        locator: groove::large_values::Locator::random(),
    };
    for request_id in 0..MAX_PENDING_CHUNK_DEMANDS as u64 {
        resolver.enqueue_relay(
            connection,
            ChunkRequestEntry {
                request_id,
                locator: request.locator.clone(),
                expected_hash: request.object_hash,
                remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
            },
        );
    }
    let upstream_id = resolver.take_outbound(1)[0].request_id;
    resolver.complete(ChunkResponseEntry {
        request_id: upstream_id,
        result: ChunkResponse::Found(vec![0x30]),
    });
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_PENDING_CHUNK_DEMANDS
    );

    resolver.enqueue_relay(
        connection,
        ChunkRequestEntry {
            request_id: MAX_PENDING_CHUNK_DEMANDS as u64,
            locator: request.locator.clone(),
            expected_hash: request.object_hash,
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        },
    );
    let state = resolver.state.borrow();
    assert_eq!(
        retained_relay_obligations(&state),
        MAX_PENDING_CHUNK_DEMANDS,
        "undrained responses retain admission capacity"
    );
    assert!(
        !state.pending_by_chunk.contains_key(&request),
        "refill is rejected while completed responses own the budget"
    );
    drop(state);

    let responses = resolver.take_relay_responses(connection, usize::MAX);
    assert_eq!(responses.len(), MAX_PENDING_CHUNK_DEMANDS);
    resolver.enqueue_relay(
        connection,
        ChunkRequestEntry {
            request_id: MAX_PENDING_CHUNK_DEMANDS as u64 + 1,
            locator: request.locator.clone(),
            expected_hash: request.object_hash,
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        },
    );
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        1,
        "draining responses releases their admission capacity"
    );
}

#[test]
fn immediate_overload_responses_cannot_bypass_the_relay_obligation_bound() {
    let ordinary = PeerChunkResolver::default();
    for request_id in [1, 2] {
        ordinary.enqueue_relay(
            31,
            ChunkRequestEntry {
                request_id,
                locator: groove::large_values::Locator::random(),
                expected_hash: [0x31; 32],
                remaining_hops: 0,
            },
        );
    }
    assert_eq!(
        ordinary.take_relay_responses(31, usize::MAX),
        vec![
            ChunkResponseEntry {
                request_id: 1,
                result: ChunkResponse::Unavailable,
            },
            ChunkResponseEntry {
                request_id: 2,
                result: ChunkResponse::Unavailable,
            },
        ],
        "ordinary immediate responses preserve request correlation"
    );

    let resolver = PeerChunkResolver::default();
    let connection = 31;
    for request_id in 0..MAX_PENDING_CHUNK_DEMANDS as u64 {
        resolver.enqueue_relay(
            connection,
            ChunkRequestEntry {
                request_id,
                locator: groove::large_values::Locator::random(),
                expected_hash: [0x31; 32],
                remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
            },
        );
    }
    for request_id in MAX_PENDING_CHUNK_DEMANDS as u64..MAX_PENDING_CHUNK_DEMANDS as u64 + 64 {
        resolver.enqueue_relay(
            connection,
            ChunkRequestEntry {
                request_id,
                locator: groove::large_values::Locator::random(),
                expected_hash: [0x31; 32],
                remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
            },
        );
        resolver.enqueue_relay(
            connection,
            ChunkRequestEntry {
                request_id: request_id + 64,
                locator: groove::large_values::Locator::random(),
                expected_hash: [0x31; 32],
                remaining_hops: 0,
            },
        );
    }
    let state = resolver.state.borrow();
    assert_eq!(
        retained_relay_obligations(&state),
        MAX_PENDING_CHUNK_DEMANDS,
        "Retryable and zero-hop responses share the existing global budget"
    );
    assert!(
        state.relay_responses.get(&connection).is_none(),
        "overload responses are dropped instead of retained past capacity"
    );
}

#[test]
fn draining_restoring_and_disconnecting_transfer_relay_response_capacity_exactly() {
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
        32,
        PeerIoPumpRole::Subscriber,
    );
    resolver.enqueue_relay(
        32,
        ChunkRequestEntry {
            request_id: 32,
            locator: groove::large_values::Locator::random(),
            expected_hash: [0x32; 32],
            remaining_hops: 0,
        },
    );
    let response = pump.take_outbound(1).expect("one immediate response");
    assert_eq!(retained_relay_obligations(&resolver.state.borrow()), 0);
    pump.restore_outbound(response);
    assert_eq!(retained_relay_obligations(&resolver.state.borrow()), 1);

    let request = groove::chunks::ChunkRequest {
        object_hash: [0x32; 32],
        locator: groove::large_values::Locator::random(),
    };
    for request_id in 0..MAX_PENDING_CHUNK_DEMANDS as u64 - 1 {
        resolver.enqueue_relay(
            32,
            ChunkRequestEntry {
                request_id,
                locator: request.locator.clone(),
                expected_hash: request.object_hash,
                remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
            },
        );
    }
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_PENDING_CHUNK_DEMANDS
    );
    resolver.enqueue_relay(
        32,
        ChunkRequestEntry {
            request_id: MAX_PENDING_CHUNK_DEMANDS as u64,
            locator: request.locator.clone(),
            expected_hash: request.object_hash,
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        },
    );
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_PENDING_CHUNK_DEMANDS
    );

    drop(pump.take_outbound(1).expect("restored response drains"));
    resolver.enqueue_relay(
        32,
        ChunkRequestEntry {
            request_id: MAX_PENDING_CHUNK_DEMANDS as u64 + 1,
            locator: request.locator.clone(),
            expected_hash: request.object_hash,
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        },
    );
    assert_eq!(
        retained_relay_obligations(&resolver.state.borrow()),
        MAX_PENDING_CHUNK_DEMANDS
    );
    pump.disconnect();
    let state = resolver.state.borrow();
    assert_eq!(retained_relay_obligations(&state), 0);
    assert!(!state.pending_by_chunk.contains_key(&request));
    assert!(state.relay_responses.get(&32).is_none());
}

// This stays at the auxiliary-pump boundary because relay admission and
// hop-local request-id correlation are not observable through database APIs.
#[test]
fn duplicate_relay_chunk_waiters_are_bounded_and_release_capacity() {
    let schema = schema();
    let server = open_db(0x25, AuthorSubject::SYSTEM, &schema);
    let (_client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let pump = subscriber.borrow().io_pump();
    let resolver = server.node.chunk_resolver.clone();
    let connection = pump.connection;
    let request = groove::chunks::ChunkRequest {
        object_hash: [0x25; 32],
        locator: groove::large_values::Locator::random(),
    };
    let route = |pump: &PeerIoPump, request_ids: std::ops::Range<u64>| {
        for first in request_ids
            .clone()
            .step_by(crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES)
        {
            let end = (first + crate::protocol_limits::MAX_CHUNK_REQUEST_BATCH_ENTRIES as u64)
                .min(request_ids.end);
            crate::db::block_on(
                pump.route_incoming(SyncMessage::ChunkRequestBatch(ChunkRequestBatch {
                    requests: (first..end)
                        .map(|request_id| ChunkRequestEntry {
                            request_id,
                            locator: request.locator.clone(),
                            expected_hash: request.object_hash,
                            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
                        })
                        .collect(),
                })),
            )
            .unwrap();
        }
    };

    route(&pump, 0..MAX_PENDING_CHUNK_DEMANDS as u64 + 1);

    let upstream_id = resolver.take_outbound(1)[0].request_id;
    {
        let state = resolver.state.borrow();
        let pending = state
            .pending_by_chunk
            .get(&request)
            .expect("the coalesced upstream demand remains pending");
        assert_eq!(
            pending.waiters.len(),
            MAX_PENDING_CHUNK_DEMANDS,
            "duplicate relay waiters share the global pending-demand ceiling"
        );
        assert!(
            state.relay_responses.get(&connection).is_none(),
            "a full obligation budget does not retain an overload response"
        );
    }

    resolver.complete(ChunkResponseEntry {
        request_id: upstream_id,
        result: ChunkResponse::Found(vec![0x25]),
    });
    let responses = resolver.take_relay_responses(connection, usize::MAX);
    assert_eq!(responses.len(), MAX_PENDING_CHUNK_DEMANDS);
    for (request_id, response) in responses.iter().enumerate() {
        assert_eq!(
            response,
            &ChunkResponseEntry {
                request_id: request_id as u64,
                result: ChunkResponse::Found(vec![0x25]),
            },
            "ordinary relay fan-out preserves every distinct request id"
        );
    }

    route(
        &pump,
        MAX_PENDING_CHUNK_DEMANDS as u64..MAX_PENDING_CHUNK_DEMANDS as u64 + 1,
    );
    assert_eq!(
        resolver.state.borrow().pending_by_chunk[&request]
            .waiters
            .len(),
        1,
        "completing a demand releases relay-waiter admission capacity"
    );
    route(
        &pump,
        MAX_PENDING_CHUNK_DEMANDS as u64 + 1..MAX_PENDING_CHUNK_DEMANDS as u64 * 2,
    );
    assert_eq!(
        resolver.state.borrow().pending_by_chunk[&request]
            .waiters
            .len(),
        MAX_PENDING_CHUNK_DEMANDS
    );

    assert!(server.detach_connection(&subscriber));
    assert!(
        !resolver
            .state
            .borrow()
            .pending_by_chunk
            .contains_key(&request),
        "standard database detach removes every relay waiter owned by the session"
    );

    let (_replacement_client, replacement_transport) = duplex();
    let replacement = server.accept_subscriber(replacement_transport, AuthorSubject::SYSTEM);
    let replacement_pump = replacement.borrow().io_pump();
    route(&replacement_pump, 0..1);
    let state = resolver.state.borrow();
    assert_eq!(
        state.pending_by_chunk[&request].waiters.len(),
        1,
        "detaching a relay releases its global waiter capacity"
    );
    assert!(
        state
            .relay_responses
            .get(&replacement_pump.connection)
            .is_none(),
        "the replacement session is admitted rather than rejected"
    );
}

#[test]
fn detach_during_missing_chunk_lookup_cannot_restore_relay_demand() {
    let schema = schema();
    let server = open_db(0x26, AuthorSubject::SYSTEM, &schema);
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
    let request = ChunkRequestEntry {
        request_id: 26,
        locator: groove::large_values::Locator::random(),
        expected_hash: [0x26; 32],
        remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
    };
    let chunk = groove::chunks::ChunkRequest {
        locator: request.locator.clone(),
        object_hash: request.expected_hash,
    };
    let mut routing = Box::pin(pump.route_incoming(SyncMessage::ChunkRequestBatch(
        ChunkRequestBatch {
            requests: vec![request],
        },
    )));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(routing.as_mut().poll(&mut context), Poll::Pending));

    assert!(server.detach_connection(&subscriber));
    storage.release(Err(groove::chunks::ChunkStorageError::Unavailable));
    assert!(matches!(
        routing.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    let state = resolver.state.borrow();
    assert!(
        !state.pending_by_chunk.contains_key(&chunk),
        "a missing result that resumes after detach cannot restore relay demand"
    );
    assert!(
        state.relay_responses.get(&pump.connection).is_none(),
        "the detached session retains no auxiliary output"
    );
}

#[test]
fn detach_during_found_chunk_lookup_cannot_restore_relay_response() {
    let schema = schema();
    let server = open_db(0x27, AuthorSubject::SYSTEM, &schema);
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
    let request = ChunkRequestEntry {
        request_id: 27,
        locator: groove::large_values::Locator::random(),
        expected_hash: [0x27; 32],
        remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
    };
    let mut routing = Box::pin(pump.route_incoming(SyncMessage::ChunkRequestBatch(
        ChunkRequestBatch {
            requests: vec![request],
        },
    )));
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(routing.as_mut().poll(&mut context), Poll::Pending));

    assert!(server.detach_connection(&subscriber));
    storage.release(Ok(bytes::Bytes::from_static(b"found after detach")));
    assert!(matches!(
        routing.as_mut().poll(&mut context),
        Poll::Ready(Ok(()))
    ));
    let state = resolver.state.borrow();
    assert!(
        state.relay_responses.get(&pump.connection).is_none(),
        "a found result that resumes after detach cannot restore relay output"
    );
    assert!(
        state
            .pending_by_chunk
            .values()
            .all(|pending| pending.waiters.iter().all(|waiter| {
                !matches!(
                    waiter,
                    ChunkDemandWaiter::Relay { connection, .. }
                        if *connection == pump.connection
                )
            })),
        "the detached session retains no relay waiter"
    );
}

#[test]
fn detach_during_peer_tick_chunk_lookup_drops_missing_and_found_outcomes() {
    for found in [false, true] {
        let schema = schema();
        let server = open_db(
            if found { 0x29 } else { 0x28 },
            AuthorSubject::SYSTEM,
            &schema,
        );
        let storage = Rc::new(DeferredChunkStorage::default());
        crate::db::block_on(async {
            server
                .node
                .node
                .lock()
                .await
                .set_chunk_storage(storage.clone());
        });
        let (mut client_transport, server_transport) = duplex();
        let subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
        let resolver = server.node.chunk_resolver.clone();
        let pump = subscriber.borrow().io_pump();
        let request = ChunkRequestEntry {
            request_id: if found { 29 } else { 28 },
            locator: groove::large_values::Locator::random(),
            expected_hash: if found { [0x29; 32] } else { [0x28; 32] },
            remaining_hops: DEFAULT_CHUNK_FORWARD_HOPS,
        };
        let chunk = groove::chunks::ChunkRequest {
            locator: request.locator.clone(),
            object_hash: request.expected_hash,
        };
        client_transport
            .send(SyncMessage::ChunkRequestBatch(ChunkRequestBatch {
                requests: vec![request],
            }))
            .unwrap();

        let mut connection = subscriber.borrow_mut();
        let mut ticking = Box::pin(connection.tick());
        let waker = futures::task::noop_waker();
        let mut context = Context::from_waker(&waker);
        assert!(matches!(ticking.as_mut().poll(&mut context), Poll::Pending));

        pump.disconnect();
        storage.release(if found {
            Ok(bytes::Bytes::from_static(b"peer tick found after detach"))
        } else {
            Err(groove::chunks::ChunkStorageError::Unavailable)
        });
        assert!(matches!(
            ticking.as_mut().poll(&mut context),
            Poll::Ready(Ok(_))
        ));
        drop(ticking);
        drop(connection);

        let state = resolver.state.borrow();
        assert!(
            !state.pending_by_chunk.contains_key(&chunk),
            "peer tick cannot restore missing relay demand after detach"
        );
        assert!(
            state.relay_responses.get(&pump.connection).is_none(),
            "peer tick cannot queue auxiliary output after detach"
        );
        assert!(
            client_transport.try_recv().is_none(),
            "peer tick cannot send a found response after detach"
        );
    }
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
