//! Internal lifecycle tests: controlled storage suspension and consuming the
//! Groove facade are below the Jazz client API boundary.
use super::*;

/// Alice replaces a runtime while its initial table scan is cold. Cancelling
/// that read must release its internal storage owner without resuming the scan.
#[futures_test::test]
async fn storage_extraction_cancels_cold_hydration() {
    for prepare in [false, true] {
        let (storage, control) = TestStorage::controlled(&["albums"]);
        let mut database = Database::new(albums_schema(), storage.clone())
            .await
            .unwrap();
        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(1), Value::String("retained".into())],
        );
        database.commit_batch(batch).await.unwrap();
        storage.evict_all();
        control.pause_on(TestStorageOperation::ScanOpen);
        let subscription = database
            .subscribe_one_sink(GraphBuilder::table("albums"))
            .await
            .unwrap();
        assert!(database.has_pending_progress());
        if prepare {
            database.prepare_for_storage_extraction().await.unwrap();
        }
        let storage = database.into_storage();
        assert!(matches!(
            subscription.try_recv(),
            Err(TryRecvError::Disconnected)
        ));
        control.resume();
        let mut reopened = Database::new(albums_schema(), storage).await.unwrap();
        let rows = reopened
            .subscribe_one_sink(GraphBuilder::table("albums"))
            .await
            .unwrap();
        reopened.drive_progress().await.unwrap();
        assert_eq!(rows.recv().unwrap().deltas.len(), 1);
    }
}

/// Alice's applied write still belongs to its persistence owner. Bob's rebuild
/// request must leave that owner and the live database usable until settlement.
#[futures_test::test]
async fn storage_extraction_rejects_unsettled_publication_without_poisoning() {
    let storage = MemoryStorage::new(&["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("durable".into())],
    );
    let publication = database.apply_batch(batch).await.unwrap();
    assert!(matches!(
        database.prepare_for_storage_extraction().await,
        Err(Error::UnsettledPublications)
    ));
    assert!(!database.poisoned);
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    // Keep the settled handle alive: only its id remains meaningful.
    database.prepare_for_storage_extraction().await.unwrap();
    let mut reopened = Database::new(albums_schema(), database.into_storage())
        .await
        .unwrap();
    let rows = reopened
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert_eq!(rows.recv().unwrap().deltas.len(), 1);
}

// Internal fixture: directly publish a synthetic source delta so a persisted
// index write and cold checksum are independently observable across teardown.
async fn pending_durable_fixture(
    publication: bool,
) -> (Database, crate::storage::TestStorageControl, Subscription) {
    use bytes::Bytes;
    use std::cell::Cell;
    use std::collections::BTreeMap;

    #[derive(Clone)]
    struct DeferredResolver {
        chunks: Rc<BTreeMap<crate::chunks::ChunkRequest, Bytes>>,
        ready: Rc<Cell<bool>>,
    }

    impl crate::chunks::MissingChunkResolver for DeferredResolver {
        fn resolve(
            &self,
            request: crate::chunks::ChunkRequest,
        ) -> crate::chunks::ChunkFuture<'_, Result<Bytes, crate::chunks::ChunkError>> {
            let chunks = Rc::clone(&self.chunks);
            let ready = Rc::clone(&self.ready);
            Box::pin(async move {
                std::future::poll_fn(|_| {
                    ready.get().then_some(()).map_or(Poll::Pending, Poll::Ready)
                })
                .await;
                chunks
                    .get(&request)
                    .cloned()
                    .ok_or(crate::chunks::ChunkError::Unavailable)
            })
        }
    }

    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("objects_by_id", ["id"]))]);
    let (storage, control) = TestStorage::controlled(&schema.column_families());
    let mut database = Database::new(schema.clone(), storage).await.unwrap();
    database.set_auto_direct_family_enabled(false);
    database.set_chunk_storage(Rc::new(crate::chunks::MemoryChunkStorage::new()));
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![0x5a; crate::large_values::INLINE_VALUE_MAX_BYTES * 2],
    )
    .unwrap();
    let staged = if publication {
        let staged = database
            .stage_large_value_preparation(prepared.clone())
            .await
            .unwrap();
        database.set_chunk_storage(Rc::new(crate::chunks::MemoryChunkStorage::new()));
        Some(staged)
    } else {
        None
    };
    let resolver_chunks = prepared
        .staged_chunks
        .iter()
        .map(|chunk| {
            (
                crate::chunks::ChunkRequest {
                    object_hash: chunk.node_ref.object_hash.0,
                    locator: chunk.node_ref.locator,
                },
                Bytes::copy_from_slice(&chunk.encoded),
            )
        })
        .collect();
    let ready = Rc::new(Cell::new(false));
    database.set_missing_chunk_resolver(Rc::new(DeferredResolver {
        chunks: Rc::new(resolver_chunks),
        ready: Rc::clone(&ready),
    }));
    let graph = GraphBuilder::table("objects").streaming_checksum("payload", "checksum", 64, 64);
    let old = database.subscribe_one_sink(graph.clone()).await.unwrap();
    database.drive_progress().await.unwrap();
    assert!(old.recv().unwrap().is_empty());

    let objects = schema.table("objects").unwrap().record_schema();
    if let Some(staged) = staged {
        let mut batch = database.open_batch();
        batch.insert(
            "objects",
            vec![Value::U64(1), Value::Large(Box::new(staged.value_ref))],
        );
        batch.accept_large_value(staged.id);
        database.commit_batch(batch).await.unwrap();
    } else {
        database
            .ivm_runtime
            .tick_resident_staged(
                vec![TableDelta {
                    variant_tag: 0,
                    table: "objects".to_owned(),
                    descriptor: objects.clone(),
                    deltas: vec![RecordDelta {
                        record: objects
                            .create(&[
                                Value::U64(1),
                                Value::Large(Box::new(prepared.value_ref.clone())),
                            ])
                            .unwrap()
                            .into(),
                        weight: 1,
                    }],
                }],
                OwnedStorage::new(Rc::clone(&database.storage)),
                false,
                None,
            )
            .await
            .unwrap();
    }
    assert!(
        database.has_pending_progress(),
        "checksum must park an incremental evaluation"
    );
    (database, control, old)
}

/// Alice's incremental checksum is waiting for a chunk. Bob replaces the
/// runtime after its last subscriber leaves; the captured index write still
/// reaches storage without the chunk becoming available.
#[futures_test::test]
async fn storage_extraction_preserves_subscriberless_incremental_writes() {
    let (mut database, control, subscription) = pending_durable_fixture(false).await;
    assert!(database.unsubscribe(subscription.id()));
    assert!(database.has_pending_progress());
    control.take_observed();
    control.pause_on(TestStorageOperation::WriteMany);
    let mut prepare = Box::pin(database.prepare_for_storage_extraction());
    assert!(futures::poll!(prepare.as_mut()).is_pending());
    assert_eq!(
        control.take_observed(),
        vec![TestStorageOperation::WriteMany]
    );
    control.resume();
    prepare.await.unwrap();
    assert!(
        !database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    let _storage = database.into_storage();
}

/// Cancellation of Bob's teardown cannot allow Alice to use a partially
/// retired runtime or silently extract its unfinished durable write.
#[futures_test::test]
async fn cancelled_storage_extraction_leaves_the_database_unusable() {
    let (mut database, control, _subscription) = pending_durable_fixture(false).await;
    control.pause_on(TestStorageOperation::WriteMany);
    let mut prepare = Box::pin(database.prepare_for_storage_extraction());
    assert!(futures::poll!(prepare.as_mut()).is_pending());
    drop(prepare);
    assert!(matches!(
        database
            .subscribe_one_sink(GraphBuilder::table("objects"))
            .await,
        Err(Error::DatabasePoisoned)
    ));
    assert!(matches!(
        database.prepare_for_storage_extraction().await,
        Err(Error::DatabasePoisoned)
    ));
    assert!(database.ivm_runtime.has_pending_storage_writes());
    control.resume();
}

/// An explicit storage failure during Bob's rebuild leaves Alice's runtime
/// unusable and keeps the failed durable obligation from being discarded.
#[futures_test::test]
async fn failed_storage_extraction_preserves_the_failure_boundary() {
    let (mut database, control, _subscription) = pending_durable_fixture(false).await;
    control.fail_next(TestStorageOperation::WriteMany);
    assert!(database.prepare_for_storage_extraction().await.is_err());
    assert!(database.poisoned);
    assert!(database.ivm_runtime.has_pending_storage_writes());
}

/// Alice's database owner waits for external publication ownership to finish.
/// Every terminal outcome wakes it; successful persistence alone is not yet
/// settlement, and repeated registration retains only the latest owner wake.
#[futures_test::test]
async fn publication_settlement_wakes_owner_on_success_failure_and_abandonment() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct WakeCount(AtomicUsize);
    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    for case in [
        "settle",
        "failure",
        "cancel",
        "drop_applied",
        "drop_receipt",
        "drop_host_guard",
    ] {
        let (storage, control) = TestStorage::controlled(&["albums"]);
        let mut database = Database::new(albums_schema(), storage).await.unwrap();
        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(1), Value::String("wake owner".into())],
        );
        let publication = database.apply_batch(batch).await.unwrap();
        let old = Arc::new(WakeCount(AtomicUsize::new(0)));
        let latest = Arc::new(WakeCount(AtomicUsize::new(0)));
        assert!(
            database
                .wait_for_publication_settlement(Some(&futures::task::waker(old.clone())))
                .unwrap()
        );
        assert!(
            database
                .wait_for_publication_settlement(Some(&futures::task::waker(latest.clone())))
                .unwrap()
        );
        match case {
            "settle" => {
                let persisted = publication.persist().await;
                assert_eq!(
                    latest.0.load(Ordering::SeqCst),
                    0,
                    "persistence still needs owner settlement"
                );
                database.finish_persistence(persisted).unwrap();
            }
            "failure" => {
                control.fail_next(TestStorageOperation::WriteMany);
                assert!(
                    database
                        .finish_persistence(publication.persist().await)
                        .is_err()
                );
            }
            "cancel" => {
                control.pause_on(TestStorageOperation::WriteMany);
                let mut persistence = Box::pin(publication.persist());
                assert!(futures::poll!(persistence.as_mut()).is_pending());
                drop(persistence);
                control.resume();
            }
            "drop_applied" => drop(publication),
            "drop_receipt" => drop(publication.persist().await),
            "drop_host_guard" => drop(database.guard_host_application().unwrap()),
            _ => unreachable!(),
        }
        assert_eq!(
            old.0.load(Ordering::SeqCst),
            0,
            "obsolete owner wake replaced"
        );
        assert_eq!(latest.0.load(Ordering::SeqCst), 1, "terminal path: {case}");
        if case == "settle" {
            assert!(!database.wait_for_publication_settlement(None).unwrap());
        } else {
            assert!(
                matches!(
                    database.wait_for_publication_settlement(None),
                    Err(Error::DatabasePoisoned)
                ),
                "terminal path: {case}"
            );
        }
    }
}

/// Bob's auxiliary peer read can suspend independently of Alice's semantic
/// owner. Rebuilding must preserve that read and the shared storage lifecycle.
#[futures_test::test]
async fn runtime_rebuild_preserves_in_flight_local_chunk_read() {
    let (storage, control) = TestStorage::controlled(&albums_schema().column_families());
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![0x5a; crate::large_values::INLINE_VALUE_MAX_BYTES * 2],
    )
    .unwrap();
    let chunk = prepared.staged_chunks[0].clone();
    database
        .stage_large_value_preparation(prepared)
        .await
        .unwrap();
    let reader = database.local_chunk_reader();
    storage.evict_all();
    control.pause_on(TestStorageOperation::Get);
    let mut read = Box::pin(reader.get(chunk.node_ref.locator, chunk.node_ref.object_hash));
    assert!(futures::poll!(read.as_mut()).is_pending());
    let lifecycle = database.large_value_lifecycle.clone();
    database.prepare_for_storage_extraction().await.unwrap();
    database.rebuild(albums_schema()).unwrap();
    let mut rebuilt = database;
    assert!(std::sync::Arc::ptr_eq(
        &lifecycle,
        &rebuilt.large_value_lifecycle
    ));
    control.resume();
    assert_eq!(read.await.unwrap().as_ref(), chunk.encoded.as_slice());
    let rows = rebuilt
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    rebuilt.drive_progress().await.unwrap();
    assert!(rows.recv().unwrap().is_empty());
}

/// Alice queues two direct incremental writes behind the same cold terminal.
/// Bob's retirement must flush their index entries in temporal order exactly once.
#[futures_test::test]
async fn storage_extraction_flushes_multiple_queued_writes_in_order() {
    let (mut database, control, _subscription) = pending_durable_fixture(false).await;
    let descriptor = database.table("objects").unwrap().record_schema();
    database
        .ivm_runtime
        .tick_resident_staged(
            vec![TableDelta {
                variant_tag: 0,
                table: "objects".into(),
                descriptor: descriptor.clone(),
                deltas: vec![RecordDelta {
                    record: descriptor
                        .create(&[Value::U64(2), Value::Bytes(vec![0x62])])
                        .unwrap()
                        .into(),
                    weight: 1,
                }],
            }],
            OwnedStorage::new(database.storage.clone()),
            false,
            None,
        )
        .await
        .unwrap();
    let storage = database.storage.clone();
    let first = database
        .persisted_index_scan_prefix("objects", "objects_by_id", &[Value::U64(1)])
        .unwrap();
    let second = database
        .persisted_index_scan_prefix("objects", "objects_by_id", &[Value::U64(2)])
        .unwrap();
    control.take_observed();
    control.pause_on(TestStorageOperation::WriteMany);
    let mut prepare = Box::pin(database.prepare_for_storage_extraction());
    assert!(futures::poll!(prepare.as_mut()).is_pending());
    assert_eq!(
        control.take_observed(),
        vec![TestStorageOperation::WriteMany]
    );
    control.release_one();
    assert!(futures::poll!(prepare.as_mut()).is_pending());
    assert_eq!(
        storage
            .prefix("indices".into(), first.clone())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        storage
            .prefix("indices".into(), second.clone())
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        control
            .take_observed()
            .iter()
            .filter(|operation| **operation == TestStorageOperation::WriteMany)
            .count(),
        1
    );
    control.resume();
    prepare.await.unwrap();
    assert_eq!(
        storage.prefix("indices".into(), first).await.unwrap().len(),
        1
    );
    assert_eq!(
        storage
            .prefix("indices".into(), second)
            .await
            .unwrap()
            .len(),
        1
    );
}
