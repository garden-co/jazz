//! Atomic batches, staged overlays, direct records, commit metrics, and poisoning.

use super::*;

use bytes::Bytes;
use std::cell::Cell;
use std::task::{Poll, Waker};

#[derive(Clone)]
struct CrashAfterChunkPut {
    storage: Rc<crate::chunks::MemoryChunkStorage>,
    fail_after_successes: Cell<Option<usize>>,
    successful_puts: Cell<usize>,
}

impl CrashAfterChunkPut {
    fn new(fail_after_successes: Option<usize>) -> Self {
        Self {
            storage: Rc::new(crate::chunks::MemoryChunkStorage::new()),
            fail_after_successes: Cell::new(fail_after_successes),
            successful_puts: Cell::new(0),
        }
    }

    fn len(&self) -> usize {
        self.storage.len()
    }
}

impl crate::chunks::ChunkKvStorage for CrashAfterChunkPut {
    fn get_exact(
        &self,
        locator: crate::large_values::Locator,
    ) -> crate::chunks::ChunkFuture<
        '_,
        Result<Option<(crate::large_values::ContentHash, Bytes)>, crate::chunks::ChunkStorageError>,
    > {
        crate::chunks::ChunkKvStorage::get_exact(&*self.storage, locator)
    }

    fn put_if_absent(
        &self,
        locator: crate::large_values::Locator,
        hash: crate::large_values::ContentHash,
        bytes: Bytes,
    ) -> crate::chunks::ChunkFuture<
        '_,
        Result<Option<(crate::large_values::ContentHash, Bytes)>, crate::chunks::ChunkStorageError>,
    > {
        if self
            .fail_after_successes
            .get()
            .is_some_and(|limit| self.successful_puts.get() >= limit)
        {
            return Box::pin(async {
                Err(crate::chunks::ChunkStorageError::Backend(
                    "injected crash after durable chunk put".to_owned(),
                ))
            });
        }
        self.successful_puts
            .set(self.successful_puts.get().saturating_add(1));
        crate::chunks::ChunkKvStorage::put_if_absent(&*self.storage, locator, hash, bytes)
    }

    fn delete_exact(
        &self,
        locator: crate::large_values::Locator,
        expected_hash: crate::large_values::ContentHash,
    ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkStorageError>> {
        crate::chunks::ChunkKvStorage::delete_exact(&*self.storage, locator, expected_hash)
    }
}

#[derive(Clone)]
struct BlockedChunkPut {
    storage: Rc<crate::chunks::MemoryChunkStorage>,
    blocked: Rc<Cell<bool>>,
    waiters: Rc<RefCell<Vec<Waker>>>,
}

impl BlockedChunkPut {
    fn new() -> Self {
        Self {
            storage: Rc::new(crate::chunks::MemoryChunkStorage::new()),
            blocked: Rc::new(Cell::new(true)),
            waiters: Rc::new(RefCell::new(Vec::new())),
        }
    }

    fn release(&self) {
        self.blocked.set(false);
        for waiter in std::mem::take(&mut *self.waiters.borrow_mut()) {
            waiter.wake();
        }
    }

    fn len(&self) -> usize {
        self.storage.len()
    }
}

impl crate::chunks::ChunkKvStorage for BlockedChunkPut {
    fn get_exact(
        &self,
        locator: crate::large_values::Locator,
    ) -> crate::chunks::ChunkFuture<
        '_,
        Result<Option<(crate::large_values::ContentHash, Bytes)>, crate::chunks::ChunkStorageError>,
    > {
        crate::chunks::ChunkKvStorage::get_exact(&*self.storage, locator)
    }

    fn put_if_absent(
        &self,
        locator: crate::large_values::Locator,
        hash: crate::large_values::ContentHash,
        bytes: Bytes,
    ) -> crate::chunks::ChunkFuture<
        '_,
        Result<Option<(crate::large_values::ContentHash, Bytes)>, crate::chunks::ChunkStorageError>,
    > {
        let blocked = self.blocked.clone();
        let waiters = self.waiters.clone();
        let storage = self.storage.clone();
        Box::pin(async move {
            std::future::poll_fn(|cx| {
                if blocked.get() {
                    waiters.borrow_mut().push(cx.waker().clone());
                    Poll::Pending
                } else {
                    Poll::Ready(())
                }
            })
            .await;
            crate::chunks::ChunkKvStorage::put_if_absent(&*storage, locator, hash, bytes).await
        })
    }

    fn delete_exact(
        &self,
        locator: crate::large_values::Locator,
        expected_hash: crate::large_values::ContentHash,
    ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkStorageError>> {
        crate::chunks::ChunkKvStorage::delete_exact(&*self.storage, locator, expected_hash)
    }
}

#[futures_test::test]
async fn staged_large_value_is_consumed_atomically_with_its_referencing_row() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage).await.unwrap();
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    database.set_chunk_storage(chunks.clone());
    let staged = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![9; crate::large_values::INLINE_VALUE_MAX_BYTES + 1],
        )
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref.clone())],
    );
    batch.accept_large_value(staged.id);
    database.commit_batch(batch).await.unwrap();

    let mut replay = database.open_batch();
    replay.insert(
        "objects",
        vec![Value::U64(2), Value::Large(staged.value_ref)],
    );
    replay.accept_large_value(staged.id);
    assert!(matches!(
        database.commit_batch(replay).await,
        Err(Error::InvalidLargeValueMetadata(_))
    ));

    let rejected = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![3; crate::large_values::INLINE_VALUE_MAX_BYTES + 1],
        )
        .await
        .unwrap();
    let before = chunks.len();
    assert!(
        database
            .evict_staged_large_value(rejected.id)
            .await
            .unwrap()
    );
    assert!(
        !database
            .evict_staged_large_value(rejected.id)
            .await
            .unwrap()
    );
    assert!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert!(chunks.len() < before);
}

#[futures_test::test]
async fn direct_consolidation_stages_a_derived_descriptor_with_reused_base_nodes() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks);

    let mut logical = vec![0x5a; crate::large_values::INLINE_VALUE_MAX_BYTES * 9];
    let staged = database
        .prepare_and_stage_large_value(crate::large_values::LargeValueKind::Bytes, &logical)
        .await
        .unwrap();
    let crate::large_values::TailAppendOutcome::Updated(with_tail) =
        crate::large_values::append_tail(&staged.value_ref, vec![0xa5]).unwrap()
    else {
        panic!("one-byte tail must remain below consolidation limits");
    };
    logical.push(0xa5);

    let consolidated = database
        .consolidate_and_stage_large_value(with_tail)
        .await
        .unwrap();

    assert!(consolidated.value_ref.edit_tail.is_empty());
    assert_eq!(
        database
            .read_large_value_range(
                &consolidated.value_ref,
                0..u64::try_from(logical.len()).unwrap(),
            )
            .await
            .unwrap(),
        logical
    );
}

#[futures_test::test]
async fn idempotent_restaging_reports_incoming_bytes_for_each_upload() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).await.unwrap();
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![4; crate::large_values::INLINE_VALUE_MAX_BYTES + 1],
    )
    .unwrap();

    let first = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let second = database
        .stage_large_value_preparation(prepared)
        .await
        .unwrap();

    assert!(first.accounting.encoded_bytes > 0);
    assert!(first.accounting.node_count > 0);
    assert_eq!(second.accounting, first.accounting);
}

#[futures_test::test]
async fn incomplete_push_upload_is_restart_persistent_and_reclaimable() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema.clone(), storage.clone())
        .await
        .unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![5; crate::large_values::INLINE_VALUE_MAX_BYTES + 1],
    )
    .unwrap();
    let upload_id = crate::large_values::StagedLargeValueId([0x55; 16]);
    database
        .stage_large_value_chunk_batch(
            upload_id,
            crate::large_values::LargeValueKind::Bytes,
            prepared.staged_chunks,
        )
        .await
        .unwrap();
    assert_eq!(
        database.pending_large_value_uploads().await.unwrap().len(),
        1
    );
    drop(database);

    let mut reopened = Database::new(schema, storage).await.unwrap();
    reopened.set_chunk_storage(chunks.clone());
    assert_eq!(
        reopened.pending_large_value_uploads().await.unwrap().len(),
        1
    );
    assert!(
        reopened
            .evict_pending_large_value_upload(upload_id)
            .await
            .unwrap()
    );
    assert!(
        reopened
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(chunks.len(), 0);
}

// This facade-level test is intentionally below Jazz's public mutation API:
// only a hostile peer can call the raw chunk/finalization split directly. It
// proves finalization is its own admission boundary rather than trusting the
// earlier staging call order.
#[futures_test::test]
async fn raw_finalization_rejects_dishonest_or_unrelated_descriptors_and_survives_reopen() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema.clone(), storage.clone())
        .await
        .unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::String,
        b"trusted upload",
    )
    .unwrap();
    let unrelated =
        crate::large_values::prepare(crate::large_values::LargeValueKind::String, b"other upload")
            .unwrap();
    let upload_id = crate::large_values::StagedLargeValueId([0x84; 16]);
    let unrelated_upload_id = crate::large_values::StagedLargeValueId([0x85; 16]);
    database
        .stage_large_value_chunk_batch(upload_id, prepared.value_ref.kind, prepared.staged_chunks)
        .await
        .unwrap();
    database
        .stage_large_value_chunk_batch(
            unrelated_upload_id,
            unrelated.value_ref.kind,
            unrelated.staged_chunks,
        )
        .await
        .unwrap();

    let mut dishonest_metrics = prepared.value_ref.clone();
    dishonest_metrics.byte_length += 1;
    dishonest_metrics.utf16_length = Some(dishonest_metrics.utf16_length.unwrap() + 1);
    let mut dishonest_hash = prepared.value_ref.clone();
    dishonest_hash.logical_hash = crate::large_values::ContentHash([0x21; 32]);
    let mut dishonest_tail = prepared.value_ref.clone();
    dishonest_tail.byte_length += 1;
    dishonest_tail.utf16_length = Some(dishonest_tail.utf16_length.unwrap() + 1);
    dishonest_tail
        .edit_tail
        .push(crate::large_values::ReplaceEdit {
            offset: prepared.value_ref.byte_length,
            delete_length: 0,
            insert_bytes: vec![0xff],
            utf16_offset: prepared.value_ref.utf16_length.unwrap(),
            delete_utf16_length: 0,
            insert_utf16_length: 1,
        });

    for descriptor in [
        dishonest_metrics,
        dishonest_hash,
        dishonest_tail,
        unrelated.value_ref,
    ] {
        assert!(
            database
                .finalize_large_value_upload(upload_id, descriptor)
                .await
                .is_err(),
            "a raw finalizer must reject every malformed or unrelated descriptor"
        );
    }
    assert_eq!(
        database.pending_large_value_uploads().await.unwrap().len(),
        2,
        "a rejected final descriptor leaves the real pending upload retryable"
    );
    assert!(database.staged_large_values().await.unwrap().is_empty());

    // The malicious attempts must not poison a durable upload. Reopening also
    // proves that the unbound journal was not accidentally promoted.
    drop(database);
    let mut reopened = Database::new(schema, storage).await.unwrap();
    reopened.set_chunk_storage(chunks);
    let staged = reopened
        .finalize_large_value_upload(upload_id, prepared.value_ref)
        .await
        .unwrap();
    assert!(
        reopened
            .pending_large_value_uploads()
            .await
            .unwrap()
            .iter()
            .all(|upload| upload.id != upload_id)
    );
    assert!(
        reopened
            .staged_large_values()
            .await
            .unwrap()
            .contains(&staged)
    );
}

// The chunk backend is deliberately separate from metadata storage here. Each
// injected backend error represents a process loss at the exact boundary after
// metadata intent is durable, but before the indicated next blob put returns.
#[futures_test::test]
async fn upload_intent_reclaims_crash_window_chunks_and_promotes_completed_uploads() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &(0..crate::large_values::INLINE_VALUE_MAX_BYTES * 8)
            .map(|index| (index % 251) as u8)
            .collect::<Vec<_>>(),
    )
    .unwrap();

    for (fail_after_successes, expected_chunks) in [(0, 0), (1, 1)] {
        let storage = MemoryStorage::new(&schema.column_families());
        let backend = Rc::new(CrashAfterChunkPut::new(Some(fail_after_successes)));
        let mut database = Database::new(schema.clone(), storage.clone())
            .await
            .unwrap();
        database.set_chunk_storage(Rc::new(crate::chunks::ManagedChunkStorage::new(
            backend.clone(),
        )));
        let upload_id = crate::large_values::StagedLargeValueId([fail_after_successes as u8; 16]);

        assert!(
            database
                .stage_large_value_chunk_batch(
                    upload_id,
                    prepared.value_ref.kind,
                    prepared.staged_chunks.clone(),
                )
                .await
                .is_err()
        );
        assert_eq!(backend.len(), expected_chunks);
        assert_eq!(
            database.pending_large_value_uploads().await.unwrap().len(),
            1
        );
        drop(database);

        let mut reopened = Database::new(schema.clone(), storage).await.unwrap();
        reopened.set_chunk_storage(Rc::new(crate::chunks::ManagedChunkStorage::new(
            backend.clone(),
        )));
        assert!(
            reopened
                .evict_pending_large_value_upload(upload_id)
                .await
                .unwrap()
        );
        assert!(
            reopened
                .reclaim_orphaned_large_value_chunks(usize::MAX)
                .await
                .unwrap()
                > 0
        );
        assert_eq!(backend.len(), 0);
    }

    let storage = MemoryStorage::new(&schema.column_families());
    let backend = Rc::new(CrashAfterChunkPut::new(None));
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(Rc::new(crate::chunks::ManagedChunkStorage::new(
        backend.clone(),
    )));
    let upload_id = crate::large_values::StagedLargeValueId([0x99; 16]);
    database
        .stage_large_value_chunk_batch(upload_id, prepared.value_ref.kind, prepared.staged_chunks)
        .await
        .unwrap();
    let staged = database
        .finalize_large_value_upload(upload_id, prepared.value_ref)
        .await
        .unwrap();
    assert!(
        database
            .pending_large_value_uploads()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(backend.len() > 0);
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        0,
        "a promoted staged receipt keeps its referenced chunks live"
    );
    assert!(
        database
            .staged_large_values()
            .await
            .unwrap()
            .contains(&staged)
    );
}

#[test]
fn eviction_and_reclamation_wait_for_an_inflight_blob_stage() {
    use futures::channel::oneshot;
    use futures::executor::LocalPool;
    use futures::task::LocalSpawnExt;

    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![3; crate::large_values::INLINE_VALUE_MAX_BYTES + 1],
    )
    .unwrap();
    let mut pool = LocalPool::new();
    let backend = Rc::new(BlockedChunkPut::new());
    let mut opened = pool
        .run_until(Database::new(
            schema.clone(),
            MemoryStorage::new(&schema.column_families()),
        ))
        .unwrap();
    opened.set_chunk_storage(Rc::new(crate::chunks::ManagedChunkStorage::new(
        backend.clone(),
    )));
    let database = Rc::new(opened);
    let upload_id = crate::large_values::StagedLargeValueId([0x73; 16]);
    let (stage_tx, stage_rx) = oneshot::channel();
    let stage_database = database.clone();
    pool.spawner()
        .spawn_local(async move {
            let _ = stage_tx.send(
                stage_database
                    .stage_large_value_chunk_batch(
                        upload_id,
                        prepared.value_ref.kind,
                        prepared.staged_chunks,
                    )
                    .await,
            );
        })
        .unwrap();
    pool.run_until_stalled();
    assert_eq!(backend.len(), 0);
    assert_eq!(
        pool.run_until(database.pending_large_value_uploads())
            .unwrap()
            .len(),
        1
    );

    let (evict_tx, mut evict_rx) = oneshot::channel();
    let evict_database = database.clone();
    pool.spawner()
        .spawn_local(async move {
            let _ = evict_tx.send(
                evict_database
                    .evict_pending_large_value_upload(upload_id)
                    .await,
            );
        })
        .unwrap();
    let (reclaim_tx, mut reclaim_rx) = oneshot::channel();
    let reclaim_database = database.clone();
    pool.spawner()
        .spawn_local(async move {
            let _ = reclaim_tx.send(
                reclaim_database
                    .reclaim_orphaned_large_value_chunks(usize::MAX)
                    .await,
            );
        })
        .unwrap();
    pool.run_until_stalled();
    assert!(evict_rx.try_recv().unwrap().is_none());
    assert!(reclaim_rx.try_recv().unwrap().is_none());

    backend.release();
    assert!(pool.run_until(stage_rx).unwrap().is_ok());
    assert!(pool.run_until(evict_rx).unwrap().unwrap());
    assert!(pool.run_until(reclaim_rx).unwrap().unwrap() > 0);
    assert_eq!(backend.len(), 0);
}

#[futures_test::test]
async fn orphan_reclamation_defers_for_active_chunk_requests_and_leases() {
    use crate::chunks::ChunkStorage;
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};

    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![8; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    assert!(database.evict_staged_large_value(staged.id).await.unwrap());

    let root_request = crate::chunks::ChunkRequest {
        object_hash: prepared.value_ref.root.object_hash.0,
        locator: prepared.value_ref.root.locator,
    };
    let provider_chunks = prepared.staged_chunks.iter().map(|chunk| {
        (
            crate::chunks::ChunkRequest {
                object_hash: chunk.node_ref.object_hash.0,
                locator: chunk.node_ref.locator,
            },
            bytes::Bytes::copy_from_slice(&chunk.encoded),
        )
    });
    let (provider, control) = crate::chunks::TestChunkProvider::controlled(provider_chunks);
    control.pause();
    let owned = crate::chunks::OwnedChunkProvider::new(Rc::new(provider));
    database.set_owned_chunk_provider(owned.clone());

    let mut pending = owned.get(root_request.clone());
    let waker = futures::task::noop_waker();
    let mut context = Context::from_waker(&waker);
    assert!(matches!(
        Pin::new(&mut pending).poll(&mut context),
        Poll::Pending
    ));
    assert_eq!(owned.cache_stats().active_requests, 1);
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        0
    );

    control.release_one();
    let lease = pending.await.unwrap();
    assert_eq!(owned.cache_stats().active_leases, 1);
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        0
    );
    drop(lease);

    assert!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert!(
        chunks
            .get(root_request.locator, prepared.value_ref.root.object_hash)
            .await
            .is_err()
    );
}

#[futures_test::test]
async fn shared_durable_root_is_reclaimed_only_after_its_last_physical_record() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks.clone());
    let staged = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![6; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
        )
        .await
        .unwrap();

    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref.clone())],
    );
    insert.insert(
        "objects",
        vec![Value::U64(2), Value::Large(staged.value_ref.clone())],
    );
    insert.accept_large_value(staged.id);
    database.commit_batch(insert).await.unwrap();
    let live_chunks = chunks.len();
    assert!(live_chunks > 0);

    let mut delete_first = database.open_batch();
    delete_first.delete("objects", PrimaryKeyValue::U64(1));
    database.commit_batch(delete_first).await.unwrap();
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        0
    );
    assert_eq!(chunks.len(), live_chunks);

    let mut delete_last = database.open_batch();
    delete_last.delete("objects", PrimaryKeyValue::U64(2));
    database.commit_batch(delete_last).await.unwrap();
    assert!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(chunks.len(), 0);
}

#[futures_test::test]
async fn root_first_upload_requests_only_authenticated_missing_frontier() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(Rc::new(crate::chunks::MemoryChunkStorage::new()));
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![5; crate::large_values::INLINE_VALUE_MAX_BYTES * 8],
    )
    .unwrap();

    let mut progress = database
        .begin_large_value_upload(prepared.value_ref.clone())
        .await
        .unwrap();
    assert_eq!(
        progress,
        crate::large_values::LargeValueUploadProgress::Missing(vec![
            prepared.value_ref.root.clone()
        ]),
        "the receiver cannot discover children before authenticating the root"
    );
    let concurrent_frontier = match database
        .begin_large_value_upload(prepared.value_ref.clone())
        .await
        .unwrap()
    {
        crate::large_values::LargeValueUploadProgress::Missing(nodes) => nodes,
        crate::large_values::LargeValueUploadProgress::Staged(_) => unreachable!(),
    };
    let unsolicited = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref != prepared.value_ref.root)
        .expect("fixture contains a descendant")
        .clone();
    assert!(matches!(
        database
            .continue_large_value_upload(prepared.value_ref.clone(), vec![unsolicited])
            .await,
        Err(Error::InvalidLargeValueMetadata(message))
            if message.contains("outside the authenticated missing frontier")
    ));
    while let crate::large_values::LargeValueUploadProgress::Missing(missing) = &progress {
        let missing = missing.clone();
        let chunks = missing
            .into_iter()
            .map(|node_ref| {
                prepared
                    .staged_chunks
                    .iter()
                    .find(|chunk| chunk.node_ref == node_ref)
                    .expect("frontier contains a reachable prepared node")
                    .clone()
            })
            .collect();
        progress = database
            .continue_large_value_upload(prepared.value_ref.clone(), chunks)
            .await
            .unwrap();
    }

    let first_claim = match progress {
        crate::large_values::LargeValueUploadProgress::Staged(staged) => staged,
        crate::large_values::LargeValueUploadProgress::Missing(_) => unreachable!(),
    };
    let stale_chunks = concurrent_frontier
        .into_iter()
        .map(|node_ref| {
            prepared
                .staged_chunks
                .iter()
                .find(|chunk| chunk.node_ref == node_ref)
                .expect("concurrent frontier contains a prepared node")
                .clone()
        })
        .collect();
    let concurrent_claim = match database
        .continue_large_value_upload(prepared.value_ref.clone(), stale_chunks)
        .await
        .unwrap()
    {
        crate::large_values::LargeValueUploadProgress::Staged(staged) => staged,
        crate::large_values::LargeValueUploadProgress::Missing(_) => {
            panic!("a stale matching batch must observe the concurrently completed tree")
        }
    };
    assert_ne!(first_claim.id, concurrent_claim.id);
    let second_claim = match database
        .begin_large_value_upload(prepared.value_ref)
        .await
        .unwrap()
    {
        crate::large_values::LargeValueUploadProgress::Staged(staged) => staged,
        crate::large_values::LargeValueUploadProgress::Missing(_) => {
            panic!("the complete deduplicated tree needs no retransmission")
        }
    };
    assert_ne!(first_claim.id, second_claim.id);
}

#[futures_test::test]
async fn bounded_upload_start_caps_new_pending_metadata_and_allows_resume() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).await.unwrap();
    let value_refs = (0_u8..3)
        .map(|seed| {
            crate::large_values::prepare(crate::large_values::LargeValueKind::Bytes, &[seed])
                .unwrap()
                .value_ref
        })
        .collect::<Vec<_>>();

    for value_ref in &value_refs[..2] {
        database
            .begin_large_value_upload_with_pending_limit(value_ref.clone(), 2)
            .await
            .unwrap();
    }
    database
        .begin_large_value_upload_with_pending_limit(value_refs[0].clone(), 2)
        .await
        .expect("an existing upload remains resumable at the limit");
    assert!(matches!(
        database
            .begin_large_value_upload_with_pending_limit(value_refs[2].clone(), 2)
            .await,
        Err(Error::PendingLargeValueUploadLimitExceeded { limit: 2 })
    ));
    assert_eq!(
        database.pending_large_value_uploads().await.unwrap().len(),
        2
    );
}

// This facade-level test is intentionally below Jazz's public mutation API:
// only the peer upload protocol can carry an untrusted physical descriptor.
// It proves that Groove rejects one before issuing a publishable staging claim.
#[futures_test::test]
async fn malformed_json_tail_upload_is_rejected_and_reclaimed_before_staging() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared =
        crate::large_values::prepare(crate::large_values::LargeValueKind::Json, b"[]").unwrap();
    let mut malformed = prepared.value_ref.clone();
    malformed.byte_length += 1;
    malformed.utf16_length = Some(malformed.utf16_length.unwrap() + 1);
    malformed.edit_tail.push(crate::large_values::ReplaceEdit {
        offset: 0,
        delete_length: 0,
        insert_bytes: b"x".to_vec(),
        utf16_offset: 0,
        delete_utf16_length: 0,
        insert_utf16_length: 1,
    });

    assert!(matches!(
        database
            .begin_large_value_upload(malformed.clone())
            .await
            .unwrap(),
        crate::large_values::LargeValueUploadProgress::Missing(_)
    ));
    assert!(matches!(
        database
            .continue_large_value_upload(malformed, prepared.staged_chunks)
            .await,
        Err(Error::IvmRuntime(
            crate::ivm::runtime::IvmRuntimeError::LargeValue(
                crate::large_values::Error::InvalidJson
            )
        ))
    ));
    assert!(
        database
            .pending_large_value_uploads()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(database.staged_large_values().await.unwrap().is_empty());
    assert!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(chunks.len(), 0);
}

// This facade-level test uses a deliberately malformed physical child because
// only the peer upload path receives untrusted pre-chunked bytes. The root
// remains authenticated while one requested child is a hash-valid invalid
// postcard, proving the whole supplied batch is checked before the first
// durable chunk put.
#[futures_test::test]
async fn malformed_later_upload_child_has_no_durable_partial_write_after_reopen() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let backend = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let managed = Rc::new(crate::chunks::ManagedChunkStorage::new(backend.clone()));
    let mut database = Database::new(schema.clone(), storage.clone())
        .await
        .unwrap();
    database.set_chunk_storage(managed);
    let mut prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![7; crate::large_values::INLINE_VALUE_MAX_BYTES * 8],
    )
    .unwrap();
    let root_index = prepared
        .staged_chunks
        .iter()
        .position(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap();
    let malformed = vec![0xff, 0x00, 0xff];
    let (root, mutated_children, valid_child_ref) = {
        let root = &mut prepared.staged_chunks[root_index];
        let crate::large_values::ChunkNode::Branch { children, .. } =
            postcard::from_bytes(&root.encoded).unwrap()
        else {
            panic!("large fixture must have a branch root");
        };
        assert!(children.len() >= 2, "fixture needs two requested children");
        let valid_child_ref = children[0].node_ref.clone();
        let mut mutated_children = children;
        mutated_children[1].node_ref = crate::large_values::NodeRef {
            object_hash: crate::large_values::object_hash(&malformed),
            locator: crate::large_values::Locator::random(),
        };
        root.encoded = postcard::to_allocvec(&crate::large_values::ChunkNode::Branch {
            format: crate::large_values::FORMAT_VERSION,
            children: mutated_children.clone(),
        })
        .unwrap();
        root.node_ref.object_hash = crate::large_values::object_hash(&root.encoded);
        (root.clone(), mutated_children, valid_child_ref)
    };
    prepared.value_ref.root = root.node_ref.clone();
    let valid_child = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == valid_child_ref)
        .unwrap()
        .clone();
    let malformed_child = crate::large_values::StagedChunk {
        node_ref: mutated_children[1].node_ref.clone(),
        encoded: malformed,
    };

    assert!(matches!(
        database
            .begin_large_value_upload(prepared.value_ref.clone())
            .await
            .unwrap(),
        crate::large_values::LargeValueUploadProgress::Missing(_)
    ));
    assert!(matches!(
        database
            .continue_large_value_upload(prepared.value_ref.clone(), vec![root.clone()])
            .await
            .unwrap(),
        crate::large_values::LargeValueUploadProgress::Missing(_)
    ));
    assert!(matches!(
        database
            .continue_large_value_upload(
                prepared.value_ref.clone(),
                vec![valid_child, malformed_child],
            )
            .await,
        Err(Error::IvmRuntime(
            crate::ivm::runtime::IvmRuntimeError::LargeValue(
                crate::large_values::Error::MalformedNode
            )
        ))
    ));
    assert!(
        database
            .pending_large_value_uploads()
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(backend.len(), 1, "the valid earlier child was never staged");
    drop(database);

    let mut reopened = Database::new(schema, storage).await.unwrap();
    reopened.set_chunk_storage(Rc::new(crate::chunks::ManagedChunkStorage::new(
        backend.clone(),
    )));
    assert!(
        reopened
            .pending_large_value_uploads()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        reopened
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(backend.len(), 0);
}

#[futures_test::test]
async fn utf8_boundary_tail_upload_is_rejected_and_reclaimed_before_staging() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared =
        crate::large_values::prepare(crate::large_values::LargeValueKind::String, "é".as_bytes())
            .unwrap();
    let mut malformed = prepared.value_ref.clone();
    malformed.byte_length += 1;
    malformed.utf16_length = Some(malformed.utf16_length.unwrap() + 1);
    malformed.edit_tail.push(crate::large_values::ReplaceEdit {
        offset: 1,
        delete_length: 0,
        insert_bytes: b"x".to_vec(),
        utf16_offset: 0,
        delete_utf16_length: 0,
        insert_utf16_length: 1,
    });

    assert!(matches!(
        database
            .begin_large_value_upload(malformed.clone())
            .await
            .unwrap(),
        crate::large_values::LargeValueUploadProgress::Missing(_)
    ));
    assert!(matches!(
        database
            .continue_large_value_upload(malformed, prepared.staged_chunks)
            .await,
        Err(Error::IvmRuntime(
            crate::ivm::runtime::IvmRuntimeError::LargeValue(
                crate::large_values::Error::InvalidUtf8
            )
        ))
    ));
    assert!(
        database
            .pending_large_value_uploads()
            .await
            .unwrap()
            .is_empty()
    );
    assert!(database.staged_large_values().await.unwrap().is_empty());
    assert!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap()
            > 0
    );
    assert_eq!(chunks.len(), 0);
}

#[futures_test::test]
async fn root_first_upload_resumes_from_the_persisted_authenticated_frontier() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let storage = MemoryStorage::new(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema.clone(), storage.clone())
        .await
        .unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![6; crate::large_values::INLINE_VALUE_MAX_BYTES * 8],
    )
    .unwrap();
    let root = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    let progress = database
        .continue_large_value_upload(prepared.value_ref.clone(), vec![root])
        .await
        .unwrap();
    assert!(matches!(
        progress,
        crate::large_values::LargeValueUploadProgress::Missing(ref missing)
            if !missing.contains(&prepared.value_ref.root)
    ));
    drop(database);

    let mut reopened = Database::new(schema, storage).await.unwrap();
    reopened.set_chunk_storage(chunks);
    let mut progress = reopened
        .begin_large_value_upload(prepared.value_ref.clone())
        .await
        .unwrap();
    assert!(matches!(
        progress,
        crate::large_values::LargeValueUploadProgress::Missing(ref missing)
            if !missing.contains(&prepared.value_ref.root)
    ));
    while let crate::large_values::LargeValueUploadProgress::Missing(missing) = progress {
        let batch = missing
            .into_iter()
            .map(|node_ref| {
                prepared
                    .staged_chunks
                    .iter()
                    .find(|chunk| chunk.node_ref == node_ref)
                    .unwrap()
                    .clone()
            })
            .collect();
        progress = reopened
            .continue_large_value_upload(prepared.value_ref.clone(), batch)
            .await
            .unwrap();
    }
    assert!(matches!(
        progress,
        crate::large_values::LargeValueUploadProgress::Staged(_)
    ));
}

#[futures_test::test]
async fn failed_persistence_does_not_retract_an_applied_subscription_delta() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(
        database
            .next_subscription(&subscription)
            .await
            .unwrap()
            .is_empty()
    );

    control.fail_next(TestStorageOperation::WriteMany);
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    assert_eq!(subscription.recv().unwrap().deltas.len(), 1);
    let persisted = applied.persist().await;
    assert!(database.finish_persistence(persisted).is_err());
}

#[futures_test::test]
async fn failed_persistence_releases_later_publications_with_an_error() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut first = database.open_batch();
    first.insert(
        "albums",
        vec![Value::U64(1), Value::String("Blue Train".to_owned())],
    );
    let first = database.apply_batch(first).await.unwrap();

    let mut second = database.open_batch();
    second.insert(
        "albums",
        vec![Value::U64(2), Value::String("Giant Steps".to_owned())],
    );
    let second = database.apply_batch(second).await.unwrap();

    control.fail_next(TestStorageOperation::WriteMany);
    let first = first.persist().await;
    let second = second.persist().await;

    assert!(database.finish_persistence(first).is_err());
    assert!(database.finish_persistence(second).is_err());
    assert_eq!(
        control
            .observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::WriteMany)
            .count(),
        1,
        "a publication behind a failed write must not reach storage",
    );
}

#[futures_test::test]
async fn commits_insert_update_and_delete_batches() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    assert!(batch.is_empty());
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        database
            .storage
            .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
            .await
            .unwrap(),
        Some(crate::records::encode_variant_record(
            0,
            &database
                .ivm_runtime
                .schema()
                .table("albums")
                .unwrap()
                .record_schema()
                .create(&[Value::U64(7), Value::String("Blue Train".to_owned())])
                .unwrap(),
        ))
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    let stored = database
        .storage
        .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
        .await
        .unwrap()
        .unwrap();
    let descriptor = database
        .ivm_runtime
        .schema()
        .table("albums")
        .unwrap()
        .record_schema();
    let stored = version_zero_payload(&stored);
    assert_eq!(
        descriptor.get(stored, "title").unwrap(),
        Value::String("Giant Steps".to_owned())
    );

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .storage
            .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
            .await
            .unwrap(),
        None
    );
}

#[futures_test::test]
async fn staged_batch_reads_observe_uncommitted_writes() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();

    let mut staged = database.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    assert_eq!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Blue Train".to_owned())]
    );
    staged.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    assert_eq!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Giant Steps".to_owned())]
    );
    staged.delete("albums", PrimaryKeyValue::U64(7));
    assert!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .await
            .unwrap()
            .is_empty()
    );
    staged.commit().await.unwrap();

    assert!(
        database
            .primary_key_scan("albums", &[Value::U64(7)])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records,
        0
    );
}

async fn vec_derived_primary_key_scan_raw(
    database: &Database,
    batch: &DatabaseBatch,
    table: &str,
    prefix: &[Value],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut key_prefix = Vec::new();
    for value in prefix {
        encode_primary_key_part(&mut key_prefix, value).unwrap();
    }
    let mut rows = database
        .primary_key_scan_raw(table, prefix)
        .await
        .unwrap()
        .into_iter()
        .map(EncodedKeyValue::into_parts)
        .collect::<std::collections::BTreeMap<_, _>>();
    for write in database
        .pending_writes_from_operations(&batch.operations)
        .unwrap()
    {
        if write.table() != table || !write.key().starts_with(&key_prefix) {
            continue;
        }
        match write {
            PendingTableWrite::Set { key, record, .. } => {
                rows.insert(key, record);
            }
            PendingTableWrite::Delete { key, .. } => {
                rows.remove(&key);
            }
        }
    }
    rows.into_iter().collect()
}

#[futures_test::test]
async fn staged_batch_storage_txn_handles_large_accumulated_batches() {
    let database = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..10_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }

    let rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(9_999)])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].record().get("title").unwrap(),
        Value::String("album-9999".to_owned())
    );
    assert_eq!(batch.txn_operations.borrow().len(), 10_000);
    assert_eq!(
        rows.iter()
            .cloned()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[Value::U64(9_999)]).await
    );

    let cached_rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
        .await
        .unwrap();
    assert_eq!(
        cached_rows[0].record().get("title").unwrap(),
        Value::String("album-42".to_owned())
    );

    batch.update(
        "albums",
        vec![Value::U64(42), Value::String("updated".to_owned())],
    );
    let updated = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
        .await
        .unwrap();
    assert_eq!(
        updated[0].record().get("title").unwrap(),
        Value::String("updated".to_owned())
    );
    assert_eq!(
        updated
            .iter()
            .cloned()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[Value::U64(42)]).await
    );

    batch.delete("albums", PrimaryKeyValue::U64(42));
    assert!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[])
            .await
            .unwrap()
            .len(),
        9_999
    );
    assert_eq!(batch.txn_indexed_operations.get(), batch.operations.len());
}

#[futures_test::test]
async fn primary_key_get_raw_observes_staged_overlay() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("stored-one".to_owned())],
    );
    seed.insert(
        "albums",
        vec![Value::U64(2), Value::String("stored-two".to_owned())],
    );
    database.commit_batch(seed).await.unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(1), Value::String("updated-one".to_owned())],
    );
    batch.delete("albums", PrimaryKeyValue::U64(2));
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("inserted-three".to_owned())],
    );

    let updated = database
        .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(1)])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.record().get("title").unwrap(),
        Value::String("updated-one".to_owned())
    );
    assert!(
        database
            .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(2)])
            .await
            .unwrap()
            .is_none()
    );
    let inserted = database
        .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(3)])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inserted.record().get("title").unwrap(),
        Value::String("inserted-three".to_owned())
    );
    assert_eq!(batch.txn_indexed_operations.get(), batch.operations.len());
}

#[futures_test::test]
async fn staged_batch_storage_txn_overlays_storage_for_prefix_scans() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("stored-one".to_owned())],
    );
    seed.insert(
        "albums",
        vec![Value::U64(2), Value::String("stored-two".to_owned())],
    );
    database.commit_batch(seed).await.unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(1), Value::String("staged-one".to_owned())],
    );
    batch.delete("albums", PrimaryKeyValue::U64(2));
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("staged-three".to_owned())],
    );

    let rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|row| row.record().get("title").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            Value::String("staged-one".to_owned()),
            Value::String("staged-three".to_owned())
        ]
    );
    assert_eq!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[])
            .await
            .unwrap()
            .into_iter()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[]).await
    );
}

#[futures_test::test]
async fn staged_batch_storage_txn_advances_only_new_operations() {
    let database = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..10_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(9_999)])
        .await
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 10_000);

    for id in 10_000..20_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(19_999)])
        .await
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 20_000);
    assert_eq!(batch.txn_operations.borrow().len(), 20_000);

    batch.update(
        "albums",
        vec![Value::U64(19_999), Value::String("tail-updated".to_owned())],
    );
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(19_999)])
        .await
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 20_001);
    assert_eq!(batch.txn_operations.borrow().len(), 20_001);
}

#[futures_test::test]
async fn staged_batch_commit_ticks_once_for_multiple_writes() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(
        database
            .next_subscription(&subscription)
            .await
            .unwrap()
            .is_empty()
    );

    let mut staged = database.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    staged.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    staged.commit().await.unwrap();

    let metrics = database.last_commit_metrics().unwrap();
    assert_eq!(metrics.tick.table_delta_records, 2);
    assert_eq!(metrics.tick.notifications_sent, 1);
    assert_eq!(metrics.tick.notification_records, 2);
    let mut observed = subscription.recv().unwrap().to_values().unwrap();
    observed.sort_by_key(|(values, _)| match values[0] {
        Value::U64(id) => id,
        _ => panic!("expected u64 id"),
    });
    assert_eq!(
        observed,
        vec![
            (
                vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
                1
            ),
            (
                vec![Value::U64(2), Value::String("Blue Train".to_owned())],
                1
            ),
        ]
    );
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
}

#[futures_test::test]
async fn staged_batch_commit_matches_one_shot_wrapper() {
    let mut staged_db = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let mut wrapper_db = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();

    let mut staged = staged_db.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    staged.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    staged.delete("albums", PrimaryKeyValue::U64(1));
    staged.commit().await.unwrap();

    let mut wrapper = wrapper_db.open_batch();
    wrapper.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    wrapper.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    wrapper.delete("albums", PrimaryKeyValue::U64(1));
    wrapper_db.commit_batch(wrapper).await.unwrap();

    assert_eq!(
        staged_db
            .primary_key_scan("albums", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.to_values())
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        wrapper_db
            .primary_key_scan("albums", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.to_values())
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    );
    assert_eq!(
        staged_db
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records,
        wrapper_db
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records
    );
    assert_eq!(
        staged_db.last_commit_metrics().unwrap().storage_writes,
        wrapper_db.last_commit_metrics().unwrap().storage_writes
    );
}

#[futures_test::test]
async fn direct_record_store_stores_ordered_records_independent_of_tables() {
    let schema = albums_schema().with_direct_record_store(DirectRecordStoreSchema::new(
        "streams",
        RecordDescriptor::new([
            ("namespace", ColumnType::String.clone()),
            ("path", ColumnType::String.clone()),
        ]),
        RecordDescriptor::new([("bytes", ColumnType::Bytes.clone())]),
    ));
    let column_families = schema.column_families();
    let storage = MemoryStorage::new(&column_families);
    let mut database = Database::new(schema.clone(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    assert!(
        database
            .next_subscription(&subscription)
            .await
            .unwrap()
            .is_empty()
    );

    {
        let store = database.direct_record_store("streams").unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ],
                &[Value::Bytes(b"two".to_vec())],
            )
            .await
            .unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/01".to_owned()),
                ],
                &[Value::Bytes(b"one".to_vec())],
            )
            .await
            .unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/03".to_owned()),
                ],
                &[Value::Bytes(b"three".to_vec())],
            )
            .await
            .unwrap();
        store
            .set(
                &[
                    Value::String("checkpoint".to_owned()),
                    Value::String("checkpoint".to_owned()),
                ],
                &[Value::Bytes(b"cp".to_vec())],
            )
            .await
            .unwrap();

        assert_eq!(
            store
                .get(&[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ])
                .await
                .unwrap()
                .unwrap()
                .get("bytes")
                .unwrap(),
            Value::Bytes(b"two".to_vec())
        );
        assert_eq!(
            store
                .range(
                    &[
                        Value::String("content".to_owned()),
                        Value::String("content/01".to_owned()),
                    ],
                    &[
                        Value::String("content".to_owned()),
                        Value::String("content/04".to_owned()),
                    ]
                )
                .await
                .unwrap()
                .into_iter()
                .map(|record| record.get("bytes").unwrap())
                .collect::<Vec<_>>(),
            vec![
                Value::Bytes(b"one".to_vec()),
                Value::Bytes(b"two".to_vec()),
                Value::Bytes(b"three".to_vec()),
            ],
        );
        assert_eq!(
            store
                .prefix(&[Value::String("content".to_owned())])
                .await
                .unwrap()
                .into_iter()
                .map(|record| record.get("bytes").unwrap())
                .collect::<Vec<_>>(),
            vec![
                Value::Bytes(b"one".to_vec()),
                Value::Bytes(b"two".to_vec()),
                Value::Bytes(b"three".to_vec()),
            ],
        );

        let raw_value = database
            .storage
            .get(
                "streams".to_owned(),
                PrimaryKeyValue::Composite(vec![
                    PrimaryKeyValue::String("content".to_owned()),
                    PrimaryKeyValue::String("content/01".to_owned()),
                ])
                .into_bytes(),
            )
            .await
            .unwrap()
            .unwrap();
        assert_eq!(raw_value, b"\0one");

        store
            .delete(&[
                Value::String("content".to_owned()),
                Value::String("content/02".to_owned()),
            ])
            .await
            .unwrap();
        assert!(
            store
                .get(&[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ])
                .await
                .unwrap()
                .is_none()
        );
    }
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    assert!(
        database
            .primary_key_scan("albums", &[])
            .await
            .unwrap()
            .is_empty()
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![(
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
            1
        )]
    );
    assert_eq!(
        database
            .direct_record_store("streams")
            .unwrap()
            .get(&[
                Value::String("checkpoint".to_owned()),
                Value::String("checkpoint".to_owned()),
            ])
            .await
            .unwrap()
            .unwrap()
            .get("bytes")
            .unwrap(),
        Value::Bytes(b"cp".to_vec())
    );
    assert_eq!(
        database
            .storage
            .get("albums".to_owned(), b"content/01".to_vec())
            .await
            .unwrap(),
        None
    );

    let storage = database.into_storage();
    let reopened = Database::new(schema, storage).await.unwrap();
    let store = reopened.direct_record_store("streams").unwrap();
    assert_eq!(
        store
            .prefix(&[Value::String("content".to_owned())])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.get("bytes").unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::Bytes(b"one".to_vec()),
            Value::Bytes(b"three".to_vec()),
        ],
    );
    assert_eq!(
        reopened
            .primary_key_scan("albums", &[Value::U64(7)])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Blue Train".to_owned())]
    );
}

async fn assert_direct_record_store_round_trips_array_of_record_values() {
    let child = RecordDescriptor::new([("id", ValueType::U64), ("title", ValueType::String)]);
    let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
        "rendered_results",
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([(
            "results",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        )]),
    ));
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).await.unwrap();
    let first = crate::records::OwnedRecord::new(
        child
            .create(&[Value::U64(1), Value::String("Kind of Blue".to_owned())])
            .unwrap(),
        child,
    );
    let second = crate::records::OwnedRecord::new(
        child
            .create(&[Value::U64(2), Value::String("A Love Supreme".to_owned())])
            .unwrap(),
        child,
    );
    let results = Value::Array(vec![Value::Record(first), Value::Record(second)]);
    let store = database.direct_record_store("rendered_results").unwrap();

    store
        .set(&[Value::U64(7)], std::slice::from_ref(&results))
        .await
        .unwrap();

    assert_eq!(
        store
            .get(&[Value::U64(7)])
            .await
            .unwrap()
            .unwrap()
            .get("results")
            .unwrap(),
        results
    );
}

async fn assert_direct_record_store_rejects_noncanonical_record_value_bytes_at_admission() {
    let child = RecordDescriptor::new([("maybe_id", ValueType::Nullable(Box::new(ValueType::U8)))]);
    let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
        "rendered_results",
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([(
            "results",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        )]),
    ));
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).await.unwrap();
    let store = database.direct_record_store("rendered_results").unwrap();
    // A fixed-width null reserves a zero payload byte; this child has a
    // noncanonical nonzero payload and must not reach durable storage.
    let noncanonical = crate::records::OwnedRecord::new(vec![0, 7], child);

    assert!(matches!(
        store
            .set(
                &[Value::U64(7)],
                &[Value::Array(vec![Value::Record(noncanonical)])],
            )
            .await,
        Err(Error::RecordEncoding(crate::records::Error::InvalidOffset))
    ));
    assert!(store.get(&[Value::U64(7)]).await.unwrap().is_none());
}

#[futures_test::test]
async fn direct_record_store_rejects_record_containing_durable_keys_at_schema_admission() {
    assert_direct_record_store_round_trips_array_of_record_values().await;
    assert_direct_record_store_rejects_noncanonical_record_value_bytes_at_admission().await;

    let child = RecordDescriptor::new([("id", ValueType::U64)]);
    for (name, key_type) in [
        ("direct_record", ValueType::Record(Box::new(child))),
        (
            "array_record",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        ),
        (
            "nullable_array_record",
            ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::Record(
                Box::new(child),
            ))))),
        ),
    ] {
        let schema =
            DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
                name,
                RecordDescriptor::new([("key", key_type)]),
                RecordDescriptor::new([("payload", ValueType::Bytes)]),
            ));
        let storage = MemoryStorage::new(&schema.column_families());

        assert!(matches!(
            Database::new(schema, storage).await,
            Err(Error::InvalidDirectRecordStoreKey(store)) if store == name
        ));
    }

    let scalar_schema =
        DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
            "scalar_key",
            RecordDescriptor::new([("id", ValueType::U64)]),
            RecordDescriptor::new([("payload", ValueType::Bytes)]),
        ));
    let scalar_storage = MemoryStorage::new(&scalar_schema.column_families());
    let scalar_database = Database::new(scalar_schema, scalar_storage).await.unwrap();
    let scalar_store = scalar_database.direct_record_store("scalar_key").unwrap();

    scalar_store
        .set(&[Value::U64(7)], &[Value::Bytes(b"allowed".to_vec())])
        .await
        .unwrap();
    assert_eq!(
        scalar_store
            .get(&[Value::U64(7)])
            .await
            .unwrap()
            .unwrap()
            .get("payload")
            .unwrap(),
        Value::Bytes(b"allowed".to_vec())
    );
}

#[futures_test::test]
async fn commit_metrics_split_storage_and_tick_work() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    database.set_tick_runtime_stats_enabled(true);
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let _initial = database.next_subscription(&subscription).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let metrics = database.last_commit_metrics().unwrap();
    assert_eq!(metrics.storage_write_count, 1);
    assert!(metrics.storage_write_bytes > 0);
    assert_eq!(metrics.tick.table_delta_records, 1);
    assert_eq!(metrics.tick.notifications_sent, 1);
    assert_eq!(metrics.tick.notification_records, 1);
    assert!(metrics.tick.runtime_stats.graph_nodes > 0);
}

#[futures_test::test]
async fn commit_metrics_split_storage_writes_by_jazz_destination() {
    async fn run(layout: StorageLayout) -> StorageWriteMetrics {
        let schema = DatabaseSchema::new([
            TableSchema::new(
                "jazz_docs_history",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                    ColumnSchema::new("tx_node_id", ColumnType::U64),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "row_uuid"],
            )),
            TableSchema::new(
                "jazz_docs_global_current",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                    ColumnSchema::new("tx_node_id", ColumnType::U64),
                    ColumnSchema::new("user_parent", ColumnType::Uuid),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")]))
            .with_index(IndexSchema::new("by_user_parent", ["user_parent"])),
            TableSchema::new(
                "jazz_docs_register_global_current",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")])),
            TableSchema::new(
                "jazz_global_changes",
                [
                    ColumnSchema::new("table_name", ColumnType::Bytes),
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("layer", ColumnType::Bytes),
                    ColumnSchema::new("global_time", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("table_name"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::bytes("layer"),
                PrimaryKeyColumn::integer("global_time", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new(
                "by_global_time",
                ["global_time", "table_name", "row_uuid", "layer"],
            )),
            TableSchema::new(
                "jazz_transactions",
                [
                    ColumnSchema::new("time", ColumnType::U64),
                    ColumnSchema::new("node_id", ColumnType::U64),
                    ColumnSchema::new("global_time", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new("by_global_time", ["global_time"])),
        ]);
        let column_families = layout.physical_column_families(schema.column_families());
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let storage = MemoryStorage::new(&refs);
        let mut database = Database::new_with_storage_layout(schema, storage, layout)
            .await
            .unwrap();
        let row_uuid = uuid(1);

        let mut batch = database.open_batch();
        batch.insert(
            "jazz_docs_history",
            vec![
                Value::Uuid(row_uuid),
                Value::U64(1),
                Value::U64(2),
                Value::Uuid(uuid(3)),
            ],
        );
        batch.insert(
            "jazz_docs_global_current",
            vec![
                Value::Uuid(row_uuid),
                Value::U64(1),
                Value::U64(2),
                Value::Uuid(uuid(3)),
            ],
        );
        batch.insert(
            "jazz_docs_register_global_current",
            vec![Value::Uuid(row_uuid), Value::U64(1)],
        );
        batch.insert(
            "jazz_global_changes",
            vec![
                Value::Bytes(b"docs".to_vec()),
                Value::Uuid(row_uuid),
                Value::Bytes(b"content".to_vec()),
                Value::U64(1),
            ],
        );
        batch.insert(
            "jazz_transactions",
            vec![Value::U64(1), Value::U64(2), Value::U64(1)],
        );
        database.commit_batch(batch).await.unwrap();

        database.last_commit_metrics().unwrap().storage_writes
    }

    let writes = run(StorageLayout::Identity).await;
    assert_eq!(writes.total.count, 9);
    assert_eq!(writes.history_rows.count, 1);
    assert_eq!(writes.history_indexes.count, 1);
    assert_eq!(writes.global_current_rows.count, 1);
    assert_eq!(writes.global_current_indexes.count, 1);
    assert_eq!(writes.register_global_current_rows.count, 1);
    assert_eq!(writes.global_changes_rows.count, 1);
    assert_eq!(writes.global_changes_indexes.count, 1);
    assert_eq!(writes.transactions_rows.count, 1);
    assert_eq!(writes.transactions_indexes.count, 1);
    assert_eq!(writes.other.count, 0);

    let class_writes = run(StorageLayout::jazz_class_v1()).await;
    assert_eq!(class_writes, writes);
}

// Same-batch consolidation and conflict behavior.

#[futures_test::test]
async fn same_key_writes_in_one_batch_emit_deltas_against_earlier_batch_writes() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let _initial = database.next_subscription(&subscription_id).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Giant Steps".into()], 1)]
    );
    let stored = database
        .storage
        .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .schema()
            .table("albums")
            .unwrap()
            .record_schema()
            .get(version_zero_payload(&stored), "title")
            .unwrap(),
        Value::String("Giant Steps".to_owned())
    );
}

#[futures_test::test]
async fn inserts_over_existing_primary_keys_are_rejected() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();
    database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    database
        .subscribe_one_sink(GraphBuilder::index("albums", "albums_by_title"))
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    let err = database.commit_batch(batch).await.unwrap_err();

    assert!(matches!(err, Error::DuplicatePrimaryKey { table, .. } if table == "albums"));
    let stored = database
        .storage
        .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .schema()
            .table("albums")
            .unwrap()
            .record_schema()
            .get(version_zero_payload(&stored), "title")
            .unwrap(),
        Value::String("Blue Train".to_owned())
    );
}

#[futures_test::test]
async fn inserts_over_primary_keys_created_earlier_in_the_same_batch_are_rejected() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    let err = database.commit_batch(batch).await.unwrap_err();

    assert!(matches!(err, Error::DuplicatePrimaryKey { table, .. } if table == "albums"));
    assert!(
        database
            .storage
            .get("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
            .await
            .unwrap()
            .is_none()
    );
}

#[futures_test::test]
async fn same_batch_same_key_operations_emit_only_the_consolidated_final_delta() {
    let storage = MemoryStorage::new(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .await
        .unwrap();
    let _initial = database.next_subscription(&subscription).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
            1
        )]
    );
}

#[futures_test::test]
async fn persistence_receipts_cannot_settle_another_database() {
    let mut first = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();
    let mut second = Database::new(albums_schema(), MemoryStorage::new(&["albums"]))
        .await
        .unwrap();

    let mut first_batch = first.open_batch();
    first_batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("first".to_owned())],
    );
    let first_applied = first.apply_batch(first_batch).await.unwrap();

    let mut second_batch = second.open_batch();
    second_batch.insert(
        "albums",
        vec![Value::U64(2), Value::String("second".to_owned())],
    );
    let second_applied = second.apply_batch(second_batch).await.unwrap();

    let foreign_persistence = first_applied.persist().await;
    assert!(matches!(
        second.finish_persistence(foreign_persistence),
        Err(Error::PublicationNotFound(PublicationId(1)))
    ));
    assert_eq!(first.durable_publication_frontier(), None);
    assert_eq!(second.durable_publication_frontier(), None);

    let second_persistence = second_applied.persist().await;
    assert_eq!(
        second.finish_persistence(second_persistence).unwrap(),
        PublicationId(1)
    );
    assert_eq!(
        second.durable_publication_frontier(),
        Some(PublicationId(1))
    );
    assert_eq!(
        second
            .query_graph(GraphBuilder::table("albums"))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(vec![Value::U64(2), Value::String("second".to_owned())], 1)]
    );
}
