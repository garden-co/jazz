//! Atomic batches, staged overlays, direct records, commit metrics, and poisoning.

use super::*;

use bytes::Bytes;
use std::cell::Cell;
use std::task::{Poll, Waker};

#[derive(Clone)]
struct FixtureChunkResolver {
    chunks: Rc<std::collections::BTreeMap<crate::chunks::ChunkRequest, Bytes>>,
}

impl crate::chunks::MissingChunkResolver for FixtureChunkResolver {
    fn resolve(
        &self,
        request: crate::chunks::ChunkRequest,
    ) -> crate::chunks::ChunkFuture<'_, Result<Bytes, crate::chunks::ChunkError>> {
        Box::pin(async move {
            self.chunks
                .get(&request)
                .cloned()
                .ok_or(crate::chunks::ChunkError::Unavailable)
        })
    }
}

/// The byte plane is deliberately separate from Groove metadata. This receipt
/// fails the metadata observer after the immutable byte has been staged, then
/// rebuilds the provider over the same metadata store. The pending marker must
/// cause exactly one retry; a regular byte-store hit alone is not evidence that
/// child/reference metadata exists.
#[futures_test::test]
async fn pending_remote_chunk_install_retries_after_provider_rebuild() {
    struct FailOnceObserver {
        attempts: Rc<Cell<usize>>,
    }

    impl crate::chunks::ChunkInstallObserver for FailOnceObserver {
        fn installed(
            &self,
            _node_ref: crate::large_values::NodeRef,
            _encoded: Bytes,
        ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkError>> {
            let attempts = Rc::clone(&self.attempts);
            Box::pin(async move {
                let attempt = attempts.get().saturating_add(1);
                attempts.set(attempt);
                if attempt == 1 {
                    Err(crate::chunks::ChunkError::Backend(
                        "injected post-staging metadata failure".to_owned(),
                    ))
                } else {
                    Ok(())
                }
            })
        }
    }

    let backing = MemoryStorage::new(&[LARGE_VALUE_METADATA_CF]);
    let metadata = Rc::new(
        LayoutStorage::new(backing, StorageLayout::Identity)
            .await
            .unwrap(),
    );
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![0x5a; crate::large_values::INLINE_VALUE_MAX_BYTES + 1],
    )
    .unwrap();
    let root = prepared.value_ref.root.clone();
    let bytes = Bytes::from(
        prepared
            .staged_chunks
            .iter()
            .find(|chunk| chunk.node_ref == root)
            .expect("prepared root is staged")
            .encoded
            .clone(),
    );
    let request = crate::chunks::ChunkRequest {
        object_hash: root.object_hash.0,
        locator: root.locator,
    };
    let resolver = Rc::new(FixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            request.clone(),
            bytes.clone(),
        )])),
    });
    let attempts = Rc::new(Cell::new(0));
    let provider = crate::chunks::StorageChunkProvider::with_resolver_observer_and_journal(
        chunks.clone(),
        resolver.clone(),
        Rc::new(FailOnceObserver {
            attempts: Rc::clone(&attempts),
        }),
        Rc::new(MetadataChunkInstallJournal {
            storage: Rc::downgrade(&metadata),
        }),
    );

    assert!(matches!(
        crate::chunks::ChunkProvider::get(&provider, request.clone()).await,
        Err(crate::chunks::ChunkError::Backend(message))
            if message.contains("post-staging metadata failure")
    ));
    assert_eq!(
        crate::chunks::ChunkStorage::get(
            chunks.as_ref(),
            request.locator,
            crate::large_values::ContentHash(request.object_hash),
        )
        .await
        .unwrap(),
        bytes,
        "the immutable blob is resident before metadata installation succeeds"
    );

    let lifecycle = Arc::new(AsyncMutex::new(()));
    let reopened = crate::chunks::StorageChunkProvider::with_resolver_observer_and_journal(
        chunks,
        resolver,
        Rc::new(MetadataChunkInstallObserver {
            storage: Rc::downgrade(&metadata),
            lifecycle: Arc::downgrade(&lifecycle),
            resident_install: None,
        }),
        Rc::new(MetadataChunkInstallJournal {
            storage: Rc::downgrade(&metadata),
        }),
    );
    assert_eq!(
        crate::chunks::ChunkProvider::get(&reopened, request.clone()).await,
        Ok(bytes)
    );
    assert_eq!(
        attempts.get(),
        1,
        "the first post-staging observer failed once"
    );
    assert!(
        metadata
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&root).unwrap(),
            )
            .await
            .unwrap()
            .is_some(),
        "the reopened retry installs the actual Groove node metadata"
    );
    assert!(
        !crate::chunks::ChunkInstallJournal::is_pending(
            &MetadataChunkInstallJournal {
                storage: Rc::downgrade(&metadata),
            },
            crate::large_values::NodeRef {
                object_hash: crate::large_values::ContentHash(request.object_hash),
                locator: request.locator,
            },
        )
        .await
        .unwrap(),
        "a successful observer completion clears its durable recovery marker"
    );
}

#[derive(Clone)]
struct CountingFixtureChunkResolver {
    chunks: Rc<std::collections::BTreeMap<crate::chunks::ChunkRequest, Bytes>>,
    calls: Rc<Cell<usize>>,
}

impl crate::chunks::MissingChunkResolver for CountingFixtureChunkResolver {
    fn resolve(
        &self,
        request: crate::chunks::ChunkRequest,
    ) -> crate::chunks::ChunkFuture<'_, Result<Bytes, crate::chunks::ChunkError>> {
        self.calls.set(self.calls.get().saturating_add(1));
        Box::pin(async move {
            self.chunks
                .get(&request)
                .cloned()
                .ok_or(crate::chunks::ChunkError::Unavailable)
        })
    }
}

#[derive(Clone)]
struct DeferredFixtureChunkResolver {
    chunks: Rc<std::collections::BTreeMap<crate::chunks::ChunkRequest, Bytes>>,
    ready: Rc<Cell<bool>>,
}

impl crate::chunks::MissingChunkResolver for DeferredFixtureChunkResolver {
    fn resolve(
        &self,
        request: crate::chunks::ChunkRequest,
    ) -> crate::chunks::ChunkFuture<'_, Result<Bytes, crate::chunks::ChunkError>> {
        let chunks = Rc::clone(&self.chunks);
        let ready = Rc::clone(&self.ready);
        Box::pin(async move {
            std::future::poll_fn(|_| ready.get().then_some(()).map_or(Poll::Pending, Poll::Ready))
                .await;
            chunks
                .get(&request)
                .cloned()
                .ok_or(crate::chunks::ChunkError::Unavailable)
        })
    }
}

#[derive(Clone)]
struct DeferredErrorChunkProvider {
    ready: Rc<Cell<bool>>,
    message: String,
}

impl crate::chunks::ChunkProvider for DeferredErrorChunkProvider {
    fn get(
        &self,
        _request: crate::chunks::ChunkRequest,
    ) -> crate::chunks::ChunkFuture<'_, Result<Bytes, crate::chunks::ChunkError>> {
        let ready = Rc::clone(&self.ready);
        let message = self.message.clone();
        Box::pin(async move {
            std::future::poll_fn(|_| ready.get().then_some(()).map_or(Poll::Pending, Poll::Ready))
                .await;
            Err(crate::chunks::ChunkError::Backend(message))
        })
    }
}

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

#[futures_test::test]
async fn raw_finalization_rejects_forged_text_coordinates_and_partial_json_tail() {
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
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks);

    let text =
        crate::large_values::prepare(crate::large_values::LargeValueKind::String, b"abcd").unwrap();
    let mut forged_text = text.value_ref.clone();
    forged_text
        .edit_tail
        .push(crate::large_values::ReplaceEdit {
            offset: 1,
            delete_length: 1,
            insert_bytes: b"X".to_vec(),
            // The byte splice starts at UTF-16 offset 1, not 2. Shape and
            // final-value validation alone cannot detect this lie.
            utf16_offset: 2,
            delete_utf16_length: 1,
            insert_utf16_length: 1,
        });
    let forged_text: crate::large_values::LargeValueRef =
        postcard::from_bytes(&postcard::to_allocvec(&forged_text).expect("encode peer descriptor"))
            .expect("decode peer descriptor");
    let text_upload = crate::large_values::StagedLargeValueId([0xa1; 16]);
    database
        .stage_large_value_chunk_batch(text_upload, text.value_ref.kind, text.staged_chunks)
        .await
        .unwrap();
    assert!(
        database
            .finalize_large_value_upload(text_upload, forged_text)
            .await
            .is_err(),
        "staged text coordinates must describe the exact byte splice"
    );

    let json =
        crate::large_values::prepare(crate::large_values::LargeValueKind::Json, br#"{"a":1}"#)
            .unwrap();
    let mut forged_json = json.value_ref.clone();
    forged_json
        .edit_tail
        .push(crate::large_values::ReplaceEdit {
            offset: 5,
            delete_length: 1,
            insert_bytes: b"2".to_vec(),
            utf16_offset: 5,
            delete_utf16_length: 1,
            insert_utf16_length: 1,
        });
    let forged_json: crate::large_values::LargeValueRef =
        postcard::from_bytes(&postcard::to_allocvec(&forged_json).expect("encode peer descriptor"))
            .expect("decode peer descriptor");
    let json_upload = crate::large_values::StagedLargeValueId([0xa2; 16]);
    database
        .stage_large_value_chunk_batch(json_upload, json.value_ref.kind, json.staged_chunks)
        .await
        .unwrap();
    assert!(
        database
            .finalize_large_value_upload(json_upload, forged_json)
            .await
            .is_err(),
        "JSON edit tails admit only whole-value replacement"
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

// This internal storage-lifecycle receipt uses a non-canonical but valid shared
// DAG that an authenticated peer may upload. It proves physical ownership and
// finalization are graph-aware even though logical materialization preserves
// every repeated child occurrence.
#[futures_test::test]
async fn repeated_child_dag_finalizes_once_per_node_and_reclaims_without_leaks() {
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
    let prepared = crate::large_values::repeated_child_dag_fixture(
        crate::large_values::MAX_TREE_DEPTH,
        crate::large_values::BRANCH_MAX_CHILDREN,
    );
    let physical_nodes = prepared.staged_chunks.len();

    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    assert_eq!(chunks.len(), physical_nodes);
    for chunk in &prepared.staged_chunks {
        let encoded = database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&chunk.node_ref).unwrap(),
            )
            .await
            .unwrap()
            .expect("every finalized physical node has metadata");
        let metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(
            metadata.references, 1,
            "one active physical parent/root contributes one reference"
        );
        assert!(
            metadata.children.len() <= 1,
            "repeated logical child occurrences are one physical ownership edge"
        );
    }

    // A second descriptor-keyed upload sees a complete local graph. Its
    // missing-frontier and admission walks must terminate over physical nodes,
    // not enumerate the 64^32 logical occurrences.
    let second = match database
        .begin_large_value_upload(prepared.value_ref.clone())
        .await
        .unwrap()
    {
        crate::large_values::LargeValueUploadProgress::Staged(staged) => staged,
        crate::large_values::LargeValueUploadProgress::Missing(_) => {
            panic!("the complete resident graph has no missing frontier")
        }
    };

    assert!(database.evict_staged_large_value(staged.id).await.unwrap());
    assert!(database.evict_staged_large_value(second.id).await.unwrap());
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        physical_nodes
    );
    assert_eq!(chunks.len(), 0);
}

// Distinct active parents each own one physical edge to a shared descendant.
// The transition overlay must therefore count the leaf twice on activation and
// consume both counts before reclaiming it on deactivation.
#[futures_test::test]
async fn shared_child_dag_counts_distinct_parent_edges_and_reclaims_once() {
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
    let prepared = crate::large_values::shared_child_dag_fixture();
    assert_eq!(prepared.staged_chunks.len(), 4);

    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    for (index, chunk) in prepared.staged_chunks.iter().enumerate() {
        let encoded = database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&chunk.node_ref).unwrap(),
            )
            .await
            .unwrap()
            .expect("every finalized physical node has metadata");
        let metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(
            metadata.references,
            if index == 0 { 2 } else { 1 },
            "the shared leaf has one inbound edge from each distinct parent"
        );
    }

    assert!(database.evict_staged_large_value(staged.id).await.unwrap());
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        prepared.staged_chunks.len()
    );
    assert_eq!(chunks.len(), 0);
}

// Resolver installation happens before the complete graph is resident. This
// receipt proves an active newly authenticated branch recursively activates a
// distinct shared child once, even when the child occurs many times.
#[futures_test::test]
async fn resolver_installed_shared_dag_recursively_activates_and_reclaims_descendants() {
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
    let prepared = crate::large_values::repeated_child_dag_fixture(
        2,
        crate::large_values::BRANCH_MAX_CHILDREN,
    );
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
    database.set_missing_chunk_resolver(Rc::new(FixtureChunkResolver {
        chunks: Rc::new(resolver_chunks),
    }));

    let staged = crate::large_values::StagedLargeValue {
        id: crate::large_values::StagedLargeValueId([0x5d; 16]),
        value_ref: prepared.value_ref.clone(),
        accounting: Default::default(),
        created_at_ms: 1,
    };
    database
        .storage
        .write_many(vec![
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: staged_large_value_key(staged.id),
                value: postcard::to_allocvec(&staged).unwrap(),
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_root_key(&staged.value_ref.root).unwrap(),
                value: postcard::to_allocvec(&LargeValueRootReferences {
                    durable: 0,
                    staged: 1,
                    node_active: false,
                })
                .unwrap(),
            },
        ])
        .await
        .unwrap();

    let provider = database.owned_chunk_provider();
    for chunk in prepared.staged_chunks.iter().rev() {
        let request = crate::chunks::ChunkRequest {
            object_hash: chunk.node_ref.object_hash.0,
            locator: chunk.node_ref.locator,
        };
        drop(provider.get(request).await.unwrap());
    }
    for chunk in &prepared.staged_chunks {
        let encoded = database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&chunk.node_ref).unwrap(),
            )
            .await
            .unwrap()
            .expect("resolver-installed active node has metadata");
        let metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded).unwrap();
        assert_eq!(metadata.references, 1);
        assert!(metadata.children.len() <= 1);
    }

    assert!(database.evict_staged_large_value(staged.id).await.unwrap());
    assert_eq!(
        database
            .reclaim_orphaned_large_value_chunks(usize::MAX)
            .await
            .unwrap(),
        prepared.staged_chunks.len()
    );
    assert_eq!(chunks.len(), 0);
}

// A received branch can simultaneously activate a staged root and fill in the
// children of an already-active placeholder held by another root. Both
// ownership transitions are required: the new root adds one reference to the
// branch, while the pre-existing active placeholder adds one to each child.
#[futures_test::test]
async fn resolver_branch_activation_composes_with_active_placeholder_children() {
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
    let prepared = crate::large_values::repeated_child_dag_fixture(1, 1);
    let root_chunk = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .expect("fixture has a root branch");
    let root_node = crate::large_values::decode_node(
        prepared.value_ref.kind,
        root_chunk.node_ref.object_hash,
        &root_chunk.encoded,
    )
    .unwrap();
    let children = unique_large_value_children(&root_node);
    assert_eq!(children.len(), 1);
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
    database.set_missing_chunk_resolver(Rc::new(FixtureChunkResolver {
        chunks: Rc::new(resolver_chunks),
    }));

    database
        .storage
        .write_many(vec![
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_root_key(&prepared.value_ref.root).unwrap(),
                value: postcard::to_allocvec(&LargeValueRootReferences {
                    durable: 0,
                    staged: 1,
                    node_active: false,
                })
                .unwrap(),
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_node_key(&prepared.value_ref.root).unwrap(),
                value: postcard::to_allocvec(&LargeValueNodeReferences {
                    references: 1,
                    upload_references: 0,
                    children: Vec::new(),
                })
                .unwrap(),
            },
        ])
        .await
        .unwrap();

    let provider = database.owned_chunk_provider();
    drop(
        provider
            .get(crate::chunks::ChunkRequest {
                object_hash: prepared.value_ref.root.object_hash.0,
                locator: prepared.value_ref.root.locator,
            })
            .await
            .unwrap(),
    );

    let root: LargeValueNodeReferences = postcard::from_bytes(
        &database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&prepared.value_ref.root).unwrap(),
            )
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(root.references, 2, "the newly activated root is retained");
    assert_eq!(root.children, children);
    let child: LargeValueNodeReferences = postcard::from_bytes(
        &database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&children[0]).unwrap(),
            )
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(child.references, 1, "the placeholder discovers its child");
}

// Resident publications may be applied before earlier writes reach durable
// storage. Root accounting must compose those pending reference changes so
// two batches that accept the same root do not overwrite one another.
#[futures_test::test]
async fn pipelined_applied_batches_compose_large_value_root_references() {
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
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![7; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let first_staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let second_staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();

    let mut first_batch = database.open_batch();
    first_batch.insert(
        "objects",
        vec![Value::U64(1), Value::Large(first_staged.value_ref.clone())],
    );
    first_batch.accept_large_value(first_staged.id);
    let first = database.apply_batch(first_batch).await.unwrap();

    let mut second_batch = database.open_batch();
    second_batch.insert(
        "objects",
        vec![Value::U64(2), Value::Large(second_staged.value_ref.clone())],
    );
    second_batch.accept_large_value(second_staged.id);
    let second = database.apply_batch(second_batch).await.unwrap();

    let first = first.persist().await;
    let second = second.persist().await;
    database.finish_persistence(first).unwrap();
    database.finish_persistence(second).unwrap();

    let references: LargeValueRootReferences = postcard::from_bytes(
        &database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_root_key(&prepared.value_ref.root).unwrap(),
            )
            .await
            .unwrap()
            .unwrap(),
    )
    .unwrap();
    assert_eq!(references.durable, 2);
    assert_eq!(references.staged, 0);
    assert!(references.node_active);
}

/// Verifies Alice's last-root deletion retains lifecycle ownership while Bob's
/// resolver installs the missing root, so no descendant reference leaks.
///
/// alice ──apply delete──► pending publication ──persist──► durable metadata
/// bob ──get root────────► resolver observer ──wait────────► install metadata
#[futures_test::test]
async fn last_root_publication_blocks_descendant_install_until_its_refcount_write_is_durable() {
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
    let prepared = crate::large_values::repeated_child_dag_fixture(1, 4);
    let root_chunk = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    let child = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref != prepared.value_ref.root)
        .unwrap()
        .node_ref
        .clone();
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref.clone())],
    );
    insert.accept_large_value(staged.id);
    database.commit_batch(insert).await.unwrap();

    database
        .storage
        .write_many(vec![
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_node_key(&prepared.value_ref.root).unwrap(),
                value: postcard::to_allocvec(&LargeValueNodeReferences {
                    references: 1,
                    upload_references: 0,
                    children: Vec::new(),
                })
                .unwrap(),
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_node_key(&child).unwrap(),
                value: postcard::to_allocvec(&LargeValueNodeReferences::default()).unwrap(),
            },
        ])
        .await
        .unwrap();
    crate::chunks::ChunkStorage::delete(
        &*chunks,
        root_chunk.node_ref.locator,
        root_chunk.node_ref.object_hash,
    )
    .await
    .unwrap();
    database.set_missing_chunk_resolver(Rc::new(FixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root_chunk.node_ref.object_hash.0,
                locator: root_chunk.node_ref.locator,
            },
            Bytes::from(root_chunk.encoded.clone()),
        )])),
    }));

    let mut delete = database.open_batch();
    delete.delete("objects", PrimaryKeyValue::U64(1));
    let applied = database.apply_batch(delete).await.unwrap();

    let provider = database.owned_chunk_provider();
    let request = crate::chunks::ChunkRequest {
        object_hash: root_chunk.node_ref.object_hash.0,
        locator: root_chunk.node_ref.locator,
    };
    let mut installation = Box::pin(provider.get(request));
    assert!(futures::poll!(installation.as_mut()).is_pending());

    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
    installation.await.unwrap();

    let encoded = database
        .storage
        .get(
            LARGE_VALUE_METADATA_CF.to_owned(),
            large_value_node_key(&child).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded).unwrap();
    assert_eq!(metadata.references, 0);
}

/// Verifies a corrupt root record rejects Alice's batch before reserving a
/// publication, so Bob's following valid batch can persist without waiting on
/// a publication that will never exist.
///
/// alice ──corrupt root──► rejected delete
/// bob ──valid insert────► publication N ──persist──► durable row
#[futures_test::test]
async fn corrupt_large_value_root_does_not_leave_a_publication_hole() {
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
    let staged = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![9; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
        )
        .await
        .unwrap();
    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref.clone())],
    );
    insert.accept_large_value(staged.id);
    database.commit_batch(insert).await.unwrap();

    database
        .storage
        .write_many(vec![OwnedWriteOperation::Set {
            cf: LARGE_VALUE_METADATA_CF.to_owned(),
            key: large_value_root_key(&staged.value_ref.root).unwrap(),
            value: vec![0xff],
        }])
        .await
        .unwrap();
    let mut rejected = database.open_batch();
    rejected.delete("objects", PrimaryKeyValue::U64(1));
    assert!(matches!(
        database.apply_batch(rejected).await,
        Err(Error::InvalidLargeValueMetadata(message))
            if message.contains("cannot decode root references")
    ));

    let mut valid = database.open_batch();
    valid.insert("objects", vec![Value::U64(2), Value::Bytes(vec![2])]);
    let applied = database.apply_batch(valid).await.unwrap();
    let mut persistence = Box::pin(applied.persist());
    let persisted = match futures::poll!(persistence.as_mut()) {
        Poll::Ready(persisted) => persisted,
        Poll::Pending => panic!("valid persistence waited on a missing publication"),
    };
    database.finish_persistence(persisted).unwrap();
}

/// Verifies cancelling Alice while she waits for lifecycle serialization does
/// not reserve a publication, so Bob's following batch persists immediately.
///
/// holder ──lock──► alice apply ──wait/cancel──✗
/// holder ──unlock────────────────► bob apply/persist
#[futures_test::test]
async fn cancelled_lifecycle_wait_does_not_leave_a_publication_hole() {
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
    let staged = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![4; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
        )
        .await
        .unwrap();
    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref)],
    );
    insert.accept_large_value(staged.id);
    database.commit_batch(insert).await.unwrap();

    let blocker = database.large_value_lifecycle.clone().lock_owned().await;
    let next_publication = database.next_publication_id;
    let mut delete = database.open_batch();
    delete.delete("objects", PrimaryKeyValue::U64(1));
    let mut cancelled = Box::pin(database.apply_batch(delete));
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);
    assert_eq!(database.next_publication_id, next_publication);
    drop(blocker);

    let mut valid = database.open_batch();
    valid.insert("objects", vec![Value::U64(2), Value::Bytes(vec![2])]);
    let applied = database.apply_batch(valid).await.unwrap();
    let mut persistence = Box::pin(applied.persist());
    let persisted = match futures::poll!(persistence.as_mut()) {
        Poll::Ready(persisted) => persisted,
        Poll::Pending => panic!("valid persistence waited on a cancelled publication"),
    };
    database.finish_persistence(persisted).unwrap();
}

/// Verifies Bob's already-queued resolver installs child metadata before
/// Alice computes her last-root deletion, so Alice observes and retracts the
/// child reference instead of persisting a stale transition snapshot.
///
/// holder ──lock──► bob observer queue ──► alice delete queue
/// holder ──unlock─► bob install ─────────► alice compute/persist
#[futures_test::test]
async fn queued_resolver_before_last_root_delete_does_not_leak_child_reference() {
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
    let prepared = crate::large_values::repeated_child_dag_fixture(1, 4);
    let root_chunk = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    let child = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref != prepared.value_ref.root)
        .unwrap()
        .node_ref
        .clone();
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref.clone())],
    );
    insert.accept_large_value(staged.id);
    database.commit_batch(insert).await.unwrap();

    database
        .storage
        .write_many(vec![
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_node_key(&prepared.value_ref.root).unwrap(),
                value: postcard::to_allocvec(&LargeValueNodeReferences {
                    references: 1,
                    upload_references: 0,
                    children: Vec::new(),
                })
                .unwrap(),
            },
            OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_node_key(&child).unwrap(),
                value: postcard::to_allocvec(&LargeValueNodeReferences::default()).unwrap(),
            },
        ])
        .await
        .unwrap();
    crate::chunks::ChunkStorage::delete(
        &*chunks,
        root_chunk.node_ref.locator,
        root_chunk.node_ref.object_hash,
    )
    .await
    .unwrap();
    database.set_missing_chunk_resolver(Rc::new(FixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root_chunk.node_ref.object_hash.0,
                locator: root_chunk.node_ref.locator,
            },
            Bytes::from(root_chunk.encoded.clone()),
        )])),
    }));

    let blocker = database.large_value_lifecycle.clone().lock_owned().await;
    let provider = database.owned_chunk_provider();
    let mut installation = Box::pin(provider.get(crate::chunks::ChunkRequest {
        object_hash: root_chunk.node_ref.object_hash.0,
        locator: root_chunk.node_ref.locator,
    }));
    assert!(futures::poll!(installation.as_mut()).is_pending());

    let mut delete = database.open_batch();
    delete.delete("objects", PrimaryKeyValue::U64(1));
    let mut deletion = Box::pin(database.apply_batch(delete));
    assert!(futures::poll!(deletion.as_mut()).is_pending());
    drop(blocker);

    installation.await.unwrap();
    let applied = deletion.await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    let encoded = database
        .storage
        .get(
            LARGE_VALUE_METADATA_CF.to_owned(),
            large_value_node_key(&child).unwrap(),
        )
        .await
        .unwrap()
        .unwrap();
    let metadata: LargeValueNodeReferences = postcard::from_bytes(&encoded).unwrap();
    assert_eq!(metadata.references, 0);
}

/// Verifies Alice's resident tick may resolve and install a genuinely missing
/// chunk before lifecycle serialization is acquired. The counted resolver and
/// immediately published subscription delta prove the observer completed
/// inside the tick rather than taking an already-resident fast path.
///
/// alice ──insert large──► tick ──missing root──► resolver/observer ──► delta
///                                      └──────── lifecycle lock afterwards
#[futures_test::test]
async fn missing_chunk_observer_completes_during_tick_before_lifecycle_lock() {
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
        &vec![3; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let root = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    crate::chunks::ChunkStorage::delete(&*chunks, root.node_ref.locator, root.node_ref.object_hash)
        .await
        .unwrap();
    let resolver_calls = Rc::new(Cell::new(0));
    database.set_missing_chunk_resolver(Rc::new(CountingFixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
            },
            Bytes::from(root.encoded),
        )])),
        calls: Rc::clone(&resolver_calls),
    }));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("objects"))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref)],
    );
    insert.accept_large_value(staged.id);
    let applied = database.apply_batch(insert).await.unwrap();

    assert!(resolver_calls.get() > 0, "the tick invoked the resolver");
    let update = subscription.try_recv_with_publication().unwrap();
    assert_eq!(update.publication, Some(applied.publication()));
    assert_eq!(update.deltas.to_values().unwrap().len(), 1);

    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
}

/// A's unpersisted root transition owns lifecycle serialization while B's
/// resident tick resolves the same now-cold root. The observer must join A's
/// serialized resident pipeline instead of waiting for the mutex that A keeps
/// until the caller can persist it.
#[futures_test::test]
async fn sequential_cold_large_value_publications_do_not_deadlock_observer() {
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
    let first_prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![7; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let first_staged = database
        .stage_large_value_preparation(first_prepared)
        .await
        .unwrap();
    let second_prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![8; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let second_staged = database
        .stage_large_value_preparation(second_prepared.clone())
        .await
        .unwrap();
    let root = second_prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == second_prepared.value_ref.root)
        .unwrap()
        .clone();
    let resolver_calls = Rc::new(Cell::new(0));
    database.set_missing_chunk_resolver(Rc::new(CountingFixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
            },
            Bytes::from(root.encoded.clone()),
        )])),
        calls: Rc::clone(&resolver_calls),
    }));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("objects"))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut first = database.open_batch();
    first.insert(
        "objects",
        vec![Value::U64(1), Value::Large(first_staged.value_ref)],
    );
    first.accept_large_value(first_staged.id);
    let first = database.apply_batch(first).await.unwrap();
    assert!(database.large_value_publication_lifecycle_guard.is_some());
    assert_eq!(resolver_calls.get(), 0);
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .unwrap()
            .publication,
        Some(first.publication())
    );

    crate::chunks::ChunkStorage::delete(&*chunks, root.node_ref.locator, root.node_ref.object_hash)
        .await
        .unwrap();
    let mut second = database.open_batch();
    second.insert(
        "objects",
        vec![Value::U64(2), Value::Large(second_staged.value_ref)],
    );
    second.accept_large_value(second_staged.id);
    let mut second_application = Box::pin(database.apply_batch(second));
    let second = match futures::poll!(second_application.as_mut()) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("B waited on A's durability-held lifecycle mutex"),
    };
    drop(second_application);
    assert!(resolver_calls.get() > 0, "B's tick invoked the resolver");
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .expect("B's observer completed during its resident tick")
            .publication,
        Some(second.publication())
    );

    let first = first.persist().await;
    let second = second.persist().await;
    database.finish_persistence(first).unwrap();
    database.finish_persistence(second).unwrap();
}

/// The first publication has no predecessor lifecycle guard, but its cold
/// resolver can still resume after the table write is durable. That late
/// installation must not re-enter a self-held lifecycle mutex or disappear.
#[futures_test::test]
async fn first_cold_publication_persists_before_resolver_without_deadlock() {
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
        &vec![3; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let root = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    crate::chunks::ChunkStorage::delete(&*chunks, root.node_ref.locator, root.node_ref.object_hash)
        .await
        .unwrap();
    database
        .storage
        .delete(
            LARGE_VALUE_METADATA_CF.to_owned(),
            large_value_node_key(&root.node_ref).unwrap(),
        )
        .await
        .unwrap();
    let resolver_ready = Rc::new(Cell::new(false));
    database.set_missing_chunk_resolver(Rc::new(DeferredFixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
            },
            Bytes::from(root.encoded),
        )])),
        ready: Rc::clone(&resolver_ready),
    }));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("objects"))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut insert = database.open_batch();
    insert.insert(
        "objects",
        vec![Value::U64(1), Value::Large(staged.value_ref)],
    );
    insert.accept_large_value(staged.id);
    let applied = database.apply_batch(insert).await.unwrap();
    assert!(subscription.try_recv().is_err());
    let persisted = applied.persist().await;

    resolver_ready.set(true);
    database.flush().await.unwrap();
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .unwrap()
            .publication,
        Some(applied.publication())
    );
    database.finish_persistence(persisted).unwrap();
    assert!(
        database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&root.node_ref).unwrap(),
            )
            .await
            .unwrap()
            .is_some()
    );
}

/// A cold B evaluation may detach after its publication is assigned while its
/// install observer remains pending. Once B's snapshot is durable, a resumed
/// observer must commit its lifecycle writes as a durable follow-on operation.
#[futures_test::test]
async fn suspended_resident_chunk_install_joins_assigned_publication() {
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
    let first = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![1; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
        )
        .await
        .unwrap();
    let second_prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![2; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let second = database
        .stage_large_value_preparation(second_prepared.clone())
        .await
        .unwrap();
    let root = second_prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == second_prepared.value_ref.root)
        .unwrap()
        .clone();
    crate::chunks::ChunkStorage::delete(&*chunks, root.node_ref.locator, root.node_ref.object_hash)
        .await
        .unwrap();
    database
        .storage
        .delete(
            LARGE_VALUE_METADATA_CF.to_owned(),
            large_value_node_key(&root.node_ref).unwrap(),
        )
        .await
        .unwrap();
    let resolver_ready = Rc::new(Cell::new(false));
    database.set_missing_chunk_resolver(Rc::new(DeferredFixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
            },
            Bytes::from(root.encoded),
        )])),
        ready: Rc::clone(&resolver_ready),
    }));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("objects"))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut first_batch = database.open_batch();
    first_batch.insert(
        "objects",
        vec![Value::U64(1), Value::Large(first.value_ref)],
    );
    first_batch.accept_large_value(first.id);
    let first = database.apply_batch(first_batch).await.unwrap();
    assert!(database.large_value_publication_lifecycle_guard.is_some());
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .unwrap()
            .publication,
        Some(first.publication())
    );

    let mut second_batch = database.open_batch();
    second_batch.insert(
        "objects",
        vec![Value::U64(2), Value::Large(second.value_ref)],
    );
    second_batch.accept_large_value(second.id);
    let second = database.apply_batch(second_batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    // A cancelled queued persistence attempt must leave B retryable. Its
    // eventual successful attempt still snapshots before the resolver wakes.
    let mut cancelled = Box::pin(second.persist());
    assert!(futures::poll!(cancelled.as_mut()).is_pending());
    drop(cancelled);

    database.finish_persistence(first.persist().await).unwrap();
    database.finish_persistence(second.persist().await).unwrap();

    resolver_ready.set(true);
    database.flush().await.unwrap();
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .unwrap()
            .publication,
        Some(second.publication())
    );

    assert!(
        database
            .storage
            .get(
                LARGE_VALUE_METADATA_CF.to_owned(),
                large_value_node_key(&root.node_ref).unwrap(),
            )
            .await
            .unwrap()
            .is_some()
    );
}

/// A late lifecycle metadata write is part of publication durability, not an
/// ordinary query-local chunk failure. If it fails after B's table snapshot is
/// durable, database progress and the affected subscription both terminate.
#[futures_test::test]
async fn late_publication_metadata_write_failure_is_fatal_and_observable() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "objects",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("payload", ColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))]);
    let (storage, control) = TestStorage::controlled(&schema.column_families());
    let chunks = Rc::new(crate::chunks::MemoryChunkStorage::new());
    let mut database = Database::new(schema, storage).await.unwrap();
    database.set_chunk_storage(chunks.clone());
    let prepared = crate::large_values::prepare(
        crate::large_values::LargeValueKind::Bytes,
        &vec![6; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
    )
    .unwrap();
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    let root = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    crate::chunks::ChunkStorage::delete(&*chunks, root.node_ref.locator, root.node_ref.object_hash)
        .await
        .unwrap();
    database
        .storage
        .delete(
            LARGE_VALUE_METADATA_CF.to_owned(),
            large_value_node_key(&root.node_ref).unwrap(),
        )
        .await
        .unwrap();
    let resolver_ready = Rc::new(Cell::new(false));
    database.set_missing_chunk_resolver(Rc::new(DeferredFixtureChunkResolver {
        chunks: Rc::new(std::collections::BTreeMap::from([(
            crate::chunks::ChunkRequest {
                object_hash: root.node_ref.object_hash.0,
                locator: root.node_ref.locator,
            },
            Bytes::from(root.encoded),
        )])),
        ready: Rc::clone(&resolver_ready),
    }));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("objects"))
        .await
        .unwrap();
    assert!(
        database
            .next_subscription(&subscription)
            .await
            .unwrap()
            .is_empty()
    );

    let mut first = database.open_batch();
    first.insert("objects", vec![Value::U64(1), Value::Bytes(vec![1])]);
    let first = database.apply_batch(first).await.unwrap();
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .unwrap()
            .publication,
        Some(first.publication())
    );
    database.finish_persistence(first.persist().await).unwrap();

    let mut second = database.open_batch();
    second.insert(
        "objects",
        vec![Value::U64(2), Value::Large(staged.value_ref)],
    );
    second.accept_large_value(staged.id);
    let second = database.apply_batch(second).await.unwrap();
    assert!(subscription.try_recv().is_err());
    database.finish_persistence(second.persist().await).unwrap();

    // A cold install now journals before it stages bytes. Pause that first
    // write so this receipt can inject the failure at the *later* metadata
    // durability boundary it is intended to exercise, rather than turning a
    // pre-staging journal write into an unrelated query-local failure.
    control.take_observed();
    control.pause_on(TestStorageOperation::WriteMany);
    resolver_ready.set(true);
    let mut flush = Box::pin(database.flush());
    assert!(futures::poll!(flush.as_mut()).is_pending());
    assert_eq!(
        control.take_observed(),
        vec![TestStorageOperation::WriteMany]
    );
    control.release_one();
    assert!(futures::poll!(flush.as_mut()).is_pending());
    assert_eq!(
        control.take_observed(),
        vec![TestStorageOperation::WriteMany]
    );
    control.fail_next(TestStorageOperation::WriteMany);
    control.release_one();
    let error = flush.await.unwrap_err();
    assert!(matches!(
        error,
        Error::IvmRuntime(IvmRuntimeError::Chunk(
            crate::chunks::ChunkError::Backend(ref message)
        )) if message.contains("injected WriteMany failure")
    ));
    assert!(database.poisoned);

    let event = std::future::poll_fn(|cx| subscription.poll_next_event(cx)).await;
    let SubscriptionEvent::Error(error) = event else {
        panic!("late metadata failure left the subscription unresolved");
    };
    assert!(matches!(
        error.source_error(),
        Some(IvmRuntimeError::Chunk(
            crate::chunks::ChunkError::Backend(message)
        )) if message.contains("injected WriteMany failure")
    ));
    assert!(matches!(
        database.flush().await,
        Err(Error::DatabasePoisoned)
    ));
}

/// A host provider can return the same public backend error text as a late
/// metadata write, but it cannot claim the runtime's private install-failure
/// provenance. The request remains query-scoped without poisoning the database.
#[futures_test::test]
async fn external_chunk_backend_error_cannot_forge_publication_durability_failure() {
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
    let staged = database
        .prepare_and_stage_large_value(
            crate::large_values::LargeValueKind::Bytes,
            &vec![9; crate::large_values::INLINE_VALUE_MAX_BYTES * 4],
        )
        .await
        .unwrap();
    let provider_ready = Rc::new(Cell::new(false));
    let forged_message = "test storage error: injected WriteMany failure";
    database.set_chunk_provider(Rc::new(DeferredErrorChunkProvider {
        ready: Rc::clone(&provider_ready),
        message: forged_message.to_owned(),
    }));
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("objects"))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut first = database.open_batch();
    first.insert("objects", vec![Value::U64(1), Value::Bytes(vec![1])]);
    let first = database.apply_batch(first).await.unwrap();
    assert_eq!(
        subscription
            .try_recv_with_publication()
            .unwrap()
            .publication,
        Some(first.publication())
    );
    database.finish_persistence(first.persist().await).unwrap();

    let mut second = database.open_batch();
    second.insert(
        "objects",
        vec![Value::U64(2), Value::Large(staged.value_ref)],
    );
    second.accept_large_value(staged.id);
    let second = database.apply_batch(second).await.unwrap();
    assert!(subscription.try_recv().is_err());
    database.finish_persistence(second.persist().await).unwrap();

    provider_ready.set(true);
    database.flush().await.unwrap();
    assert!(!database.poisoned);
    let waker = futures::task::noop_waker();
    let mut context = std::task::Context::from_waker(&waker);
    assert!(subscription.poll_next_event(&mut context).is_pending());
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
            crate::large_values::decode_canonical_node(&root.encoded).unwrap()
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
        root.encoded = crate::large_values::encode_node(&crate::large_values::ChunkNode::Branch {
            format: crate::large_values::FORMAT_VERSION,
            kind: crate::large_values::LargeValueKind::Bytes,
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
        assert_eq!(raw_value, b"\x02one");

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
