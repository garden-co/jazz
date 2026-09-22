//! Public publication/index regression with controlled unavailable content.
use super::*;

// Controlled storage is needed to separate publication durability from a
// checksum waiting for unavailable content; all writes use the public facade.
async fn pending_publication_fixture() -> (
    Database,
    crate::storage::TestStorageControl,
    Subscription,
    AppliedBatch,
    Rc<Cell<bool>>,
) {
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
    let staged = database
        .stage_large_value_preparation(prepared.clone())
        .await
        .unwrap();
    database.set_chunk_storage(Rc::new(crate::chunks::MemoryChunkStorage::new()));
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

    let mut batch = database.open_batch();
    batch.insert(
        "objects",
        vec![Value::U64(1), Value::Large(Box::new(staged.value_ref))],
    );
    batch.accept_large_value(staged.id);
    let publication = database.apply_batch(batch).await.unwrap();
    assert!(
        database.has_pending_progress(),
        "checksum must park an incremental evaluation"
    );
    (database, control, old, publication, ready)
}

/// Alice commits an indexed row while its checksum waits for unavailable
/// content. Its index must already be durable when the publication settles,
/// before any later query progress or runtime replacement.
#[futures_test::test]
async fn blocked_publication_persists_its_index_before_query_completion() {
    let (mut database, _control, _subscription, publication, _ready) =
        pending_publication_fixture().await;
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    assert_eq!(
        database
            .storage
            .prefix("objects".into(), Vec::new())
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        !database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty(),
        "a settled base publication must include its declared index writes"
    );
}

/// Alice's earlier blocked publication must not restore an index entry after
/// Bob deletes the row and the final checksum subscriber leaves.
#[futures_test::test]
async fn late_query_completion_cannot_resurrect_deleted_index_entries() {
    let (mut database, _control, subscription, publication, ready) =
        pending_publication_fixture().await;
    database
        .finish_persistence(publication.persist().await)
        .unwrap();
    assert!(
        !database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(database.unsubscribe(subscription.id()));
    let mut deletion = database.open_batch();
    deletion.delete("objects", PrimaryKeyValue::U64(1));
    database.commit_batch(deletion).await.unwrap();
    assert!(database.has_pending_progress());
    ready.set(true);
    database.drive_progress().await.unwrap();
    assert!(
        database
            .storage
            .prefix("objects".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

/// Alice's rejected atomic persistence must leave neither a base row nor an
/// index entry, even though the checksum terminal remains cold.
#[futures_test::test]
async fn failed_blocked_publication_does_not_persist_an_index_alone() {
    let (mut database, control, _subscription, publication, _ready) =
        pending_publication_fixture().await;
    control.fail_next(TestStorageOperation::WriteMany);
    assert!(
        database
            .finish_persistence(publication.persist().await)
            .is_err()
    );
    assert!(
        database
            .storage
            .prefix("objects".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

/// Alice sees her resident index before persistence, while Bob reopening the
/// underlying storage cannot see either base or index until the atomic write.
#[futures_test::test]
async fn resident_index_visible_before_publication_persistence() {
    let (database, _control, _subscription, _publication, _ready) =
        pending_publication_fixture().await;
    assert_eq!(
        database
            .index_get("objects", "objects_by_id", &[Value::U64(1)])
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        database
            .storage
            .prefix("objects".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

/// Cancelling Alice's shared atomic write leaves both base and index absent
/// and prevents Bob from continuing to use the incomplete runtime.
#[futures_test::test]
async fn cancelled_publication_keeps_base_and_index_atomic() {
    let (database, control, _subscription, publication, _ready) =
        pending_publication_fixture().await;
    control.pause_on(TestStorageOperation::WriteMany);
    let mut persistence = Box::pin(publication.persist());
    assert!(futures::poll!(persistence.as_mut()).is_pending());
    drop(persistence);
    assert!(matches!(
        database.ensure_usable(),
        Err(Error::DatabasePoisoned)
    ));
    assert!(
        database
            .storage
            .prefix("objects".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    control.resume();
}
