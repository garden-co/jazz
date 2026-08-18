// Persistence-boundary contracts for the future split main-thread / async-store
// runtime. The current synchronous store is intentionally exercised through the
// same seam: a failed durable commit is not permission to publish an IVM delta,
// a view, or a fate acknowledgement.

#[derive(Clone)]
struct FailWriteManyMemoryStorage {
    inner: MemoryStorage,
    fail_on_write_many: std::rc::Rc<std::cell::Cell<Option<usize>>>,
    write_many_calls: std::rc::Rc<std::cell::Cell<usize>>,
}

impl FailWriteManyMemoryStorage {
    fn new(column_families: &[&str]) -> Self {
        Self {
            inner: MemoryStorage::new(column_families),
            fail_on_write_many: std::rc::Rc::new(std::cell::Cell::new(None)),
            write_many_calls: std::rc::Rc::new(std::cell::Cell::new(0)),
        }
    }

    /// Fail the Nth following storage-atomic commit. Counting from the next
    /// call lets a test target either durable ingest or its crash-window
    /// finalization without depending on bootstrap writes.
    fn fail_nth_following_write_many(&self, nth: usize) {
        assert!(nth > 0, "write-many failpoint is one-based");
        self.fail_on_write_many
            .set(Some(self.write_many_calls.get() + nth));
    }

    fn write_many_call_count(&self) -> usize {
        self.write_many_calls.get()
    }
}

impl ResidentStorage for FailWriteManyMemoryStorage {
    fn get(
        &self,
        cf: &ColumnFamilyName,
        key: &Key,
    ) -> Result<Option<StorageValue>, groove::storage::Error> {
        self.inner.get(cf, key)
    }

    fn set(
        &self,
        cf: &ColumnFamilyName,
        key: &Key,
        value: &[u8],
    ) -> Result<(), groove::storage::Error> {
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), groove::storage::Error> {
        self.inner.delete(cf, key)
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), groove::storage::Error> {
        self.inner.scan_range(cf, start, end, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), groove::storage::Error> {
        self.inner.scan_prefix(cf, prefix, visit)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), groove::storage::Error> {
        let call = self.write_many_calls.get() + 1;
        self.write_many_calls.set(call);
        if self.fail_on_write_many.get() == Some(call) {
            self.fail_on_write_many.set(None);
            return Err(groove::storage::Error::InvalidStorageLayout(
                "injected durable commit failure".to_owned(),
            ));
        }
        self.inner.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for FailWriteManyMemoryStorage {
    fn reopen(
        mut self,
        column_families: &[&str],
    ) -> Result<Self, groove::storage::Error> {
        self.inner = self.inner.reopen(column_families)?;
        Ok(self)
    }
}

fn fail_write_many_node() -> (NodeState<FailWriteManyMemoryStorage>, FailWriteManyMemoryStorage) {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailWriteManyMemoryStorage::new(&refs);
    let node = NodeState::new(node(0xd1), node_schema, storage.clone()).unwrap();
    (node, storage)
}

fn assert_poisoned_node_exposes_nothing(core: &mut NodeState<FailWriteManyMemoryStorage>) {
    assert!(matches!(
        core.subscribe_history("todos"),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));
    assert!(matches!(
        core.current_rows("todos", DurabilityTier::Global),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));
    assert!(matches!(
        core.query_table_versions("todos"),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));
    assert!(matches!(
        core.database.flush(),
        Err(groove::db::Error::DatabasePoisoned)
    ));
}

struct GatedAuthorityStorage {
    inner: groove::storage::async_ordered::ImmediateStorage<MemoryStorage>,
    released: std::rc::Rc<std::cell::Cell<bool>>,
    cancellations: std::rc::Rc<std::cell::Cell<usize>>,
    committed_units: std::rc::Rc<std::cell::RefCell<Vec<usize>>>,
    fail_commits: std::rc::Rc<std::cell::Cell<bool>>,
}

impl groove::storage::async_ordered::OrderedKvStorage for GatedAuthorityStorage {
    fn poll_request(
        &mut self,
        request: &groove::storage::async_ordered::OwnedStorageRequest,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<groove::storage::async_ordered::OwnedStorageResponse, groove::storage::Error>,
    > {
        if !self.released.get() {
            return std::task::Poll::Pending;
        }
        if let groove::storage::async_ordered::OwnedStorageOperation::Commit(operations) =
            request.operation()
        {
            if self.fail_commits.get() {
                return std::task::Poll::Ready(Err(groove::storage::Error::Backend {
                    backend: "gated-authority-test",
                    message: "injected commit failure".to_owned(),
                }));
            }
            self.committed_units.borrow_mut().push(operations.len());
        }
        groove::storage::async_ordered::OrderedKvStorage::poll_request(
            &mut self.inner,
            request,
            context,
        )
    }

    fn cancel_request(
        &mut self,
        _request: groove::storage::async_ordered::StorageRequestId,
    ) -> Result<(), groove::storage::Error> {
        self.cancellations
            .set(self.cancellations.get().saturating_add(1));
        Ok(())
    }
}

struct CommitGatedAuthorityStorage {
    inner: groove::storage::async_ordered::ImmediateStorage<MemoryStorage>,
    released: std::rc::Rc<std::cell::Cell<bool>>,
    fail: std::rc::Rc<std::cell::Cell<bool>>,
    completed: std::rc::Rc<std::cell::RefCell<Vec<&'static str>>>,
}

impl groove::storage::async_ordered::OrderedKvStorage for CommitGatedAuthorityStorage {
    fn poll_request(
        &mut self,
        request: &groove::storage::async_ordered::OwnedStorageRequest,
        context: &mut std::task::Context<'_>,
    ) -> std::task::Poll<
        Result<groove::storage::async_ordered::OwnedStorageResponse, groove::storage::Error>,
    > {
        if matches!(
            request.operation(),
            groove::storage::async_ordered::OwnedStorageOperation::Commit(_)
        ) {
            if !self.released.get() {
                return std::task::Poll::Pending;
            }
            if self.fail.get() {
                return std::task::Poll::Ready(Err(groove::storage::Error::Backend {
                    backend: "commit-gated-authority-test",
                    message: "injected authority commit failure".to_owned(),
                }));
            }
        }
        let result = groove::storage::async_ordered::OrderedKvStorage::poll_request(
            &mut self.inner,
            request,
            context,
        );
        if matches!(result, std::task::Poll::Ready(Ok(_))) {
            let operation = match request.operation() {
                groove::storage::async_ordered::OwnedStorageOperation::Commit(_) => "commit",
                groove::storage::async_ordered::OwnedStorageOperation::Flush => "flush",
                groove::storage::async_ordered::OwnedStorageOperation::Close => "close",
                _ => "read",
            };
            self.completed.borrow_mut().push(operation);
        }
        result
    }

    fn cancel_request(
        &mut self,
        request: groove::storage::async_ordered::StorageRequestId,
    ) -> Result<(), groove::storage::Error> {
        self.inner.cancel_request(request)
    }
}

fn authority_runtime(
    released: std::rc::Rc<std::cell::Cell<bool>>,
    fail: std::rc::Rc<std::cell::Cell<bool>>,
) -> DemandDrivenNode {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released,
        fail,
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let mut opening = PollableNodeOpen::new(node(0xce), node_schema, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(runtime)) = opening.poll(&mut context) else {
        panic!("commit-only gate must not delay node opening reads")
    };
    runtime
}

#[test]
fn demand_driven_db_preserves_synchronous_facade_visibility_before_durability() {
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let failed = std::rc::Rc::new(std::cell::Cell::new(false));
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let completed = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released: std::rc::Rc::clone(&released),
        fail: failed,
        completed: std::rc::Rc::clone(&completed),
    };
    let mut opening = crate::db::PollableDbOpen::new(
        node_schema,
        crate::db::DbIdentity {
            node: node(0xc9),
            author: AuthorId::from_bytes([0xc9; 16]),
        },
        Box::new(storage),
    )
    .with_id_source(crate::db::SeededRowIdSource::new(0xc9));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut owner)) = opening.poll(&mut context) else {
        panic!("commit-only gate must allow database opening")
    };
    let prepared = owner.prepare_query(&owner.table("todos")).unwrap();
    let opts = crate::db::ReadOpts {
        tier: DurabilityTier::None,
        local_updates: crate::db::LocalUpdates::Immediate,
        propagation: crate::db::Propagation::LocalOnly,
        ..crate::db::ReadOpts::default()
    };
    let std::task::Poll::Ready(Ok(mut subscription)) =
        owner.poll_subscribe(&mut context, &prepared, opts.clone())
    else {
        panic!("a resident subscription opening must complete in its first poll")
    };
    assert!(matches!(
        crate::db::block_on(subscription.next_event()),
        Some(crate::db::SubscriptionEvent::Delta { reset: true, .. })
    ));
    released.set(false);
    let write = crate::db::block_on(owner.insert("todos", title_cells("facade immediate")))
        .unwrap();
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event()
    else {
        panic!("insert must queue its immediate callback before returning")
    };
    let std::task::Poll::Ready(Ok(rows)) = owner.poll_all(&mut context, &prepared, opts.clone())
    else {
        panic!("a resident post-write read must complete in its first poll")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), write.row_uuid());
    assert!(owner.poll_persistence(&mut context).is_pending());
    released.set(true);
    assert!(matches!(
        owner.poll_persistence(&mut context),
        std::task::Poll::Ready(Ok(()))
    ));
    drop(subscription);
    drop(write);
    crate::db::block_on(owner.close()).unwrap();
    assert!(completed.borrow().ends_with(&["commit", "flush", "close"]));
    let mut reopened = NodeState::new(node(0xc9), schema(), durable).unwrap();
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn demand_driven_db_acquires_cold_subscription_before_registering_it() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let identity = crate::db::DbIdentity {
        node: node(0xca),
        author: AuthorId::from_bytes([0xca; 16]),
    };
    let seeded = crate::db::block_on(crate::db::Db::open(crate::db::DbConfig {
        schema: node_schema.clone(),
        storage: durable.clone(),
        identity,
        id_source: Some(Box::new(crate::db::SeededRowIdSource::new(0xca))),
    }))
    .unwrap();
    seeded.insert("todos", title_cells("durable seed")).unwrap();
    drop(seeded);

    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut owner)) = opening.poll(&mut context) else {
        panic!("released metadata reads must open the database")
    };
    let prepared = owner.prepare_query(&owner.table("todos")).unwrap();
    let opts = crate::db::ReadOpts {
        tier: DurabilityTier::None,
        local_updates: crate::db::LocalUpdates::Immediate,
        propagation: crate::db::Propagation::LocalOnly,
        ..crate::db::ReadOpts::default()
    };
    let subscriptions_before = owner.runtime_stats_for_test().active_subscriptions;
    released.set(false);
    assert!(owner
        .poll_subscribe(&mut context, &prepared, opts.clone())
        .is_pending());
    assert_eq!(
        owner.runtime_stats_for_test().active_subscriptions,
        subscriptions_before,
        "a suspended cold opening must not leave a real subscription registered"
    );
    released.set(true);
    let std::task::Poll::Ready(Ok(mut subscription)) =
        owner.poll_subscribe(&mut context, &prepared, opts.clone())
    else {
        panic!("released input must complete the subscription opening")
    };
    let Some(crate::db::SubscriptionEvent::Delta {
        reset: true, added, ..
    }) = subscription.try_next_event()
    else {
        panic!("completed opening must synchronously queue its initial reset")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(
        owner.runtime_stats_for_test().active_subscriptions,
        subscriptions_before + 1
    );
    released.set(false);
    let write = {
        let mut insert = std::pin::pin!(owner.insert("todos", title_cells("cold insert")));
        assert!(std::future::Future::poll(insert.as_mut(), &mut context).is_pending());
        assert!(
            subscription.try_next_event().is_none(),
            "acquisition must not publish a partial local write"
        );
        released.set(true);
        let std::task::Poll::Ready(Ok(write)) =
            std::future::Future::poll(insert.as_mut(), &mut context)
        else {
            panic!("released write inputs must publish in the resolving poll")
        };
        write
    };
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event()
    else {
        panic!("the resolving write poll must synchronously refresh subscriptions")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), write.row_uuid());
    released.set(false);
    let std::task::Poll::Ready(Ok(rows)) =
        owner.poll_all(&mut context, &prepared, opts.clone())
    else {
        panic!("the direct written rows must remain first-poll visible")
    };
    assert_eq!(rows.len(), 2);

    let updated_write = {
        let mut update = std::pin::pin!(owner.update(
            "todos",
            write.row_uuid(),
            std::collections::BTreeMap::from([(
                "title".to_owned(),
                groove::records::Value::String("resident update".to_owned()),
            )]),
        ));
        let updated = match std::future::Future::poll(update.as_mut(), &mut context) {
            std::task::Poll::Ready(Ok(updated)) => updated,
            std::task::Poll::Ready(Err(error)) => panic!("resident update failed: {error}"),
            std::task::Poll::Pending => {
                panic!("an update over a resident row must complete in its first poll")
            }
        };
        updated
    };
    let Some(crate::db::SubscriptionEvent::Delta { updated, .. }) =
        subscription.try_next_event()
    else {
        panic!("the first-poll update must synchronously refresh subscriptions")
    };
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].row_uuid(), updated_write.row_uuid());

    let deleted_write = {
        let mut delete = std::pin::pin!(owner.delete("todos", write.row_uuid()));
        match std::future::Future::poll(delete.as_mut(), &mut context) {
            std::task::Poll::Ready(Ok(deleted)) => deleted,
            std::task::Poll::Ready(Err(error)) => panic!("resident delete failed: {error}"),
            std::task::Poll::Pending => {
                panic!("a delete over a resident row must complete in its first poll")
            }
        }
    };
    let Some(crate::db::SubscriptionEvent::Delta { removed, .. }) =
        subscription.try_next_event()
    else {
        panic!("the first-poll delete must synchronously refresh subscriptions")
    };
    assert_eq!(removed.len(), 1);
    assert_eq!(deleted_write.row_uuid(), write.row_uuid());
    let std::task::Poll::Ready(Ok(rows)) = owner.poll_all(&mut context, &prepared, opts) else {
        panic!("a resident post-delete read must complete in its first poll")
    };
    assert_eq!(rows.len(), 1);

    let restored_write = {
        let mut restore = std::pin::pin!(owner.restore(
            "todos",
            write.row_uuid(),
            title_cells("resident restore"),
        ));
        match std::future::Future::poll(restore.as_mut(), &mut context) {
            std::task::Poll::Ready(Ok(restored)) => restored,
            std::task::Poll::Ready(Err(error)) => panic!("resident restore failed: {error}"),
            std::task::Poll::Pending => {
                panic!("a restore over resident witnesses must complete in its first poll")
            }
        }
    };
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event()
    else {
        panic!("the first-poll restore must synchronously refresh subscriptions")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(restored_write.row_uuid(), write.row_uuid());
    assert!(owner.poll_persistence(&mut context).is_pending());
}

#[test]
fn demand_driven_db_acquires_cold_relation_snapshot() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let identity = crate::db::DbIdentity {
        node: node(0xcb),
        author: AuthorId::from_bytes([0xcb; 16]),
    };
    let seeded = crate::db::block_on(crate::db::Db::open(crate::db::DbConfig {
        schema: node_schema.clone(),
        storage: durable.clone(),
        identity,
        id_source: Some(Box::new(crate::db::SeededRowIdSource::new(0xcb))),
    }))
    .unwrap();
    seeded.insert("todos", title_cells("relation seed")).unwrap();
    drop(seeded);

    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut owner)) = opening.poll(&mut context) else {
        panic!("released metadata reads must open the database")
    };
    let prepared = owner.prepare_query(&owner.table("todos")).unwrap();
    let opts = crate::db::ReadOpts {
        tier: DurabilityTier::None,
        local_updates: crate::db::LocalUpdates::Immediate,
        propagation: crate::db::Propagation::LocalOnly,
        ..crate::db::ReadOpts::default()
    };
    released.set(false);
    assert!(owner
        .poll_relation_snapshot(&mut context, &prepared, opts.clone())
        .is_pending());
    released.set(true);
    let std::task::Poll::Ready(Ok(snapshot)) =
        owner.poll_relation_snapshot(&mut context, &prepared, opts)
    else {
        panic!("released relation inputs must finish materialization")
    };
    assert_eq!(snapshot.root_count, 1);
    assert_eq!(snapshot.rows.len(), 1);
}

#[test]
fn demand_driven_db_acquires_cold_mutations_before_single_publish() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let identity = crate::db::DbIdentity {
        node: node(0xcc),
        author: AuthorId::from_bytes([0xcc; 16]),
    };
    let seeded = crate::db::block_on(crate::db::Db::open(crate::db::DbConfig {
        schema: node_schema.clone(),
        storage: durable.clone(),
        identity,
        id_source: Some(Box::new(crate::db::SeededRowIdSource::new(0xcc))),
    }))
    .unwrap();
    let row = seeded
        .insert("todos", title_cells("cold update seed"))
        .unwrap()
        .row_uuid();
    let delete_row = seeded
        .insert("todos", title_cells("cold delete seed"))
        .unwrap()
        .row_uuid();
    let restore_row = seeded
        .insert("todos", title_cells("cold restore seed"))
        .unwrap()
        .row_uuid();
    seeded.delete("todos", restore_row).unwrap();
    drop(seeded);

    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut owner)) = opening.poll(&mut context) else {
        panic!("released metadata reads must open the database")
    };
    released.set(false);
    let deleted = {
        let mut delete = std::pin::pin!(owner.delete("todos", delete_row));
        assert!(std::future::Future::poll(delete.as_mut(), &mut context).is_pending());
        released.set(true);
        let std::task::Poll::Ready(Ok(deleted)) =
            std::future::Future::poll(delete.as_mut(), &mut context)
        else {
            panic!("released delete dependencies must publish exactly once")
        };
        deleted
    };
    assert_eq!(deleted.row_uuid(), delete_row);
    released.set(false);
    let restored = {
        let mut restore = std::pin::pin!(owner.restore(
            "todos",
            restore_row,
            title_cells("after restore acquisition"),
        ));
        assert!(std::future::Future::poll(restore.as_mut(), &mut context).is_pending());
        released.set(true);
        let std::task::Poll::Ready(Ok(restored)) =
            std::future::Future::poll(restore.as_mut(), &mut context)
        else {
            panic!("released restore dependencies must publish exactly once")
        };
        restored
    };
    assert_eq!(restored.row_uuid(), restore_row);
    let upsert_row = RowUuid::from_bytes([0xcd; 16]);
    released.set(false);
    let upserted = {
        let mut upsert = std::pin::pin!(owner.upsert(
            "todos",
            upsert_row,
            title_cells("after upsert acquisition"),
        ));
        assert!(std::future::Future::poll(upsert.as_mut(), &mut context).is_pending());
        released.set(true);
        let std::task::Poll::Ready(Ok(upserted)) =
            std::future::Future::poll(upsert.as_mut(), &mut context)
        else {
            panic!("released upsert dependencies must publish exactly once")
        };
        upserted
    };
    assert_eq!(upserted.row_uuid(), upsert_row);
    released.set(false);
    let write = {
        let mut update = std::pin::pin!(owner.update(
            "todos",
            row,
            std::collections::BTreeMap::from([(
                "title".to_owned(),
                groove::records::Value::String("after acquisition".to_owned()),
            )]),
        ));
        assert!(std::future::Future::poll(update.as_mut(), &mut context).is_pending());
        released.set(true);
        let std::task::Poll::Ready(Ok(write)) =
            std::future::Future::poll(update.as_mut(), &mut context)
        else {
            panic!("released update dependencies must publish exactly once")
        };
        write
    };
    assert_eq!(write.row_uuid(), row);
    let prepared = owner
        .prepare_query(&crate::query::Query::from("todos").filter(crate::query::eq(
            crate::query::col("id"),
            crate::query::lit(groove::records::Value::Uuid(row.0)),
        )))
        .unwrap();
    let rows = crate::db::block_on(owner.all(
        &prepared,
        crate::db::ReadOpts {
            tier: DurabilityTier::None,
            local_updates: crate::db::LocalUpdates::Immediate,
            propagation: crate::db::Propagation::LocalOnly,
            ..crate::db::ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
}

struct PersistenceTestWake;

impl std::task::Wake for PersistenceTestWake {
    fn wake(self: std::sync::Arc<Self>) {}
}

#[test]
fn pollable_node_open_acquires_inputs_then_durably_finalizes_once() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let released = std::rc::Rc::new(std::cell::Cell::new(false));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xd2), node_schema.clone(), Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);

    assert!(opening.poll(&mut context).is_pending());
    released.set(true);
    let std::task::Poll::Ready(Ok(mut opened)) = opening.poll(&mut context) else {
        panic!("released node opening must acquire, construct, and finalize")
    };
    assert!(matches!(
        opened.poll_current_rows(&mut context, "todos", DurabilityTier::None),
        std::task::Poll::Ready(Ok(_))
    ));
    drop(opened);

    NodeState::new(node(0xd2), node_schema, durable)
        .expect("a ready pollable node must have durably finalized its catalogue metadata");
}

#[test]
fn dropping_pollable_node_open_cancels_its_exact_pending_input() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let cancellations = std::rc::Rc::new(std::cell::Cell::new(0));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::new(std::cell::Cell::new(false)),
        cancellations: std::rc::Rc::clone(&cancellations),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xd4), node_schema, Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);

    assert!(opening.poll(&mut context).is_pending());
    drop(opening);
    assert_eq!(cancellations.get(), 1);
}

#[test]
fn immediate_storage_inherits_first_poll_node_readiness() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let mut opening = PollableNodeOpen::new(
        node(0xd3),
        node_schema,
        Box::new(groove::storage::async_ordered::ImmediateStorage::new(
            MemoryStorage::new(&refs),
        )),
    );
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);

    assert!(matches!(opening.poll(&mut context), std::task::Poll::Ready(Ok(_))));
}

#[test]
fn demand_driven_node_publishes_locally_before_one_atomic_durable_unit() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let committed_units = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::clone(&committed_units),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening =
        PollableNodeOpen::new(node(0xd5), node_schema.clone(), Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("immediate backend must open in its first poll")
    };
    committed_units.borrow_mut().clear();
    released.set(false);

    let commit = MergeableCommit::new("todos", row(0xd5), 10).cells(title_cells("resident"));
    assert!(matches!(
        runtime.poll_mergeable_commit(&mut context, &commit),
        std::task::Poll::Ready(Ok(_))
    ));
    let std::task::Poll::Ready(Ok(rows)) =
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::None)
    else {
        panic!("the published local row must be synchronously queryable")
    };
    assert_eq!(rows.len(), 1);
    assert!(committed_units.borrow().is_empty());
    assert!(runtime.poll_persistence(&mut context).is_pending());
    assert!(committed_units.borrow().is_empty());

    released.set(true);
    let outcome = runtime.poll_persistence(&mut context);
    assert!(
        matches!(outcome, std::task::Poll::Ready(Ok(()))),
        "released immediate persistence returned {outcome:?}"
    );
    assert_eq!(committed_units.borrow().len(), 1);
    assert!(!committed_units.borrow()[0].eq(&0));
    drop(runtime);
    let mut reopened = NodeState::new(node(0xd5), node_schema, durable).unwrap();
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::None)
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn cold_mergeable_preparation_suspends_before_resident_publication() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let committed_units = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let durable = MemoryStorage::new(&refs);
    let bootstrap_backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::clone(&committed_units),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(
        node(0xd7),
        node_schema.clone(),
        Box::new(bootstrap_backend),
    );
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(bootstrap)) = opening.poll(&mut context) else {
        panic!("released backend must open in its first poll")
    };
    drop(bootstrap);

    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::clone(&committed_units),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xd7), node_schema, Box::new(backend));
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("checkpointed backend must reopen in its first poll")
    };
    committed_units.borrow_mut().clear();
    released.set(false);

    let commit =
        MergeableCommit::new("todos", row(0xd7), 10).cells(title_cells("after acquisition"));
    assert!(runtime
        .poll_mergeable_commit(&mut context, &commit)
        .is_pending());
    assert!(
        committed_units.borrow().is_empty(),
        "a cold preflight cannot publish a durable batch"
    );

    released.set(true);
    let mut published = false;
    for _ in 0..16 {
        match runtime.poll_mergeable_commit(&mut context, &commit) {
            std::task::Poll::Pending => assert!(
                committed_units.borrow().is_empty(),
                "each acquisition poll must remain publication-free"
            ),
            std::task::Poll::Ready(Ok(_tx_id)) => {
                published = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("prepared write failed: {error}"),
        }
    }
    assert!(published, "the admitted write did not publish");
    let std::task::Poll::Ready(Ok(rows)) =
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::None)
    else {
        panic!("published local row must be resident without another await")
    };
    assert_eq!(rows.len(), 1);
    assert!(
        committed_units.borrow().is_empty(),
        "resident visibility precedes asynchronous durability"
    );
}

#[test]
fn prepared_local_write_notifies_jazz_subscription_in_publish_poll() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xd8), node_schema, Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("released backend must open in its first poll")
    };
    let std::task::Poll::Ready(Ok(subscription)) =
        runtime.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("released empty history must open in its first poll")
    };
    assert!(subscription.recv().unwrap().is_empty());

    released.set(false);
    let commit = MergeableCommit::new("todos", row(0xd8), 10).cells(title_cells("callback"));
    assert!(matches!(
        runtime.poll_mergeable_commit(&mut context, &commit),
        std::task::Poll::Ready(Ok(_))
    ));
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap().len(),
        1,
        "the callback must be queued before the publishing poll returns Ready"
    );
    assert!(runtime.poll_persistence(&mut context).is_pending());
}

#[test]
fn prepared_branch_creation_publishes_metadata_before_async_durability() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xd9), node_schema, Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("released backend must open in its first poll")
    };

    released.set(false);
    let branch = BranchId(uuid::Uuid::from_bytes([0xda; 16]));
    let author = AuthorId(uuid::Uuid::from_bytes([0xdb; 16]));
    assert!(matches!(
        runtime.poll_create_branch(&mut context, branch, author),
        std::task::Poll::Ready(Ok(_))
    ));
    assert_eq!(
        runtime.resident().branch_record(branch).unwrap().created_by,
        author
    );
    assert_eq!(
        runtime.resident().pending_branch_metadata_uploads().len(),
        1,
        "resident metadata and its sync outbox must publish together"
    );
    assert!(runtime.poll_persistence(&mut context).is_pending());
}

#[test]
fn prepared_fate_publishes_resident_tier_before_async_durability() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xda), node_schema, Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("released backend must open in its first poll")
    };
    let commit = MergeableCommit::new("todos", row(0xda), 10).cells(title_cells("accepted"));
    let std::task::Poll::Ready(Ok(tx_id)) =
        runtime.poll_mergeable_commit(&mut context, &commit)
    else {
        panic!("resident local write must publish in its first poll")
    };
    let std::task::Poll::Ready(Ok(edge_rows)) =
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::Edge)
    else {
        panic!("resident Edge read must not suspend")
    };
    assert!(edge_rows.is_empty());

    released.set(false);
    assert!(matches!(
        runtime.poll_apply_fate_update(
            &mut context,
            tx_id,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        ),
        std::task::Poll::Ready(Ok(()))
    ));
    let std::task::Poll::Ready(Ok(edge_rows)) =
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::Edge)
    else {
        panic!("published fate must keep its current table resident")
    };
    assert_eq!(
        edge_rows.len(),
        1,
        "the publishing poll must expose the accepted row at its new tier"
    );
    assert!(
        runtime.poll_persistence(&mut context).is_pending(),
        "resident authority visibility must not wait for async durability"
    );
}

#[test]
fn cold_fate_preparation_suspends_before_authority_publication() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let committed_units = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let make_backend = || GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::clone(&committed_units),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);

    let mut opening = PollableNodeOpen::new(
        node(0xdc),
        node_schema.clone(),
        Box::new(make_backend()),
    );
    let std::task::Poll::Ready(Ok(mut bootstrap)) = opening.poll(&mut context) else {
        panic!("released backend must open in its first poll")
    };
    let commit = MergeableCommit::new("todos", row(0xdc), 10).cells(title_cells("cold fate"));
    let std::task::Poll::Ready(Ok(tx_id)) =
        bootstrap.poll_mergeable_commit(&mut context, &commit)
    else {
        panic!("bootstrap write must publish")
    };
    assert!(matches!(
        bootstrap.poll_persistence(&mut context),
        std::task::Poll::Ready(Ok(()))
    ));
    drop(bootstrap);

    let mut opening = PollableNodeOpen::new(node(0xdc), node_schema, Box::new(make_backend()));
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("checkpointed backend must reopen in its first poll")
    };
    committed_units.borrow_mut().clear();
    released.set(false);
    assert!(runtime
        .poll_apply_fate_update(
            &mut context,
            tx_id,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        )
        .is_pending());
    assert!(
        committed_units.borrow().is_empty(),
        "cold fate acquisition must not publish a durable or resident update"
    );

    released.set(true);
    let mut published = false;
    for _ in 0..32 {
        match runtime.poll_apply_fate_update(
            &mut context,
            tx_id,
            Fate::Accepted,
            None,
            Some(DurabilityTier::Edge),
        ) {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Ok(())) => {
                published = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("prepared fate failed: {error}"),
        }
    }
    assert!(published, "released fate inputs did not publish");
    let std::task::Poll::Ready(Ok(rows)) =
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::Edge)
    else {
        panic!("published accepted row must be resident")
    };
    assert_eq!(rows.len(), 1);
}

#[test]
fn demand_driven_node_poisoned_after_durable_commit_failure() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let fail_commits = std::rc::Rc::new(std::cell::Cell::new(false));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::new(std::cell::Cell::new(true)),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::clone(&fail_commits),
    };
    let mut opening = PollableNodeOpen::new(node(0xd6), node_schema, Box::new(backend));
    let waker = std::sync::Arc::new(PersistenceTestWake).into();
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("immediate backend must open in its first poll")
    };
    let commit =
        MergeableCommit::new("todos", row(0xd6), 10).cells(title_cells("ambiguous"));
    assert!(matches!(
        runtime.poll_mergeable_commit(&mut context, &commit),
        std::task::Poll::Ready(Ok(_))
    ));

    fail_commits.set(true);
    assert!(matches!(
        runtime.poll_persistence(&mut context),
        std::task::Poll::Ready(Err(Error::Storage(_)))
    ));
    assert!(matches!(
        runtime.poll_current_rows(&mut context, "todos", DurabilityTier::None),
        std::task::Poll::Ready(Err(Error::Groove(
            groove::db::Error::DatabasePoisoned
        )))
    ));
}

#[test]
fn demand_driven_authority_releases_fate_and_subscription_after_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcd), 10).cells(title_cells("scheduled")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let failed = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut authority = authority_runtime(std::rc::Rc::clone(&released), failed);
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(history)) =
        authority.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("resident history must open")
    };
    assert!(history.recv().unwrap().is_empty());
    released.set(false);

    assert!(authority
        .poll_ingest_commit_unit(
            &mut context,
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        )
        .is_pending());
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(
        authority.poll_current_rows(&mut context, "todos", DurabilityTier::Global),
        std::task::Poll::Ready(Err(Error::InvalidStoredValue(_)))
    ));

    released.set(true);
    let outcome = authority.poll_ingest_commit_unit(
        &mut context,
        tx,
        versions,
        u64::MAX - SKEW_TOLERANCE_MS,
        None,
    );
    let std::task::Poll::Ready(Ok(responses)) = outcome else {
        panic!("released authority commit must complete: {outcome:?}")
    };
    assert!(matches!(
        responses.as_slice(),
        [SyncMessage::FateUpdate {
            tx_id: response_tx,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        }] if *response_tx == tx_id
    ));
    assert_eq!(history.recv().unwrap().to_values().unwrap().len(), 1);
}

#[test]
fn cold_authority_preflight_suspends_before_transaction_or_callback_publication() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xca), 10).cells(title_cells("cold authority")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let durable = MemoryStorage::new(&refs);
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let mut bootstrap = PollableNodeOpen::new(
        node(0xca),
        node_schema.clone(),
        Box::new(groove::storage::async_ordered::ImmediateStorage::new(
            durable.clone(),
        )),
    );
    let std::task::Poll::Ready(Ok(bootstrap)) = bootstrap.poll(&mut context) else {
        panic!("immediate bootstrap must complete")
    };
    drop(bootstrap);

    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = PollableNodeOpen::new(node(0xca), node_schema, Box::new(backend));
    let std::task::Poll::Ready(Ok(mut authority)) = opening.poll(&mut context) else {
        panic!("checkpointed authority must reopen")
    };
    let std::task::Poll::Ready(Ok(history)) =
        authority.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("empty history must open before gating storage")
    };
    assert!(history.recv().unwrap().is_empty());
    released.set(false);
    assert!(authority
        .poll_ingest_commit_unit(
            &mut context,
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        )
        .is_pending());
    assert!(authority.node.borrow_mut().transaction_record(tx_id).is_none());
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    released.set(true);
    let responses = loop {
        match authority.poll_ingest_commit_unit(
            &mut context,
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        ) {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Ok(responses)) => break responses,
            std::task::Poll::Ready(Err(error)) => panic!("cold authority ingest failed: {error}"),
        }
    };
    assert!(matches!(
        responses.as_slice(),
        [SyncMessage::FateUpdate { tx_id: response, .. }] if *response == tx_id
    ));
    assert_eq!(history.recv().unwrap().to_values().unwrap().len(), 1);
}

#[test]
fn immediate_authority_storage_completes_through_the_same_first_poll() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcc), 10).cells(title_cells("immediate")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let mut authority = authority_runtime(
        std::rc::Rc::new(std::cell::Cell::new(true)),
        std::rc::Rc::new(std::cell::Cell::new(false)),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let outcome = authority.poll_ingest_commit_unit(
        &mut context,
        tx,
        versions,
        u64::MAX - SKEW_TOLERANCE_MS,
        None,
    );
    assert!(matches!(
        outcome,
        std::task::Poll::Ready(Ok(responses))
            if matches!(responses.as_slice(), [SyncMessage::FateUpdate { tx_id: response, .. }] if *response == tx_id)
    ));
}

#[test]
fn failed_demand_driven_authority_commit_discards_publication_and_poisons() {
    let (mut writer, _) = fail_write_many_node();
    let (_, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcb), 10).cells(title_cells("must poison")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let failed = std::rc::Rc::new(std::cell::Cell::new(false));
    let mut authority =
        authority_runtime(std::rc::Rc::clone(&released), std::rc::Rc::clone(&failed));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(history)) =
        authority.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("resident history must open")
    };
    assert!(history.recv().unwrap().is_empty());
    failed.set(true);
    assert!(matches!(
        authority.poll_ingest_commit_unit(
            &mut context,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        ),
        std::task::Poll::Ready(Err(Error::Storage(_)))
    ));
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert!(matches!(
        authority.poll_current_rows(&mut context, "todos", DurabilityTier::Global),
        std::task::Poll::Ready(Err(Error::Groove(
            groove::db::Error::DatabasePoisoned
        )))
    ));
}

/// INV-TX-25: one commit unit is one durable publication boundary. If the
/// storage batch that contains a multi-row local write fails, neither a subset
/// of the transaction nor a derived history subscription delta may escape.
#[test]
fn failed_multi_row_local_commit_is_not_partially_durable_or_published() {
    let (mut writer, storage) = fail_write_many_node();
    let history = writer.subscribe_history("todos").unwrap();
    assert!(
        history.recv().unwrap().is_empty(),
        "subscription setup must start from the empty durable history"
    );
    storage.fail_nth_following_write_many(1);

    let error = writer
        .commit_mergeable_many(vec![
            MergeableCommit::new("todos", row(0xd1), 10).cells(title_cells("first")),
            MergeableCommit::new("todos", row(0xd2), 10).cells(title_cells("second")),
        ])
        .expect_err("injected final persistence failure must fail the full commit unit");
    assert!(
        matches!(error, Error::Groove(groove::db::Error::Storage(_))),
        "unexpected durable-commit error: {error:?}"
    );
    assert!(
        matches!(history.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
        "a failed durable commit must not publish a history/IVM delta"
    );

    drop(writer);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    assert!(
        reopened.query_table_versions("todos").unwrap().is_empty(),
        "failed storage batch must not leave a partial canonical history"
    );
    assert!(
        reopened
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty(),
        "failed storage batch must not leave a current-row or maintained-view input"
    );
}

/// A fate authority may only return a wire acknowledgement after every durable
/// component of finalization has completed. The injected first batch failure
/// proves that no `FateUpdate` can be observed for an uncommitted unit.
#[test]
fn authority_storage_failure_returns_no_fate_ack_or_partial_transaction() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd3), 10).cells(title_cells("authority")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    storage.fail_nth_following_write_many(1);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let error = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("authority must not acknowledge a unit whose persistence failed");
    assert!(
        matches!(error, Error::Groove(groove::db::Error::Storage(_))),
        "unexpected authority persistence error: {error:?}"
    );

    drop(core);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    assert!(
        reopened.transaction_record(tx_id).is_none(),
        "an unacknowledged authority write must leave no fate metadata"
    );
    assert!(reopened.query_table_versions("todos").unwrap().is_empty());
}

/// Canonical history, currency cleanup, checkpoint, and consistency marker are
/// one storage transaction. There is no intermediate finalization state:
/// failure leaves the durable node exactly before the authority unit.
#[test]
fn atomic_authority_ingest_failure_persists_and_publishes_nothing() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd6), 10).cells(title_cells("atomic failure")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    storage.fail_nth_following_write_many(1);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("failed atomic authority ingest must not acknowledge the unit");
    assert!(
        matches!(
            history.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "atomic failure must discard its staged IVM notifications"
    );
    assert_poisoned_node_exposes_nothing(&mut core);

    drop(core);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    assert!(reopened.transaction_record(tx_id).is_none());
}

/// The successful control proves one atomic authority batch releases exactly
/// one subscription tick after its single storage commit completes.
#[test]
fn successful_authority_finalization_is_one_storage_batch() {
    let (mut writer, _) = fail_write_many_node();
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd5), 10).cells(title_cells("published")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    let writes_before = storage.write_many_call_count();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let updates = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    assert_eq!(
        storage.write_many_call_count() - writes_before,
        1,
        "canonical persistence, cleanup, checkpoint, and marker must share one storage batch"
    );
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        }]
    ));
    let published = history
        .try_recv()
        .expect("successful finalization must publish its buffered history delta");
    assert_eq!(
        published.to_values().unwrap().len(),
        1,
        "one commit unit must publish one consolidated history delta"
    );
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
}

/// An inner failed authority scope poisons the whole nested publication unit.
/// Completing outer cleanup is then a safe no-op: it must neither panic nor
/// release the inner scope's speculative subscription output.
#[test]
fn nested_inner_failure_makes_outer_finish_safe_and_publishes_nothing() {
    let (mut writer, _) = fail_write_many_node();
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd7), 10).cells(title_cells("nested failure")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    let outer = core.database.begin_durable_publication_scope().unwrap();
    storage.fail_nth_following_write_many(1);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("inner finalization failure must abort the nested publication unit");

    outer.finish(&mut core.database);
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
}

/// Nested successful scopes retain all tick output until the outermost token is
/// consumed, then publish the commit unit exactly once.
#[test]
fn nested_success_publishes_exactly_once_at_outer_finish() {
    let (mut writer, _) = fail_write_many_node();
    let (_tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd8), 10).cells(title_cells("nested success")),
        )
        .unwrap();

    let (mut core, _) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    let outer = core.database.begin_durable_publication_scope().unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let updates = core
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        }]
    ));
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    outer.finish(&mut core.database);
    assert_eq!(history.try_recv().unwrap().to_values().unwrap().len(), 1);
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
}
