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
    fn is_durable(&self) -> bool {
        true
    }

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

struct QueuedInboundTransport {
    inbound: std::rc::Rc<std::cell::RefCell<std::collections::VecDeque<SyncMessage>>>,
    outbound: std::rc::Rc<std::cell::RefCell<Vec<SyncMessage>>>,
    session_context: Option<crate::db::ConnectionSessionContext>,
}

#[test]
fn demand_driven_subscriber_compilation_suspends_before_consuming_the_wire_frame() {
    let node_schema = schema();
    let shape = Query::from("todos").validate(&node_schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    let inbound = std::collections::VecDeque::from([
        SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            opts: opts.clone(),
            ast: crate::protocol::ShapeAst::from_validated(&shape),
        },
        SyncMessage::Subscribe(crate::protocol::Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }),
    ]);
    let refs = node_schema.column_families();
    let refs = refs.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let memory = MemoryStorage::new(&refs);
    let mut seeded = NodeState::new_history_complete(node(0xb8), node_schema.clone(), memory.clone())
        .unwrap();
    let write = seeded
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xba), 1).cells(title_cells("cold opening")),
        )
        .unwrap();
    seeded
        .apply_fate_update(
            write,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    drop(seeded);
    let storage = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(memory),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = crate::db::PollableDbOpen::new_history_complete(
        node_schema,
        crate::db::DbIdentity {
            node: node(0xb8),
            author: AuthorId::SYSTEM,
        },
        Box::new(storage),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut authority)) = opening.poll(&mut context) else {
        panic!("synchronous setup opens the demand-driven authority")
    };
    let outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _subscriber = authority.accept_subscriber(
        Box::new(QueuedInboundTransport {
            inbound: std::rc::Rc::new(std::cell::RefCell::new(inbound)),
            outbound: std::rc::Rc::clone(&outbound),
            session_context: None,
        }),
        AuthorId::from_bytes([0xb9; 16]),
    );

    released.set(false);
    assert!(matches!(
        authority.poll_tick(&mut context),
        std::task::Poll::Ready(Ok(_))
    ));
    assert!(authority.poll_tick(&mut context).is_pending());
    assert!(outbound.borrow().is_empty());

    released.set(true);
    for _ in 0..64 {
        match authority.poll_tick(&mut context) {
            std::task::Poll::Pending | std::task::Poll::Ready(Ok(_)) => {}
            std::task::Poll::Ready(Err(error)) => panic!("subscriber compilation failed: {error}"),
        }
        if outbound.borrow().iter().any(|message| matches!(
            message,
            SyncMessage::ViewUpdate {
                subscription: observed,
                reset_result_set: true,
                ..
            } if *observed == subscription
        )) {
            return;
        }
    }
    panic!("resident retry must consume the staged Subscribe and emit its opening");
}

impl crate::db::Transport for QueuedInboundTransport {
    fn send(
        &mut self,
        message: SyncMessage,
    ) -> Result<(), crate::wire::TransportError> {
        self.outbound.borrow_mut().push(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }

    fn connection_session_context(&self) -> Option<crate::db::ConnectionSessionContext> {
        self.session_context
    }
}

impl groove::storage::async_ordered::OrderedKvStorage for CommitGatedAuthorityStorage {
    fn is_durable(&self) -> bool {
        true
    }

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
    assert_eq!(
        owner.write_state(write.mergeable_tx_id()).unwrap().durability,
        DurabilityTier::None,
        "resident publication must not claim durability before commit"
    );
    assert!(owner.poll_persistence(&mut context).is_pending());
    released.set(true);
    assert!(matches!(
        owner.poll_persistence(&mut context),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(
        owner.write_state(write.mergeable_tx_id()).unwrap().durability,
        DurabilityTier::Local,
        "a completed durable commit must advance the exact transaction"
    );
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
fn synchronous_memory_publication_never_claims_local_durability() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = groove::storage::async_ordered::ImmediateStorage::new(
        MemoryStorage::new(&refs),
    );
    let mut opening = crate::db::PollableDbOpen::new(
        node_schema,
        crate::db::DbIdentity {
            node: node(0xcb),
            author: AuthorId::from_bytes([0xcb; 16]),
        },
        Box::new(storage),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut owner)) = opening.poll(&mut context) else {
        panic!("memory must open in its first poll")
    };

    let write = crate::db::block_on(owner.insert("todos", title_cells("volatile"))).unwrap();
    assert_eq!(
        owner.write_state(write.mergeable_tx_id()).unwrap().durability,
        DurabilityTier::None
    );
    assert!(matches!(
        owner.poll_persistence(&mut context),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(
        owner.write_state(write.mergeable_tx_id()).unwrap().durability,
        DurabilityTier::None,
        "completion timing cannot turn volatile memory into local durability"
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
    let std::task::Poll::Ready(Ok(rows)) =
        owner.poll_all(&mut context, &prepared, opts.clone())
    else {
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

    let tx_id = crate::db::block_on(owner.begin_mergeable()).unwrap();
    let tx_row_a = RowUuid::from_bytes([0xcf; 16]);
    let tx_row_b = RowUuid::from_bytes([0xd0; 16]);
    crate::db::block_on(owner.mergeable_insert(
        tx_id,
        "todos",
        tx_row_a,
        title_cells("staged a"),
    ))
    .unwrap();
    crate::db::block_on(owner.mergeable_insert(
        tx_id,
        "todos",
        tx_row_b,
        title_cells("staged b"),
    ))
    .unwrap();
    let staged = {
        let mut read = std::pin::pin!(owner.transaction_all(tx_id, &prepared, opts.clone()));
        assert!(std::future::Future::poll(read.as_mut(), &mut context).is_pending());
        assert!(subscription.try_next_event().is_none());
        released.set(true);
        crate::db::block_on(read.as_mut()).expect("released transaction inputs must read")
    };
    assert!(staged.iter().any(|row| row.row_uuid() == tx_row_a));
    assert!(staged.iter().any(|row| row.row_uuid() == tx_row_b));
    released.set(false);
    assert!(
        subscription.try_next_event().is_none(),
        "staged transaction writes must remain invisible"
    );
    let committed = {
        let mut commit = std::pin::pin!(owner.commit_mergeable(tx_id));
        assert!(std::future::Future::poll(commit.as_mut(), &mut context).is_pending());
        assert!(subscription.try_next_event().is_none());
        released.set(true);
        crate::db::block_on(commit.as_mut()).expect("released mergeable inputs must publish once")
    };
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event()
    else {
        panic!("transaction publication must synchronously refresh subscriptions")
    };
    assert_eq!(added.len(), 2);
    assert!(committed.time.0 > 0);
    released.set(true);

    let exclusive_id = crate::db::block_on(owner.begin_exclusive()).unwrap();
    assert!(crate::db::block_on(owner.exclusive_read(
        exclusive_id,
        "todos",
        write.row_uuid(),
    ))
    .unwrap()
    .is_some());
    let exclusive_row = RowUuid::from_bytes([0xd1; 16]);
    crate::db::block_on(owner.exclusive_insert(
        exclusive_id,
        "todos",
        exclusive_row,
        title_cells("exclusive staged"),
    ))
    .unwrap();
    assert!(
        subscription.try_next_event().is_none(),
        "exclusive staging must remain private"
    );
    released.set(false);
    let exclusive_tx = {
        let mut commit = std::pin::pin!(owner.commit_exclusive(exclusive_id));
        assert!(std::future::Future::poll(commit.as_mut(), &mut context).is_pending());
        assert!(subscription.try_next_event().is_none());
        released.set(true);
        crate::db::block_on(commit.as_mut()).expect("released exclusive inputs must publish once")
    };
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) = subscription.try_next_event()
    else {
        panic!("exclusive publication must synchronously refresh subscriptions")
    };
    assert_eq!(added.len(), 1);
    assert!(exclusive_tx.time.0 > committed.time.0);
    released.set(false);
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
    let committed_units = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let storage = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::clone(&committed_units),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut owner)) = opening.poll(&mut context) else {
        panic!("released metadata reads must open the database")
    };
    let exclusive_id = crate::db::block_on(owner.begin_exclusive()).unwrap();
    let exclusive_row = RowUuid::from_bytes([0xd2; 16]);
    crate::db::block_on(owner.exclusive_insert(
        exclusive_id,
        "todos",
        exclusive_row,
        title_cells("cold exclusive seed"),
    ))
    .unwrap();
    released.set(false);
    {
        let mut commit = std::pin::pin!(owner.commit_exclusive(exclusive_id));
        assert!(std::future::Future::poll(commit.as_mut(), &mut context).is_pending());
        released.set(true);
        crate::db::block_on(commit.as_mut()).expect("released exclusive inputs must publish once");
    }
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

    released.set(true);
    let branch = crate::db::block_on(owner.create_branch()).unwrap();
    crate::db::block_on(std::future::poll_fn(|context| {
        owner.poll_persistence(context)
    }))
    .unwrap();
    let resident = owner.resident_node_for_test();
    assert!(!resident
        .borrow()
        .current_branch_partition_exists_for_test("todos", branch));
    assert!(!resident
        .borrow()
        .current_branch_partition_is_durable_for_test("todos", branch));
    let branch_query = owner.prepare_query(&owner.table("todos")).unwrap();
    let branch_opts = crate::db::ReadOpts {
        tier: DurabilityTier::None,
        local_updates: crate::db::LocalUpdates::Immediate,
        propagation: crate::db::Propagation::LocalOnly,
        read_view: crate::protocol::ReadViewSpec {
            source: crate::protocol::ReadViewSourceSpec::Branch { branch: branch.0 },
            ..crate::protocol::ReadViewSpec::default()
        },
        ..crate::db::ReadOpts::default()
    };
    let mut branch_subscription =
        crate::db::block_on(owner.subscribe(&branch_query, branch_opts.clone())).unwrap();
    let Some(crate::db::SubscriptionEvent::Delta {
        reset: true, added, ..
    }) = branch_subscription.try_next_event()
    else {
        panic!("the empty branch subscription must open before its first partition")
    };
    assert!(added.is_empty());
    assert!(resident
        .borrow()
        .current_branch_partition_exists_for_test("todos", branch));
    assert!(!resident
        .borrow()
        .current_branch_partition_is_durable_for_test("todos", branch));
    let units_before_branch_write = committed_units.borrow().len();
    released.set(false);
    let branch_write = {
        let mut insert = std::pin::pin!(owner.insert_on_branch(
            branch,
            "todos",
            title_cells("first async branch row"),
        ));
        assert!(std::future::Future::poll(insert.as_mut(), &mut context).is_pending());
        assert!(
            !resident
                .borrow()
                .current_branch_partition_is_durable_for_test("todos", branch),
            "cold preparation must not publish the durable partition marker"
        );
        released.set(true);
        crate::db::block_on(insert.as_mut()).expect("released branch inputs must publish once")
    };
    assert!(resident
        .borrow()
        .current_branch_partition_exists_for_test("todos", branch));
    assert!(resident
        .borrow()
        .current_branch_partition_is_durable_for_test("todos", branch));
    assert_ne!(branch_write.row_uuid(), row);
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) =
        branch_subscription.try_next_event()
    else {
        panic!("the first branch row must synchronously refresh its open subscription")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), branch_write.row_uuid());
    released.set(false);
    let std::task::Poll::Ready(Ok(branch_rows)) =
        owner.poll_all(&mut context, &branch_query, branch_opts)
    else {
        panic!("the first branch row must be synchronously queryable before durability")
    };
    assert_eq!(branch_rows.len(), 1);
    assert_eq!(branch_rows[0].row_uuid(), branch_write.row_uuid());
    released.set(true);
    crate::db::block_on(std::future::poll_fn(|context| {
        owner.poll_persistence(context)
    }))
    .unwrap();
    let committed = committed_units.borrow();
    assert_eq!(committed.len(), units_before_branch_write + 1);
    assert!(
        committed.last().copied().unwrap_or_default() >= 3,
        "one durable unit must contain partition metadata, transaction, and row"
    );
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
    let durable = MemoryStorage::new(&refs);
    let fail_commits = std::rc::Rc::new(std::cell::Cell::new(false));
    let backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable.clone()),
        released: std::rc::Rc::new(std::cell::Cell::new(true)),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::clone(&fail_commits),
    };
    let mut opening = PollableNodeOpen::new(node(0xd6), node_schema.clone(), Box::new(backend));
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
    drop(runtime);

    fail_commits.set(false);
    let recovery_backend = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(durable),
        released: std::rc::Rc::new(std::cell::Cell::new(true)),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits,
    };
    let mut reopening = PollableNodeOpen::new(node(0xd6), node_schema, Box::new(recovery_backend));
    let std::task::Poll::Ready(Ok(mut recovered)) = reopening.poll(&mut context) else {
        panic!("a fresh storage session must recover the last coherent durable state")
    };
    let std::task::Poll::Ready(Ok(rows)) =
        recovered.poll_current_rows(&mut context, "todos", DurabilityTier::None)
    else {
        panic!("recovered current rows must be queryable")
    };
    assert!(rows.is_empty(), "the failed optimistic write must not survive reopen");
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
fn cold_relay_ingress_suspends_and_withholds_callbacks_until_durable() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcb), 10).cells(title_cells("cold relay")),
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
        node(0xcb),
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
    let mut opening = PollableNodeOpen::new(node(0xcb), node_schema, Box::new(backend));
    let std::task::Poll::Ready(Ok(mut relay)) = opening.poll(&mut context) else {
        panic!("checkpointed relay must reopen")
    };
    let std::task::Poll::Ready(Ok(history)) =
        relay.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("empty history must open before gating storage")
    };
    assert!(history.recv().unwrap().is_empty());

    released.set(false);
    assert!(relay
        .poll_ingest_relay_commit_unit(&mut context, tx.clone(), versions.clone())
        .is_pending());
    assert!(relay.node.borrow_mut().transaction_record(tx_id).is_none());
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    released.set(true);
    let outcome = loop {
        match relay.poll_ingest_relay_commit_unit(&mut context, tx.clone(), versions.clone()) {
            std::task::Poll::Pending => {}
            ready => break ready,
        }
    };
    let std::task::Poll::Ready(Ok(())) = outcome else {
        panic!("released relay ingress must complete: {outcome:?}")
    };
    assert_eq!(history.recv().unwrap().to_values().unwrap().len(), 1);
    let stored = relay
        .node
        .borrow_mut()
        .transaction_record(tx_id)
        .expect("durable relay ingress must publish its transaction");
    assert_eq!(stored.fate, Fate::Pending);
    assert_eq!(stored.durability, DurabilityTier::Local);
}

#[test]
fn relay_ingress_quarantines_external_publication_until_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcc), 10).cells(title_cells("relay commit")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let mut relay = authority_runtime(
        std::rc::Rc::clone(&released),
        std::rc::Rc::new(std::cell::Cell::new(false)),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(history)) =
        relay.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("empty relay history must open")
    };
    assert!(history.recv().unwrap().is_empty());

    released.set(false);
    assert!(relay
        .poll_ingest_relay_commit_unit(&mut context, tx.clone(), versions.clone())
        .is_pending());
    assert!(
        relay.node.borrow_mut().transaction_record(tx_id).is_some(),
        "resident ingress publishes once before its durable request completes"
    );
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    released.set(true);
    assert!(matches!(
        relay.poll_ingest_relay_commit_unit(&mut context, tx, versions),
        std::task::Poll::Ready(Ok(()))
    ));
    assert_eq!(history.recv().unwrap().to_values().unwrap().len(), 1);
}

#[test]
fn peer_view_update_withholds_subscription_publication_until_durable() {
    let (mut writer, _) = fail_write_many_node();
    let (mut core, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xce), 10).cells(title_cells("peer view")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, 10).unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let update = core.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate {
        subscription,
        settled_through,
        reset_result_set,
        version_carriers,
        version_bundles,
        peer_payload_inventory,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
    } = update
    else {
        panic!("current rows must produce a view update")
    };
    let parts = ViewUpdateParts {
        subscription,
        settled_through,
        defer_settlement: false,
        reset_result_set,
        version_carriers,
        version_bundles,
        peer_complete_tx_payload_refs: peer_payload_inventory.complete_tx_payloads,
        authorization_progress: peer_payload_inventory.authorization_progress,
        opening_pending: peer_payload_inventory.opening_pending,
        result_member_adds,
        result_member_removes,
        terminal_operations,
        program_fact_adds,
        program_fact_removes,
    };

    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let mut receiver = authority_runtime(
        std::rc::Rc::clone(&released),
        std::rc::Rc::new(std::cell::Cell::new(false)),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(history)) =
        receiver.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("empty receiver history must open")
    };
    assert!(history.recv().unwrap().is_empty());

    released.set(false);
    assert!(receiver
        .poll_apply_peer_view_updates(&mut context, std::slice::from_ref(&parts))
        .is_pending());
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));

    released.set(true);
    assert!(matches!(
        receiver.poll_apply_peer_view_updates(&mut context, std::slice::from_ref(&parts)),
        std::task::Poll::Ready(Ok(()))
    ));
    let delta = history.recv().unwrap();
    assert_eq!(delta.to_values().unwrap().len(), 1);
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
}

#[test]
fn peer_repair_payload_withholds_canonical_publication_until_durable() {
    let (mut writer, _) = fail_write_many_node();
    let (mut core, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xca), 10).cells(title_cells("repair payload")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, 10).unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let bundles = version_bundles_for_update(&core.view_update_for_current_rows("todos").unwrap());
    let requests = vec![crate::protocol::RowVersionRef::new(
        "todos",
        row(0xca),
        tx_id,
    )];
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let mut receiver = authority_runtime(
        std::rc::Rc::clone(&released),
        std::rc::Rc::new(std::cell::Cell::new(false)),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(history)) =
        receiver.poll_subscribe_history(&mut context, "todos")
    else {
        panic!("empty receiver history must open")
    };
    assert!(history.recv().unwrap().is_empty());

    released.set(false);
    assert!(receiver
        .poll_apply_peer_repair_payloads(&mut context, &requests, &bundles)
        .is_pending());
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    released.set(true);
    loop {
        match receiver.poll_apply_peer_repair_payloads(&mut context, &requests, &bundles) {
            std::task::Poll::Pending => assert!(matches!(
                history.try_recv(),
                Err(std::sync::mpsc::TryRecvError::Empty)
            )),
            std::task::Poll::Ready(Ok(())) => break,
            std::task::Poll::Ready(Err(error)) => panic!("repair failed: {error}"),
        }
    }
    assert_eq!(history.recv().unwrap().to_values().unwrap().len(), 1);
}

#[test]
fn peer_catalogue_snapshot_is_owned_until_durable() {
    let snapshot = catalogue_snapshot_fixture();
    let expected_schema = snapshot.current_write_schema.schema;
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let mut receiver = authority_runtime(
        std::rc::Rc::clone(&released),
        std::rc::Rc::new(std::cell::Cell::new(false)),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    released.set(false);
    assert!(receiver
        .poll_apply_peer_catalogue_snapshot(&mut context, &snapshot)
        .is_pending());
    released.set(true);
    loop {
        match receiver.poll_apply_peer_catalogue_snapshot(&mut context, &snapshot) {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Ok(())) => break,
            std::task::Poll::Ready(Err(error)) => panic!("catalogue snapshot failed: {error}"),
        }
    }
    assert_eq!(
        receiver.resident().current_write_schema().unwrap().schema,
        expected_schema
    );
}

#[test]
fn peer_branch_metadata_is_owned_until_durable() {
    let metadata = crate::protocol::BranchMetadata {
        branch_id: BranchId::from_bytes([0xcb; 16]),
        created_by: AuthorId::from_bytes([0xcb; 16]),
        parent: None,
        base: None,
        open: true,
    };
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let mut receiver = authority_runtime(
        std::rc::Rc::clone(&released),
        std::rc::Rc::new(std::cell::Cell::new(false)),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    released.set(false);
    assert!(receiver
        .poll_apply_peer_branch_metadata(&mut context, &metadata)
        .is_pending());
    released.set(true);
    loop {
        match receiver.poll_apply_peer_branch_metadata(&mut context, &metadata) {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Ok(())) => break,
            std::task::Poll::Ready(Err(error)) => panic!("branch metadata failed: {error}"),
        }
    }
    assert_eq!(
        receiver.resident().branch_record(metadata.branch_id).cloned(),
        Some(BranchRecord {
            branch_id: metadata.branch_id,
            created_by: metadata.created_by,
            parent: None,
            base: None,
            state: crate::node::codec::BranchState::Open,
        })
    );
}

#[test]
fn demand_driven_peer_tick_retains_view_update_until_durable() {
    let (mut writer, _) = fail_write_many_node();
    let (mut core, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcd), 10).cells(title_cells("wire view")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, 10).unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalSeq(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let update = core.view_update_for_current_rows("todos").unwrap();

    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xcd),
        author: AuthorId::from_bytes([0xcd; 16]),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut receiver)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven database")
    };
    let prepared = receiver.prepare_query(&receiver.table("todos")).unwrap();
    let mut subscription = futures::executor::block_on(receiver.subscribe(
        &prepared,
        crate::db::ReadOpts {
            tier: DurabilityTier::None,
            local_updates: crate::db::LocalUpdates::Immediate,
            propagation: crate::db::Propagation::LocalOnly,
            ..crate::db::ReadOpts::default()
        },
    ))
    .unwrap();
    assert!(matches!(
        futures::executor::block_on(subscription.next_event()),
        Some(crate::db::SubscriptionEvent::Delta { reset: true, .. })
    ));
    let inbound = std::rc::Rc::new(std::cell::RefCell::new(
        std::collections::VecDeque::from([update]),
    ));
    let connection = receiver.connect_upstream(Box::new(QueuedInboundTransport {
        inbound: std::rc::Rc::clone(&inbound),
        outbound: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        session_context: None,
    }));
    released.set(false);
    assert!(receiver.poll_tick(&mut context).is_pending());
    assert!(connection.borrow().has_staged_view_update_for_test());
    assert!(subscription.try_next_event().is_none());

    released.set(true);
    let mut completed = false;
    for _ in 0..16 {
        match receiver.poll_tick(&mut context) {
            std::task::Poll::Pending => assert!(subscription.try_next_event().is_none()),
            std::task::Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("receiver tick failed: {error}"),
        }
    }
    assert!(completed, "receiver batch must durably complete");
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) =
        subscription.try_next_event()
    else {
        panic!("durable view update must publish one subscription delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), row(0xcd));
    assert!(!connection.borrow().has_staged_view_update_for_test());
    assert!(subscription.try_next_event().is_none());
}

#[test]
fn demand_driven_peer_tick_retains_relay_frame_across_async_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcf), 10).cells(title_cells("peer relay")),
        )
        .unwrap();
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xcf),
        author: AuthorId::from_bytes([0xcf; 16]),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut relay)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven database")
    };
    let prepared = relay.prepare_query(&relay.table("todos")).unwrap();
    let mut subscription = futures::executor::block_on(relay.subscribe(
        &prepared,
        crate::db::ReadOpts {
            tier: DurabilityTier::None,
            local_updates: crate::db::LocalUpdates::Immediate,
            propagation: crate::db::Propagation::LocalOnly,
            ..crate::db::ReadOpts::default()
        },
    ))
    .unwrap();
    assert!(matches!(
        futures::executor::block_on(subscription.next_event()),
        Some(crate::db::SubscriptionEvent::Delta { reset: true, .. })
    ));

    let accepted = SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Accepted,
        global_seq: None,
        durability: Some(DurabilityTier::Edge),
    };
    let inbound = std::rc::Rc::new(std::cell::RefCell::new(
        std::collections::VecDeque::from([unit, accepted]),
    ));
    let _connection = relay.connect_upstream(Box::new(QueuedInboundTransport {
        inbound: std::rc::Rc::clone(&inbound),
        outbound: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        session_context: None,
    }));
    released.set(false);
    assert!(relay.poll_tick(&mut context).is_pending());
    assert!(subscription.try_next_event().is_none());

    released.set(true);
    assert!(
        relay.poll_tick(&mut context).is_pending(),
        "the following fate frame must remain staged after the relay commit"
    );
    let Some(crate::db::SubscriptionEvent::Delta { added, .. }) =
        subscription.try_next_event()
    else {
        panic!("durable peer ingress must release one subscription delta")
    };
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].row_uuid(), row(0xcf));
    released.set(false);
    assert!(
        relay.poll_tick(&mut context).is_pending(),
        "the accepted fate frame must remain owned until its durable unit completes"
    );
    released.set(true);
    assert!(matches!(relay.poll_tick(&mut context), std::task::Poll::Ready(Ok(_))));
    assert!(subscription.try_next_event().is_none());
    let rows = futures::executor::block_on(relay.all(
        &prepared,
        crate::db::ReadOpts {
            tier: DurabilityTier::Local,
            propagation: crate::db::Propagation::LocalOnly,
            ..crate::db::ReadOpts::default()
        },
    ))
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(relay.write_state(tx_id).unwrap().fate, Fate::Accepted);
    assert_eq!(
        relay.write_state(tx_id).unwrap().durability,
        DurabilityTier::Edge
    );
}

#[test]
fn routed_peer_fate_reaches_downstream_only_after_durable_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd0), 10).cells(title_cells("routed fate")),
        )
        .unwrap();
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xd0),
        author: AuthorId::from_bytes([0xd0; 16]),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut edge)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven edge")
    };
    let inbound = std::rc::Rc::new(std::cell::RefCell::new(
        std::collections::VecDeque::from([unit]),
    ));
    let connection = edge.connect_upstream(Box::new(QueuedInboundTransport {
        inbound: std::rc::Rc::clone(&inbound),
        outbound: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        session_context: Some(crate::db::ConnectionSessionContext {
            local: crate::wire::WireAuthorityEndpoint {
                node: identity.node,
                epoch: 1,
            },
            remote: crate::wire::WireAuthorityEndpoint {
                node: node(0xa0),
                epoch: 2,
            },
            link_identity: identity.author,
            negotiated_features: crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS,
        }),
    }));
    loop {
        match edge.poll_tick(&mut context) {
            std::task::Poll::Pending => {}
            std::task::Poll::Ready(Ok(_)) => break,
            std::task::Poll::Ready(Err(error)) => panic!("relay ingress failed: {error}"),
        }
    }
    let routed = connection
        .borrow()
        .install_selected_fate_route_for_test(tx_id);
    let fate = SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Accepted,
        global_seq: Some(GlobalSeq(1)),
        durability: Some(DurabilityTier::Global),
    };
    inbound.borrow_mut().push_back(fate.clone());

    released.set(false);
    assert!(edge.poll_tick(&mut context).is_pending());
    assert!(routed.borrow().is_empty());

    released.set(true);
    loop {
        match edge.poll_tick(&mut context) {
            std::task::Poll::Pending => assert!(routed.borrow().is_empty()),
            std::task::Poll::Ready(Ok(_)) => break,
            std::task::Poll::Ready(Err(error)) => panic!("routed fate failed: {error}"),
        }
    }
    assert_eq!(routed.borrow().as_slice(), [fate]);
}

#[test]
fn subscriber_relay_acknowledges_local_durability_only_after_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd1), 10).cells(title_cells("subscriber relay")),
        )
        .unwrap();
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xd1),
        author: AuthorId::from_bytes([0xd1; 16]),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut relay)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven relay")
    };
    let inbound = std::rc::Rc::new(std::cell::RefCell::new(
        std::collections::VecDeque::from([unit]),
    ));
    let outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _subscriber = relay.accept_subscriber(
        Box::new(QueuedInboundTransport {
            inbound,
            outbound: std::rc::Rc::clone(&outbound),
            session_context: None,
        }),
        identity.author,
    );

    released.set(false);
    assert!(relay.poll_tick(&mut context).is_pending());
    assert!(outbound.borrow().is_empty());

    released.set(true);
    let mut completed = false;
    for _ in 0..16 {
        match relay.poll_tick(&mut context) {
            std::task::Poll::Pending => assert!(outbound.borrow().is_empty()),
            std::task::Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("subscriber relay failed: {error}"),
        }
    }
    assert!(completed);
    assert!(outbound.borrow().iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id: acknowledged,
            fate: Fate::Pending,
            durability: Some(DurabilityTier::Local),
            ..
        } if *acknowledged == tx_id
    )));
}

#[test]
fn edge_subscriber_acknowledges_and_relays_only_after_durable_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd2), 10).cells(title_cells("edge subscriber")),
        )
        .unwrap();
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xd2),
        author: AuthorId::from_bytes([0xd2; 16]),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut edge)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven edge")
    };
    let authority_outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _authority = edge.connect_upstream(Box::new(QueuedInboundTransport {
        inbound: std::rc::Rc::new(std::cell::RefCell::new(
            std::collections::VecDeque::new(),
        )),
        outbound: std::rc::Rc::clone(&authority_outbound),
        session_context: Some(crate::db::ConnectionSessionContext {
            local: crate::wire::WireAuthorityEndpoint {
                node: identity.node,
                epoch: 1,
            },
            remote: crate::wire::WireAuthorityEndpoint {
                node: node(0xa2),
                epoch: 2,
            },
            link_identity: identity.author,
            negotiated_features: crate::wire::FEATURE_AUTHORIZATION_SCOPE_VIEWS,
        }),
    }));
    let subscriber_outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _subscriber = edge.accept_edge_authority_subscriber_with_claims(
        Box::new(QueuedInboundTransport {
            inbound: std::rc::Rc::new(std::cell::RefCell::new(
                std::collections::VecDeque::from([unit]),
            )),
            outbound: std::rc::Rc::clone(&subscriber_outbound),
            session_context: None,
        }),
        identity.author,
        std::collections::BTreeMap::new(),
    );

    released.set(false);
    assert!(edge.poll_tick(&mut context).is_pending());
    assert!(subscriber_outbound.borrow().is_empty());
    assert!(authority_outbound.borrow().is_empty());

    released.set(true);
    let mut completed = false;
    for _ in 0..16 {
        match edge.poll_tick(&mut context) {
            std::task::Poll::Pending => {
                assert!(subscriber_outbound.borrow().is_empty());
                assert!(authority_outbound.borrow().is_empty());
            }
            std::task::Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("edge subscriber failed: {error}"),
        }
    }
    assert!(completed);
    assert!(subscriber_outbound.borrow().iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id: acknowledged,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Edge),
            ..
        } if *acknowledged == tx_id
    )));
    assert!(authority_outbound.borrow().iter().any(|message| matches!(
        message,
        SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id
    )));
}

#[test]
fn authority_subscriber_releases_terminal_fate_only_after_durable_commit() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd3), 10).cells(title_cells("authority subscriber")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("writer must produce a commit unit")
    };
    let writer_id = tx.made_by;
    let unit = SyncMessage::CommitUnit { tx, versions };
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xd3),
        author: AuthorId::SYSTEM,
    };
    let mut opening =
        crate::db::PollableDbOpen::new_history_complete(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut authority)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven authority")
    };
    let outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _subscriber = authority.accept_subscriber(
        Box::new(QueuedInboundTransport {
            inbound: std::rc::Rc::new(std::cell::RefCell::new(
                std::collections::VecDeque::from([unit]),
            )),
            outbound: std::rc::Rc::clone(&outbound),
            session_context: None,
        }),
        writer_id,
    );

    released.set(false);
    assert!(authority.poll_tick(&mut context).is_pending());
    assert!(outbound.borrow().is_empty());

    released.set(true);
    let mut completed = false;
    for _ in 0..16 {
        match authority.poll_tick(&mut context) {
            std::task::Poll::Pending => assert!(outbound.borrow().is_empty()),
            std::task::Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("authority subscriber failed: {error}"),
        }
    }
    assert!(completed);
    assert!(outbound.borrow().iter().any(|message| matches!(
        message,
        SyncMessage::FateUpdate {
            tx_id: acknowledged,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        } if *acknowledged == tx_id
    )));
}

#[test]
fn session_branch_metadata_echo_waits_for_durable_commit() {
    let identity = crate::db::DbIdentity {
        node: node(0xd4),
        author: AuthorId::from_bytes([0xd4; 16]),
    };
    let metadata = crate::protocol::BranchMetadata {
        branch_id: BranchId::from_bytes([0xd4; 16]),
        created_by: identity.author,
        parent: None,
        base: Some(crate::tx::Snapshot {
            owner: NodeUuid(uuid::Uuid::nil()),
            global_base: GlobalSeq::default(),
            local_base: TxTime::default(),
            dots: Vec::new(),
        }),
        open: true,
    };
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let mut opening = crate::db::PollableDbOpen::new(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut relay)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven relay")
    };
    let outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _subscriber = relay.accept_subscriber(
        Box::new(QueuedInboundTransport {
            inbound: std::rc::Rc::new(std::cell::RefCell::new(
                std::collections::VecDeque::from([SyncMessage::BranchMetadata(
                    metadata.clone(),
                )]),
            )),
            outbound: std::rc::Rc::clone(&outbound),
            session_context: None,
        }),
        identity.author,
    );

    released.set(false);
    assert!(relay.poll_tick(&mut context).is_pending());
    assert!(outbound.borrow().is_empty());

    released.set(true);
    let mut completed = false;
    for _ in 0..16 {
        match relay.poll_tick(&mut context) {
            std::task::Poll::Pending => assert!(outbound.borrow().is_empty()),
            std::task::Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("session metadata failed: {error}"),
        }
    }
    assert!(completed);
    assert!(outbound.borrow().contains(&SyncMessage::BranchMetadata(metadata)));
}

#[test]
fn incremental_catalogue_ack_waits_for_durable_commit() {
    let node_schema = schema();
    let message = SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(crate::protocol::SchemaVersion::new(node_schema.clone())),
    };
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xd5),
        author: AuthorId::SYSTEM,
    };
    let mut opening =
        crate::db::PollableDbOpen::new_history_complete(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut authority)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven authority")
    };
    let outbound = std::rc::Rc::new(std::cell::RefCell::new(Vec::new()));
    let _subscriber = authority.accept_subscriber_with_claims_and_trust(
        Box::new(QueuedInboundTransport {
            inbound: std::rc::Rc::new(std::cell::RefCell::new(
                std::collections::VecDeque::from([message]),
            )),
            outbound: std::rc::Rc::clone(&outbound),
            session_context: None,
        }),
        AuthorId::SYSTEM,
        std::collections::BTreeMap::new(),
        crate::node::CommitUnitTrust::TrustedBackend,
    );

    released.set(false);
    assert!(authority.poll_tick(&mut context).is_pending());
    assert!(!outbound
        .borrow()
        .iter()
        .any(|message| matches!(message, SyncMessage::CatalogueAck(_))));

    released.set(true);
    let mut completed = false;
    for _ in 0..16 {
        match authority.poll_tick(&mut context) {
            std::task::Poll::Pending => assert!(!outbound
                .borrow()
                .iter()
                .any(|message| matches!(message, SyncMessage::CatalogueAck(_)))),
            std::task::Poll::Ready(Ok(_)) => {
                completed = true;
                break;
            }
            std::task::Poll::Ready(Err(error)) => panic!("catalogue publish failed: {error}"),
        }
    }
    assert!(completed);
    assert!(outbound
        .borrow()
        .iter()
        .any(|message| matches!(message, SyncMessage::CatalogueAck(_))));
}

#[test]
fn local_catalogue_publication_waits_for_the_same_durable_boundary() {
    let node_schema = schema();
    let message = SyncMessage::PublishSchema {
        author: AuthorId::SYSTEM,
        schema: Box::new(crate::protocol::SchemaVersion::new(node_schema.clone())),
    };
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let mut opening = PollableNodeOpen::new_history_complete(
        node(0xd4),
        node_schema,
        Box::new(storage),
    );
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut runtime)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven authority")
    };

    released.set(false);
    assert!(runtime
        .poll_apply_trusted_catalogue_message(&mut context, &message)
        .is_pending());

    released.set(true);
    let std::task::Poll::Ready(Ok(responses)) =
        runtime.poll_apply_trusted_catalogue_message(&mut context, &message)
    else {
        panic!("released catalogue publication must finish")
    };
    assert!(responses
        .iter()
        .any(|message| matches!(message, SyncMessage::CatalogueAck(_))));
}

#[test]
fn authority_bootstrap_seed_is_not_settled_before_async_ingest_commits() {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
    let released = std::rc::Rc::new(std::cell::Cell::new(true));
    let storage = CommitGatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(MemoryStorage::new(&refs)),
        released: std::rc::Rc::clone(&released),
        fail: std::rc::Rc::new(std::cell::Cell::new(false)),
        completed: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
    };
    let identity = crate::db::DbIdentity {
        node: node(0xd3),
        author: AuthorId::SYSTEM,
    };
    let mut opening =
        crate::db::PollableDbOpen::new_history_complete(node_schema, identity, Box::new(storage));
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let std::task::Poll::Ready(Ok(mut authority)) = opening.poll(&mut context) else {
        panic!("commit-only gate must open the demand-driven authority")
    };

    released.set(false);
    let tx_id = {
        let mut seed = std::pin::pin!(authority.seed_settled_mergeable_for_bootstrap(
            "todos",
            row(0xd3),
            AuthorId::SYSTEM,
            title_cells("seeded"),
        ));
        assert!(seed.as_mut().poll(&mut context).is_pending());
        released.set(true);
        let std::task::Poll::Ready(Ok(tx_id)) = seed.as_mut().poll(&mut context) else {
            panic!("released seed must settle through authority ingest")
        };
        tx_id
    };
    assert_eq!(
        authority.write_state(tx_id).unwrap().durability,
        DurabilityTier::Global
    );
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
