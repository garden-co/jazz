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

#[test]
fn jazz_commit_emits_owned_persistence_batches_after_local_visibility() {
    let (mut writer, _) = fail_write_many_node();
    writer.enable_async_persistence_capture();
    let tx_id = writer
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xcf), 10).cells(title_cells("resident")),
        )
        .unwrap();

    assert_eq!(
        writer
            .current_rows("todos", DurabilityTier::None)
            .unwrap()
            .len(),
        1,
        "the Jazz local frontier must advance before durability is driven"
    );
    let batches = writer.take_pending_persistence_batches();
    assert!(!batches.is_empty());
    assert!(
        batches
            .into_iter()
            .flat_map(|batch| batch.into_operations())
            .any(|operation| matches!(
                operation,
                groove::storage::OwnedWriteOperation::Set { ref cf, .. }
                    if cf == "jazz_transactions"
            )),
        "the captured closure must include the canonical transaction"
    );
    assert!(writer.transaction_record(tx_id).is_some());
}

#[test]
fn async_authority_ingest_owns_complete_persistence_before_fate_publication() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xce), 10).cells(title_cells("quarantined")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };

    let (mut authority, _) = fail_write_many_node();
    let pending = authority
        .ingest_commit_unit_for_async_persistence(
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        )
        .unwrap();

    // Resident evaluation has completed, but the only externally publishable
    // fate is still owned by `pending` rather than returned independently.
    assert_eq!(
        authority.transaction_record(tx_id).unwrap().fate,
        Fate::Accepted
    );
    let (batches, responses, _poison) = pending.into_parts();
    assert!(batches.len() >= 2, "ingest and finalization form one unit");
    assert!(batches.iter().any(|batch| {
        batch.clone().into_operations().iter().any(|operation| {
            matches!(
                operation,
                groove::storage::OwnedWriteOperation::Set { cf, .. }
                    if cf == "jazz_transactions"
            )
        })
    }));
    assert_eq!(responses.len(), 1);
    assert!(matches!(
        &responses[0],
        SyncMessage::FateUpdate {
            tx_id: response_tx,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        } if *response_tx == tx_id
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

fn persistence_storage_for(pending: &PendingAuthorityPublication) -> MemoryStorage {
    let column_families = pending
        .persistence
        .iter()
        .flat_map(|batch| batch.clone().into_operations())
        .map(|operation| match operation {
            groove::storage::OwnedWriteOperation::Set { cf, .. }
            | groove::storage::OwnedWriteOperation::Delete { cf, .. }
            | groove::storage::OwnedWriteOperation::Delta { cf, .. } => cf,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    MemoryStorage::new(&refs)
}

#[test]
fn authority_scheduler_releases_fate_only_after_async_storage_completion() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcd), 10).cells(title_cells("scheduled")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let (mut authority, _) = fail_write_many_node();
    let pending = authority
        .ingest_commit_unit_for_async_persistence(
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        )
        .unwrap();

    let released = std::rc::Rc::new(std::cell::Cell::new(false));
    let storage = GatedAuthorityStorage {
        inner: groove::storage::async_ordered::ImmediateStorage::new(persistence_storage_for(
            &pending,
        )),
        released: std::rc::Rc::clone(&released),
        cancellations: std::rc::Rc::new(std::cell::Cell::new(0)),
        committed_units: std::rc::Rc::new(std::cell::RefCell::new(Vec::new())),
        fail_commits: std::rc::Rc::new(std::cell::Cell::new(false)),
    };
    let mut scheduler = AuthorityPersistenceScheduler::new(Box::new(storage));
    scheduler.enqueue(pending);
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);

    assert!(scheduler.poll(&mut context).is_pending());
    assert!(scheduler.has_pending());
    released.set(true);
    let outcome = scheduler.poll(&mut context);
    let std::task::Poll::Ready(Ok(responses)) = outcome else {
        panic!("released persistence unit must complete: {outcome:?}")
    };
    assert!(!scheduler.has_pending());
    assert!(matches!(
        responses.as_slice(),
        [SyncMessage::FateUpdate {
            tx_id: response_tx,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        }] if *response_tx == tx_id
    ));
}

#[test]
fn authority_scheduler_uses_same_first_poll_path_for_immediate_storage() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcc), 10).cells(title_cells("immediate")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let (mut authority, _) = fail_write_many_node();
    let pending = authority
        .ingest_commit_unit_for_async_persistence(
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        )
        .unwrap();
    let durable = persistence_storage_for(&pending);
    let mut scheduler = AuthorityPersistenceScheduler::new(Box::new(
        groove::storage::async_ordered::ImmediateStorage::new(durable),
    ));
    scheduler.enqueue(pending);
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    let outcome = scheduler.poll(&mut context);
    let std::task::Poll::Ready(Ok(responses)) = outcome else {
        panic!("immediate storage must complete through its first scheduler poll: {outcome:?}")
    };
    assert!(matches!(
        responses.as_slice(),
        [SyncMessage::FateUpdate { tx_id: response_tx, .. }] if *response_tx == tx_id
    ));
}

#[test]
fn authority_persistence_failure_discards_fate_and_poisons_resident_node() {
    let (mut writer, _) = fail_write_many_node();
    let (_, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xcb), 10).cells(title_cells("must poison")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let (mut authority, _) = fail_write_many_node();
    let pending = authority
        .ingest_commit_unit_for_async_persistence(
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
            None,
        )
        .unwrap();
    let column_families = pending
        .persistence
        .iter()
        .flat_map(|batch| batch.clone().into_operations())
        .map(|operation| match operation {
            groove::storage::OwnedWriteOperation::Set { cf, .. }
            | groove::storage::OwnedWriteOperation::Delete { cf, .. }
            | groove::storage::OwnedWriteOperation::Delta { cf, .. } => cf,
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let persistence = FailWriteManyMemoryStorage::new(&refs);
    persistence.fail_nth_following_write_many(1);
    let mut scheduler = AuthorityPersistenceScheduler::new(Box::new(
        groove::storage::async_ordered::ImmediateStorage::new(persistence),
    ));
    scheduler.enqueue(pending);
    let waker = std::task::Waker::from(std::sync::Arc::new(PersistenceTestWake));
    let mut context = std::task::Context::from_waker(&waker);
    assert!(matches!(
        scheduler.poll(&mut context),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(matches!(
        authority.current_rows("todos", DurabilityTier::Global),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
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

/// The synchronous implementation currently has a deliberate recovery window
/// between durable ingest and cleanup/consistency-marker finalization. A
/// failure in that second boundary may return no acknowledgement, but reopening
/// must recover one coherent accepted unit before any later view can serve it.
#[test]
fn restart_after_finalization_boundary_failure_recovers_one_coherent_transaction() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd4), 10).cells(title_cells("recovered")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    // Ingest writes canonical transaction/history/current first, then removes
    // obsolete ahead-current rows in a second batch. Interrupt that latter
    // write to exercise the exact crash-recovery seam.
    storage.fail_nth_following_write_many(2);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("interrupted finalization must not acknowledge the unit");
    assert!(
        matches!(history.try_recv(), Err(std::sync::mpsc::TryRecvError::Empty)),
        "durable ingest must not publish before cleanup/finalization succeeds"
    );
    assert_poisoned_node_exposes_nothing(&mut core);

    drop(core);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    let stored = reopened
        .transaction_record(tx_id)
        .expect("restart must recover the canonical transaction");
    assert_eq!(stored.fate, Fate::Accepted);
    assert_eq!(stored.global_seq, Some(GlobalSeq(1)));
    assert_eq!(stored.durability, DurabilityTier::Global);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd4), title_cells("recovered"))]),
        "recovery must expose either the whole accepted transaction or none of it"
    );
    assert_currency_tables_match_storage(&mut reopened, "todos");
}

/// The consistency marker is part of the same publication boundary as both
/// database batches. If only the marker write fails, fate/IVM output still must
/// remain private until reopen proves the stored unit coherent.
#[test]
fn marker_failure_publishes_no_history_or_fate_and_reopens_coherently() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd6), 10).cells(title_cells("marker recovery")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    storage.fail_nth_following_write_many(3);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("failed consistency marker must not acknowledge the unit");
    assert!(
        matches!(
            history.try_recv(),
            Err(std::sync::mpsc::TryRecvError::Empty)
        ),
        "marker failure must discard canonical and cleanup tick notifications"
    );
    assert_poisoned_node_exposes_nothing(&mut core);

    drop(core);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    let stored = reopened
        .transaction_record(tx_id)
        .expect("restart must recover the accepted transaction before serving it");
    assert_eq!(stored.fate, Fate::Accepted);
    assert_eq!(stored.global_seq, Some(GlobalSeq(1)));
    assert_eq!(stored.durability, DurabilityTier::Global);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd6), title_cells("marker recovery"))])
    );
    assert_currency_tables_match_storage(&mut reopened, "todos");
}

/// The successful control proves the same multi-batch authority path releases
/// exactly one subscription tick, and only after both storage commits complete.
#[test]
fn successful_authority_finalization_publishes_after_every_storage_batch() {
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
        3,
        "authority ingest must finish canonical persistence, cleanup, and its marker before returning fate"
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
    storage.fail_nth_following_write_many(2);
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
