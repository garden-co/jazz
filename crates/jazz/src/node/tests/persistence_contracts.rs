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
            inner: MemoryStorage::new(column_families).expect("valid memory storage families"),
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

impl OrderedKvStorage for FailWriteManyMemoryStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<Option<StorageValue>, groove::storage::Error>> {
        self.inner.get(cf, key)
    }

    fn put_if_absent(&self, cf: String, key: Vec<u8>, value: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<Option<StorageValue>, groove::storage::Error>> {
        self.inner.put_if_absent(cf, key, value)
    }

    fn compare_and_delete(&self, cf: String, key: Vec<u8>, expected: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<bool, groove::storage::Error>> {
        self.inner.compare_and_delete(cf, key, expected)
    }

    fn set(
        &self,
        cf: String,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> groove::storage::StorageFuture<'_, Result<(), groove::storage::Error>> {
        self.inner.set(cf, key, value)
    }

    fn delete(&self, cf: String, key: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<(), groove::storage::Error>> {
        self.inner.delete(cf, key)
    }

    fn scan(&self, request: groove::storage::ScanRequest) -> groove::storage::StorageFuture<'_, Result<groove::storage::StorageScan<'_>, groove::storage::Error>> {
        self.inner.scan(request)
    }

    fn write_many(&self, operations: Vec<groove::storage::OwnedWriteOperation>) -> groove::storage::StorageFuture<'_, Result<(), groove::storage::Error>> {
        let call = self.write_many_calls.get() + 1;
        self.write_many_calls.set(call);
        if self.fail_on_write_many.get() == Some(call) {
            self.fail_on_write_many.set(None);
            return Box::pin(async { Err(groove::storage::Error::InvalidStorageLayout("injected durable commit failure".to_owned())) });
        }
        self.inner.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for FailWriteManyMemoryStorage {
    fn reopen(self, column_families: Vec<String>) -> groove::storage::StorageFuture<'static, Result<Self, groove::storage::Error>> {
        Box::pin(async move {
            let Self { inner, fail_on_write_many, write_many_calls } = self;
            Ok(Self {
                inner: inner.reopen(column_families).await?,
                fail_on_write_many,
                write_many_calls,
            })
        })
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
        core.subscribe_history("todos").resolve(),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));
    assert!(matches!(
        core.current_rows("todos", DurabilityTier::Global).resolve(),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));
    assert!(matches!(
        core.query_table_versions("todos").resolve(),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));
    assert!(matches!(
        core.database.flush().resolve(),
        Err(groove::db::Error::DatabasePoisoned)
    ));
}

/// A multi-row local commit is one immediate resident publication even when
/// its later atomic storage batch fails. Durable reopen still observes none of
/// the transaction; the live database is poisoned instead of retracting rows.
#[test]
fn failed_multi_row_local_commit_is_fully_resident_but_not_partially_durable() {
    let (mut writer, storage) = fail_write_many_node();
    let history = writer.subscribe_history("todos").unwrap();
    assert!(
        history.recv().unwrap().is_empty(),
        "subscription setup must start from the empty durable history"
    );
    storage.fail_nth_following_write_many(1);

    let error = writer
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("todos", row(0xd1), 10).cells(title_cells("first")),
            MergeableCommit::new("todos", row(0xd2), 10).cells(title_cells("second")),
        ])
        .expect_err("injected final persistence failure must fail the full commit unit");
    assert!(
        matches!(error, Error::Groove(groove::db::Error::Storage(_))),
        "unexpected durable-commit error: {error:?}"
    );
    assert_eq!(
        history.try_recv().unwrap().to_values().unwrap().len(),
        2,
        "the complete resident commit unit must remain synchronously visible"
    );
    assert_poisoned_node_exposes_nothing(&mut writer);

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
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0xd3), 10).cells(title_cells("authority")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    storage.fail_nth_following_write_many(1);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let error = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
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

/// Authority finalization stores canonical state and cleanup atomically, then
/// releases exactly one subscription tick and its fate acknowledgement.
#[test]
fn successful_authority_finalization_uses_one_atomic_storage_batch() {
    let (mut writer, _) = fail_write_many_node();
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
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
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();

    assert_eq!(
        storage.write_many_call_count() - writes_before,
        1,
        "authority ingest must persist canonical state and cleanup in one atomic batch"
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

/// Authority ingress holds subscription output until its one durable batch
/// succeeds; a failed batch emits neither resident deltas nor fate output.
#[test]
fn authority_persistence_failure_publishes_no_resident_subscription_output() {
    let (mut writer, _) = fail_write_many_node();
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0xd7), 10).cells(title_cells("nested failure")),
        )
        .unwrap();

    let (mut core, storage) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    storage.fail_nth_following_write_many(1);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("persistence failure must fail the commit unit");
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_poisoned_node_exposes_nothing(&mut core);
}

/// A complete commit unit is one Groove publication and its resident delta is
/// delivered immediately, exactly once.
#[test]
fn commit_unit_publishes_one_resident_delta_immediately() {
    let (mut writer, _) = fail_write_many_node();
    let (_tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0xd8), 10).cells(title_cells("nested success")),
        )
        .unwrap();

    let (mut core, _) = fail_write_many_node();
    let history = core.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };
    let updates = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert!(matches!(
        updates.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Global),
            ..
        }]
    ));
    assert_eq!(history.try_recv().unwrap().to_values().unwrap().len(), 1);
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
}
