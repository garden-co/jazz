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

impl OrderedKvStorage for FailWriteManyMemoryStorage {
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

    fn write_many_outcome(&self, operations: &[WriteOperation<'_>]) -> WriteManyOutcome {
        match self.write_many(operations) {
            Ok(()) => WriteManyOutcome::Committed,
            Err(error) => WriteManyOutcome::DefinitelyNotCommitted(error),
        }
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

/// Deliberately exercises the acknowledgement-loss case: the backing batch is
/// durable, but the adapter reports an error. It intentionally relies on the
/// conservative default `write_many_outcome` classification rather than
/// claiming a pre-commit failure.
#[derive(Clone)]
struct CommitThenErrorMemoryStorage {
    inner: MemoryStorage,
    fail_next_after_commit: std::rc::Rc<std::cell::Cell<bool>>,
}

impl CommitThenErrorMemoryStorage {
    fn new(column_families: &[&str]) -> Self {
        Self {
            inner: MemoryStorage::new(column_families),
            fail_next_after_commit: std::rc::Rc::new(std::cell::Cell::new(false)),
        }
    }

    fn fail_next_after_commit(&self) {
        self.fail_next_after_commit.set(true);
    }
}

impl OrderedKvStorage for CommitThenErrorMemoryStorage {
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
        self.inner.write_many(operations)?;
        if self.fail_next_after_commit.replace(false) {
            return Err(groove::storage::Error::InvalidStorageLayout(
                "injected post-commit acknowledgement failure".to_owned(),
            ));
        }
        Ok(())
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for CommitThenErrorMemoryStorage {
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

fn commit_then_error_node(
) -> (
    NodeState<CommitThenErrorMemoryStorage>,
    CommitThenErrorMemoryStorage,
) {
    let node_schema = schema();
    let column_families = node_schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = CommitThenErrorMemoryStorage::new(&refs);
    let node = NodeState::new(node(0xd1), node_schema, storage.clone()).unwrap();
    (node, storage)
}

fn assert_poisoned_node_exposes_nothing<S: OrderedKvStorage>(core: &mut NodeState<S>) {
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

#[test]
fn authority_transient_storage_failure_accepts_exact_resend_after_storage_heals() {
    // This lower-level fault injection is necessary: a real ENOSPC/write error
    // cannot be produced deterministically through the public client API.
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xd5), 10).cells(title_cells("retry")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };

    let (mut authority, storage) = fail_write_many_node();
    storage.fail_nth_following_write_many(1);
    authority
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("first durable commit is intentionally failed");

    authority
        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect("the exact resend should apply after the transient storage failure clears");
    assert_eq!(
        authority.transaction_record(tx_id).map(|record| record.fate),
        Some(Fate::Accepted)
    );
}

/// Fate persistence and its consistency marker share one publication scope.
/// If the marker write fails after the fate/current batch commits, the live
/// runtime must fail closed rather than serving the newly accepted state.
#[test]
fn fate_marker_failure_poisoned_then_reopen_recovers_accepted_current_row() {
    let (mut core, storage) = fail_write_many_node();
    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xd9), 10).cells(title_cells("marker fate")),
        )
        .unwrap();

    // `apply_fate_update` first commits fate/current changes, then records the
    // consistency marker. Interrupt only the latter write.
    storage.fail_nth_following_write_many(2);
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .expect_err("a failed fate marker must fail-stop the live database");
    assert_poisoned_node_exposes_nothing(&mut core);

    drop(core);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    let stored = reopened
        .transaction_record(tx_id)
        .expect("the pre-marker fate batch is durable and must recover coherently");
    assert_eq!(stored.fate, Fate::Accepted);
    assert_eq!(stored.global_time, Some(GlobalTime(1)));
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xd9), title_cells("marker fate"))])
    );
    assert_currency_tables_match_storage(&mut reopened, "todos");
}

/// An error returned after the storage batch commits is ambiguous, not a
/// retryable failure. The live node must neither publish the speculative tick
/// nor accept a resend; reopening must rebuild the already-durable unit and
/// make it visible as one coherent history/current-row update.
#[test]
fn authority_post_commit_storage_error_poisoned_then_reopen_recovers_visibility() {
    let (mut writer, _) = fail_write_many_node();
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xda), 10).cells(title_cells("ack lost")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("local write must produce a commit unit")
    };

    let (mut authority, storage) = commit_then_error_node();
    let history = authority.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());
    storage.fail_next_after_commit();
    authority
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .expect_err("a post-commit acknowledgement failure must not publish or retry in process");
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_poisoned_node_exposes_nothing(&mut authority);
    assert!(matches!(
        authority.ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS),
        Err(Error::Groove(groove::db::Error::DatabasePoisoned))
    ));

    drop(authority);
    let mut reopened = NodeState::new(node(0xd1), schema(), storage).unwrap();
    let stored = reopened
        .transaction_record(tx_id)
        .expect("the acknowledged-lost batch is nevertheless durable");
    assert_eq!(stored.fate, Fate::Accepted);
    assert_eq!(stored.global_time, Some(GlobalTime::new(10, 0).unwrap()));
    assert_eq!(stored.durability, DurabilityTier::Global);
    assert_eq!(
        reopened
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row(0xda), title_cells("ack lost"))]),
        "reopen must expose the mutation that the failed acknowledgement hid"
    );
    assert_currency_tables_match_storage(&mut reopened, "todos");
}

/// Once the source unit has committed, a later derived-merge failure may not
/// rewind the authority allocator across that durable sequence. The third
/// source unit proves the next allocation is 3 rather than reusing 2.
#[test]
fn derived_merge_failure_after_source_durability_does_not_reuse_global_time() {
    let (mut writer, _) = fail_write_many_node();
    let (_, first) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xdb), 10).cells(title_cells("first head")),
        )
        .unwrap();
    let (_, second) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xdb), 20).cells(title_cells("second head")),
        )
        .unwrap();
    let (_, third) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row(0xdc), 30).cells(title_cells("third source")),
        )
        .unwrap();
    let (mut authority, storage) = fail_write_many_node();
    let history = authority.subscribe_history("todos").unwrap();
    assert!(history.recv().unwrap().is_empty());

    let SyncMessage::CommitUnit {
        tx: first_tx,
        versions: first_versions,
    } = first
    else {
        panic!("local write must produce a commit unit")
    };
    authority
        .ingest_commit_unit(
            first_tx,
            first_versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert_eq!(
        history.recv().unwrap().to_values().unwrap().len(),
        1,
        "the first complete authority unit may publish normally"
    );

    // Authority ingest uses three writes for source canonical persistence,
    // cleanup, and marker finalization. Fail the following derived local
    // merge's initial batch, after source sequence 2 is durable.
    storage.fail_nth_following_write_many(4);
    let SyncMessage::CommitUnit {
        tx: second_tx,
        versions: second_versions,
    } = second
    else {
        panic!("local write must produce a commit unit")
    };
    authority
        .ingest_commit_unit(
            second_tx,
            second_versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .expect_err("derived merge storage failure must interrupt the authority response");
    assert!(matches!(
        history.try_recv(),
        Err(std::sync::mpsc::TryRecvError::Empty)
    ));
    assert_poisoned_node_exposes_nothing(&mut authority);

    drop(authority);
    let mut authority = NodeState::new(node(0xd1), schema(), storage).unwrap();

    let SyncMessage::CommitUnit {
        tx: third_tx,
        versions: third_versions,
    } = third
    else {
        panic!("local write must produce a commit unit")
    };
    let updates = authority
        .ingest_commit_unit(third_tx, third_versions, u64::MAX - SKEW_TOLERANCE_MS)
        .expect("the durable source sequence must remain allocated after a derived failure");
    let [SyncMessage::FateUpdate {
        fate: Fate::Accepted,
        global_time: Some(global_time),
        durability: Some(DurabilityTier::Global),
        ..
    }] = updates.as_slice()
    else {
        panic!("expected one globally accepted fate update: {updates:?}");
    };
    assert!(*global_time > GlobalTime::new(20, 0).unwrap());
    assert_eq!(global_time.physical_ms(), 30);
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
    assert_eq!(stored.global_time, Some(GlobalTime::new(10, 0).unwrap()));
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
    assert_eq!(stored.global_time, Some(GlobalTime::new(10, 0).unwrap()));
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
