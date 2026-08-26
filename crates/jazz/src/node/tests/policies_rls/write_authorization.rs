// Write attribution, ownership, joins, rejection, and cleanup.

use crate::query::{Include, JoinMode, OrderDirection};
use crate::tools::public_api::relation_ir::{
    ColumnRef as PublicRelColumnRef, JoinCondition as PublicRelJoinCondition,
    JoinKind as PublicRelJoinKind, PredicateCmpOp as PublicRelPredicateCmpOp,
    PredicateExpr as PublicRelPredicateExpr, RelExpr as PublicRelExpr,
    ValueRef as PublicRelValueRef,
};
use std::cell::Cell;
use std::rc::Rc;

// The injected read failure is necessary to exercise the externally visible
// retry contract: a failed rejection must leave the transaction pending for a
// later public `finalize_local_mergeable_commit` call.
#[derive(Clone)]
struct FailTransactionReadMemoryStorage {
    inner: MemoryStorage,
    fail_after_transaction_reads: Rc<Cell<Option<usize>>>,
}

impl FailTransactionReadMemoryStorage {
    fn new(column_families: &[&str]) -> Self {
        Self {
            inner: MemoryStorage::new(column_families),
            fail_after_transaction_reads: Rc::new(Cell::new(None)),
        }
    }

    fn fail_after_transaction_reads(&self, successful_reads: usize) {
        self.fail_after_transaction_reads.set(Some(successful_reads));
    }
}

impl OrderedKvStorage for FailTransactionReadMemoryStorage {
    fn get(&self, cf: String, key: Vec<u8>) -> groove::storage::StorageFuture<'_, Result<Option<StorageValue>, groove::storage::Error>> {
        if key
            .windows("jazz_transactions".len())
            .any(|window| window == b"jazz_transactions")
            && let Some(remaining) = self.fail_after_transaction_reads.get()
        {
            if remaining == 0 {
                self.fail_after_transaction_reads.set(None);
                return Box::pin(async { Err(groove::storage::Error::InvalidStorageLayout("injected transaction read failure".to_owned())) });
            }
            self.fail_after_transaction_reads.set(Some(remaining - 1));
        }
        self.inner.get(cf, key)
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
        self.inner.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for FailTransactionReadMemoryStorage {
    fn reopen(self, column_families: Vec<String>) -> groove::storage::StorageFuture<'static, Result<Self, groove::storage::Error>> {
        Box::pin(async move {
            let Self { inner, fail_after_transaction_reads } = self;
            Ok(Self { inner: inner.reopen(column_families).await?, fail_after_transaction_reads })
        })
    }
}

/// Pending local versions are materialized before self-finalization.  Their
/// presence must not turn a new row into an update and thereby let an update
/// policy stand in for a missing or rejecting INSERT policy.
#[test]
fn local_authority_keeps_insert_and_update_policies_distinct() {
    let update_policy = || {
        PublicTablePolicies::new()
            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
    };
    let schema_for = |insert_policy: Option<PublicPolicyExpr>| {
        let policies = insert_policy.map_or_else(update_policy, |insert| {
            update_policy().with_insert(insert)
        });
        build_public_test_schema(PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("owner", PublicColumnType::Uuid)
                .policies(policies),
        ))
    };
    let author = user(0xa1);

    for (label, insert_policy) in [
        ("omitted", None),
        ("false", Some(PublicPolicyExpr::False)),
    ] {
        let schema = schema_for(insert_policy);
        let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
        let tx_id = core
            .commit_mergeable_settled(
                MergeableCommit::new("todos", row(0x91), 10)
                    .made_by(author)
                    .cells(owner_cells(author, "must not insert")),
            )
            .unwrap();
        core.finalize_local_mergeable_commit_settled(tx_id).unwrap();
        assert!(matches!(
            core.transaction_state_settled(tx_id),
            Some((
                Fate::Rejected(RejectionReason::AuthorizationDenied),
                None,
                DurabilityTier::Local
            ))
        ), "{label} INSERT policy must deny a new local row");
    }

    let schema = schema_for(None);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let existing = row(0x92);
    accept_global(
        &mut core,
        MergeableCommit::new("todos", existing, 20).cells(owner_cells(author, "before")),
    );
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", existing, 21)
                .made_by(author)
                .cells(owner_cells(author, "after")),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(tx_id).unwrap();
    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ), "declared UPDATE policy must still permit an existing row");

    let schema = schema_for(None);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let coalesced_row = row(0x94);
    let open_tx = OpenTransactionId::new();
    crate::db::block_on(core.open_mergeable(open_tx, author, Some(author))).unwrap();
    crate::db::block_on(core.tx_write_mergeable(
        open_tx,
        "todos",
        coalesced_row,
        owner_cells(author, "coalesced insert"),
        None,
        Vec::new(),
        Some(25),
        false,
    ))
    .unwrap();
    crate::db::block_on(core.tx_patch_mergeable(
        open_tx,
        "todos",
        coalesced_row,
        BTreeMap::from([("title".to_owned(), Value::String("patched insert".to_owned()))]),
        Some(26),
    ))
    .unwrap();
    let tx_id = core
        .commit_mergeable_open_settled(open_tx, || 27)
        .unwrap();
    core.finalize_local_mergeable_commit_settled(tx_id).unwrap();
    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local
        ))
    ), "coalesced insert-then-update must remain an INSERT for policy purposes");

    let pending_update_denied = PublicTablePolicies::new()
        .with_insert(PublicPolicyExpr::True)
        .with_update(Some(PublicPolicyExpr::False), PublicPolicyExpr::False);
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .policies(pending_update_denied),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let pending_row = row(0x93);
    let first = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", pending_row, 30)
                .made_by(author)
                .cells(owner_cells(author, "first pending insert")),
        )
        .unwrap();
    let second = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", pending_row, 31)
                .made_by(author)
                .cells(owner_cells(author, "second pending update")),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(second).unwrap();
    assert!(matches!(
        core.transaction_state_settled(second),
        Some((
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local
        ))
    ), "a newer pending row must classify against the older pending insert");
    assert!(matches!(
        core.transaction_state_settled(first),
        Some((Fate::Pending, None, DurabilityTier::Local))
    ), "the predecessor remains independently pending");
}

/// This stays at the node boundary because admission evaluates policy-pinned
/// inline rows before a public client receives a write outcome. It proves the
/// provenance visible to that inline program matches the public milliseconds
/// contract at both its persisted-old-row and incoming-version boundaries.
#[test]
fn write_policy_timestamp_provenance_uses_physical_milliseconds() {
    let created_at_ms = 1_777_777_777_777;
    let updated_at_ms = created_at_ms + 1;
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .policies(
                PublicTablePolicies::new()
                    .with_insert(PublicPolicyExpr::eq_literal(
                        "$createdAt",
                        PublicValue::Timestamp(created_at_ms),
                    ))
                    .with_update(
                        Some(PublicPolicyExpr::eq_literal(
                            "$createdAt",
                            PublicValue::Timestamp(created_at_ms),
                        )),
                        PublicPolicyExpr::eq_literal(
                            "$updatedAt",
                            PublicValue::Timestamp(updated_at_ms),
                        ),
                    ),
            ),
    ));
    let (_core_dir, mut core) = open_node_with_schema(node(0x9a), schema);
    let author = user(0xa1);
    let row_uuid = row(0x9a);

    let insert = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, created_at_ms)
                .made_by(author)
                .cells(title_cells("created")),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(insert).unwrap();
    assert!(matches!(
        core.transaction_state_settled(insert),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));

    let update = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row_uuid, updated_at_ms)
                .made_by(author)
                .parents(vec![insert])
                .cells(title_cells("updated")),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(update).unwrap();
    assert!(matches!(
        core.transaction_state_settled(update),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));
}

#[test]
fn local_insert_policy_classification_survives_finalization_retry() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .policies(
                PublicTablePolicies::new()
                    .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True),
            ),
    ));
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailTransactionReadMemoryStorage::new(&column_family_refs);
    let mut core = NodeState::new(node(0x95), schema, storage.clone()).unwrap();
    let author = user(0xa1);
    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x95), 10)
                .made_by(author)
                .cells(owner_cells(author, "retrying insert")),
        )
        .unwrap();

    storage.fail_after_transaction_reads(2);
    assert!(core
        .finalize_local_mergeable_commit_settled(tx_id)
        .expect_err("injected finalization failure")
        .to_string()
        .contains("injected transaction read failure"));
    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((Fate::Pending, None, DurabilityTier::Local))
    ));

    core.finalize_local_mergeable_commit_settled(tx_id).unwrap();
    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local
        ))
    ), "retry must reapply INSERT policy, not reinterpret the candidate as an update");
}

#[test]
fn attributed_write_retry_preserves_permission_subject_after_rejection_error() {
    let schema = owner_policy_schema();
    let backend = user(0xb0);
    let attributed_user = user(0xa1);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailTransactionReadMemoryStorage::new(&column_family_refs);
    let mut core = NodeState::new(node(0x90), schema, storage.clone()).unwrap();
    install_test_uuid_sub_claim(&mut core, backend);

    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x90), 10)
                .made_by(attributed_user)
                .permission_subject(backend)
                .cells(owner_cells(attributed_user, "attributed retry")),
        )
        .unwrap();

    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((Fate::Pending, None, DurabilityTier::Local))
    ));
    // The finalizer reads its pending transaction and the policy evaluator reads
    // its transaction provenance before `ingest_rejected_transaction` retries
    // that lookup to persist the rejection.
    storage.fail_after_transaction_reads(2);
    let error = core.finalize_local_mergeable_commit_settled(tx_id).unwrap_err();
    assert!(error.to_string().contains("injected transaction read failure"));
    let pending_state = core.transaction_state_settled(tx_id);
    assert!(matches!(
        pending_state,
        Some((Fate::Pending, None, DurabilityTier::Local))
    ), "failed finalization must leave the transaction pending, got {pending_state:?}");

    core.finalize_local_mergeable_commit_settled(tx_id).unwrap();

    // The retry must still use the trusted backend as its authenticated subject.
    // `made_by` owns the row and would incorrectly accept this transaction.
    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local
        ))
    ));
    assert!(core
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn attributed_write_checkpoint_error_cleans_up_terminal_permission_subject() {
    let schema = owner_policy_schema();
    let backend = user(0xb0);
    let attributed_user = user(0xa1);
    let column_families = schema.column_families();
    let column_family_refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    let storage = FailTransactionReadMemoryStorage::new(&column_family_refs);
    let mut core = NodeState::new(node(0x90), schema, storage.clone()).unwrap();
    install_test_uuid_sub_claim(&mut core, backend);

    let tx_id = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x90), 10)
                .made_by(attributed_user)
                .permission_subject(backend)
                .cells(owner_cells(backend, "checkpoint cleanup")),
        )
        .unwrap();

    // The first six transaction reads are part of validation and acceptance;
    // The seventh after Accepted persists.
    storage.fail_after_transaction_reads(6);
    let error = core.finalize_local_mergeable_commit_settled(tx_id).unwrap_err();
    assert!(error.to_string().contains("injected transaction read failure"));
    let terminal_state = core.transaction_state_settled(tx_id);
    assert!(matches!(
        terminal_state,
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ), "checkpoint failure must follow persisted acceptance, got {terminal_state:?}");

    // This internal assertion is necessary because local_permission_subjects is
    // deliberately local-only and has no user-visible API. A terminal transaction
    // cannot retry, so its entry otherwise has no public lifecycle event.
    assert!(!core.open_tx.local_permission_subjects.contains_key(&tx_id));
}

#[test]
fn write_policy_rejection_cleans_up_client() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let author = user(0xa1);
    let other = user(0xb2);
    let row_uuid = row(1);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(author)
                .cells(owner_cells(other, "wrong owner")),
        )
        .unwrap();

    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        }
    );
    writer.apply_sync_message_settled(fate).unwrap();
    assert!(
        writer
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn session_owner_string_uuid_write_policy_accepts_matching_author() {
    let schema = build_public_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner_id", PublicColumnType::Text)
            .policies(
                public_write_policies(public_claim_eq("owner_id", "user_id"))
                    .with_select(PublicPolicyExpr::True),
            ),
    ));
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let author = user(0xa1);
    let claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String(author.test_uuid().to_string()),
    )]);
    writer.set_test_provider_claims(author, claims.clone());
    core.set_test_provider_claims(author, claims);
    let row_uuid = row(0x51);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("owned".to_owned())),
                    ("owner_id".to_owned(), Value::String(author.test_uuid().to_string())),
                ])),
        )
        .unwrap();

    let [fate] = core.apply_sync_message_settled(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime::new(10, 0).unwrap()),
            durability: Some(DurabilityTier::Global),
        }
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<Vec<_>>(),
        vec![(
            row_uuid,
            BTreeMap::from([
                ("title".to_owned(), Value::String("owned".to_owned())),
                ("owner_id".to_owned(), Value::String(author.test_uuid().to_string())),
            ]),
        )]
    );
}

#[test]
fn owner_only_delete_requires_current_owner() {
    let schema = owner_policy_schema();
    let (_owner_dir, mut owner_writer) = open_node_with_schema(node(1), schema.clone());
    let (_other_dir, mut other_writer) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let owner = user(0xa1);
    let other = user(0xb2);
    let row_uuid = row(1);
    let create = commit_owner_policy_global(
        &mut owner_writer,
        &mut core,
        row_uuid,
        owner,
        owner,
        "owned",
        10,
    );

    let (bad_delete, bad_unit) = other_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 11)
                .made_by(other)
                .parents(vec![create])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [bad_fate] = core
        .apply_sync_message_settled(bad_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        bad_fate,
        SyncMessage::FateUpdate {
            tx_id: bad_delete,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        }
    );

    let (good_delete, good_unit) = owner_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 12)
                .made_by(owner)
                .parents(vec![create])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [good_fate] = core
        .apply_sync_message_settled(good_unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        good_fate,
        SyncMessage::FateUpdate {
            tx_id: good_delete,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime::new(12, 0).unwrap()),
            durability: Some(DurabilityTier::Global),
        }
    );
    assert!(
        core.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}
#[test]
fn owner_only_read_narrows_view_updates_per_peer_identity() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_a_dir, mut reader_a) = open_node_with_schema(node(3), schema.clone());
    let (_reader_b_dir, mut reader_b) = open_node_with_schema(node(4), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let tx_a = commit_core_owner_fixture(&mut core, row(1), author_a, "a row", 10);
    let tx_b = commit_core_owner_fixture(&mut core, row(2), author_b, "b row", 11);
    let mut link_a = PeerState::client_link(author_a);
    let mut link_b = PeerState::client_link(author_b);

    let update_a = link_a.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update_a, BTreeSet::from([row(1)]));
    reader_a.apply_sync_message_settled(update_a).unwrap();
    let update_b = link_b.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update_b, BTreeSet::from([row(2)]));
    reader_b.apply_sync_message_settled(update_b).unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();

    assert_eq!(
        link_a.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_a]))
    );
    assert_eq!(
        link_b.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_b]))
    );
    assert_policy_subscription_rows(&mut reader_a, 42, author_a);
    assert_policy_subscription_rows(&mut reader_b, 43, author_b);
}

#[test]
fn maintained_public_query_bundle_filters_private_rows_from_same_tx() {
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("announcements")
                    .column("title", PublicColumnType::Text)
                    .policies(public_all_policies()),
            )
            .table(
                PublicTableSchemaBuilder::new("messages")
                    .column("body", PublicColumnType::Text)
                    .column("owner_id", PublicColumnType::Text)
                    .policies(
                        public_all_policies()
                            .with_select(public_claim_eq("owner_id", "user_id")),
                    ),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_bob_dir, mut bob_node) = open_node_with_schema(node(4), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    let announcement_row = row(0x11);
    let private_message_row = row(0x12);
    let tx_id = core
        .commit_mergeable_many_settled(vec![
            MergeableCommit::new("announcements", announcement_row, 10)
                .made_by(alice)
                .cells(BTreeMap::from([("title".to_owned(), v("public"))])),
            MergeableCommit::new("messages", private_message_row, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("body".to_owned(), v("alice private")),
                    ("owner_id".to_owned(), Value::String(alice.test_uuid().to_string())),
                ])),
        ])
        .unwrap();
    core.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let shape = Query::from("announcements").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let mut bob_peer = PeerState::client_link(bob);

    let update = bob_peer
        .rehydrate_query(&mut core, &shape, &binding)
        .unwrap();
    let version_bundles = version_bundles_for_update(&update);
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads, ..
            },
        result_member_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(result_member_adds, &vec![(
        groove::Intern::new("announcements".to_owned()),
        announcement_row,
        tx_id
    )]);
    assert!(complete_tx_payloads.is_empty());
    assert!(!bob_peer.shipped_complete_tx_payloads().contains(&tx_id));
    let shipped_rows = version_bundles
        .iter()
        .flat_map(|bundle| bundle.versions.iter().map(|version| version.row_uuid()))
        .collect::<BTreeSet<_>>();
    assert_eq!(shipped_rows, BTreeSet::from([announcement_row]));

    bob_node.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        bob_node
            .current_rows("announcements", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<Vec<_>>(),
        vec![(
            announcement_row,
            BTreeMap::from([("title".to_owned(), v("public"))])
        )]
    );
    assert!(
        bob_node
            .current_rows("messages", DurabilityTier::Local)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn owner_transfer_removes_settled_result_set_without_redacting_local_copy() {
    let schema = owner_policy_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_a_dir, mut reader_a) = open_node_with_schema(node(3), schema.clone());
    let (_reader_b_dir, mut reader_b) = open_node_with_schema(node(4), schema);
    let author_a = user(0xa1);
    let author_b = user(0xb2);
    let row_uuid = row(7);
    let tx_a = commit_core_owner_fixture(&mut core, row_uuid, author_a, "owned by A", 10);
    let mut link_a = PeerState::client_link(author_a);

    let update = link_a.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([row_uuid]));
    reader_a.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_a, "owned by A"))]
    );

    let tx_b = commit_core_owner_fixture(&mut core, row_uuid, author_b, "owned by B", 11);
    let update = link_a.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs, ..
            },
        result_member_adds,
        result_member_removes,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert!(version_bundles.is_empty());
    assert!(complete_tx_payload_refs.is_empty());
    assert!(result_member_adds.is_empty());
    assert_eq!(
        result_member_removes,
        &vec![("todos".to_owned().into(), row_uuid, tx_a)]
    );
    reader_a.apply_sync_message_settled(update).unwrap();
    assert!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_a, "owned by A"))]
    );

    let mut link_b = PeerState::client_link(author_b);
    let update = link_b.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update, BTreeSet::from([row_uuid]));
    reader_b.apply_sync_message_settled(update).unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    assert_eq!(
        link_b.subscription_result_sets(subscription),
        Some(BTreeSet::from([tx_b]))
    );
    assert_eq!(
        reader_b
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_b, "owned by B"))]
    );
}
#[test]
fn join_policy_authorizes_writes_reads_and_next_emission_revocation() {
    let invited = user(0xa1);
    let uninvited = user(0xb2);
    let canvas_row = row(8);
    let invite_row = row(9);
    let canvas_policy = public_outer_exists(
        "canvasInvites",
        "canvas",
        "id",
        [public_claim_eq("userID", "sub")],
    );
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("canvases")
                    .column("title", PublicColumnType::Text)
                    .policies(
                        public_write_policies(canvas_policy.clone()).with_select(canvas_policy),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("canvasInvites")
                    .fk_column("canvas", "canvases")
                    .column("userID", PublicColumnType::Uuid),
            ),
    );
    let (_uninvited_writer_dir, mut uninvited_writer) =
        open_node_with_schema(node(1), schema.clone());
    let (_invited_writer_dir, mut invited_writer) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_invited_dir, mut invited_reader) = open_node_with_schema(node(3), schema.clone());
    let (_uninvited_dir, mut uninvited_reader) = open_node_with_schema(node(4), schema);
    install_test_uuid_sub_claim(&mut core, invited);
    install_test_uuid_sub_claim(&mut core, uninvited);

    let denied_tx = uninvited_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("canvases", canvas_row, 10)
                .made_by(uninvited)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("blocked".to_owned()),
                )])),
        )
        .unwrap();
    let [denied] = core
        .apply_sync_message_settled(denied_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        denied,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));

    let invite_tx = core
        .commit_mergeable_settled(MergeableCommit::new("canvasInvites", invite_row, 11).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("userID".to_owned(), Value::Uuid(invited.test_uuid())),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        invite_tx,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let accepted_tx = invited_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("canvases", canvas_row, 12)
                .made_by(invited)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("allowed".to_owned()),
                )])),
        )
        .unwrap();
    let accepted_id = accepted_tx.0;
    let [accepted] = core
        .apply_sync_message_settled(accepted_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        accepted,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));
    assert!(matches!(
        core.transaction_state_settled(accepted_id),
        Some((Fate::Accepted, _, DurabilityTier::Global))
    ));

    let mut invited_link = PeerState::client_link(invited);
    let invited_update = invited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    invited_reader.apply_sync_message_settled(invited_update).unwrap();
    assert_eq!(
        invited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(|row| {
                let table = invited_reader
                    .catalogue
                    .schema
                    .tables
                    .iter()
                    .find(|table| table.name == "canvases")
                    .expect("canvases table");
                (
                    row.row_uuid(),
                    BTreeMap::from([(
                        "title".to_owned(),
                        row.cell(table, "title").expect("title cell"),
                    )]),
                )
            })
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            canvas_row,
            BTreeMap::from([("title".to_owned(), Value::String("allowed".to_owned()))])
        )])
    );

    let mut uninvited_link = PeerState::client_link(uninvited);
    let uninvited_update = uninvited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    uninvited_reader
        .apply_sync_message_settled(uninvited_update)
        .unwrap();
    assert!(
        uninvited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let revoke_tx = core
        .commit_mergeable_settled(
            MergeableCommit::new("canvasInvites", invite_row, 13).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.apply_fate_update(
        revoke_tx,
        Fate::Accepted,
        Some(GlobalTime(3)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    let revoked_update = invited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_removes, ..
    }) = &revoked_update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_removes,
        &vec![("canvases".to_owned().into(), canvas_row, accepted_id)]
    );
    invited_reader.apply_sync_message_settled(revoked_update).unwrap();
    assert!(
        invited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    // Closure-row policy revocation is still checked at emission; C2 composes
    // output-row policies into the subscription graph.
}

/// The authority accepts Alice's editor insert only when the candidate's
/// canvas agrees with the referenced layer; changing only the candidate canvas
/// must produce the ordinary authorization-denied fate.
///
/// This stays at the node boundary because only the authority's settled fate
/// proves that the compiled policy joins constrain an incoming candidate.
///
/// ```text
/// alice ──shape(layer A, canvas A)──► authority ──► Accepted
/// alice ──shape(layer A, canvas B)──► authority ──► AuthorizationDenied
/// ```
#[test]
fn nested_correlated_exists_insert_policy_rejects_cross_canvas_candidates() {
    let alice = user(0xa3);
    let canvas_a = row(0xa4);
    let canvas_b = row(0xa5);
    let layer_a = row(0xa6);
    let editor_membership = row(0xa7);
    let accepted_shape = row(0xa8);
    let rejected_shape = row(0xa9);

    let insert_policy = PublicPolicyExpr::Exists {
        table: "layers".to_owned(),
        condition: Box::new(PublicPolicyExpr::And(vec![
            PublicPolicyExpr::Cmp {
                column: "id".to_owned(),
                op: PublicCmpOp::Eq,
                value: PublicPolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "layer_id".to_owned(),
                ]),
            },
            PublicPolicyExpr::Cmp {
                column: "canvas_id".to_owned(),
                op: PublicCmpOp::Eq,
                value: PublicPolicyValue::SessionRef(vec![
                    "__jazz_outer_row".to_owned(),
                    "canvas_id".to_owned(),
                ]),
            },
            PublicPolicyExpr::Exists {
                table: "canvas_members".to_owned(),
                condition: Box::new(PublicPolicyExpr::And(vec![
                    PublicPolicyExpr::Cmp {
                        column: "canvas_id".to_owned(),
                        op: PublicCmpOp::Eq,
                        value: PublicPolicyValue::SessionRef(vec![
                            "__jazz_outer_row".to_owned(),
                            "canvas_id".to_owned(),
                        ]),
                    },
                    public_claim_eq("user_id", "sub"),
                    public_literal_eq("role", PublicValue::Text("editor".to_owned())),
                ])),
            },
        ])),
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("canvases")
                    .column("title", PublicColumnType::Text),
            )
            .table(PublicTableSchemaBuilder::new("layers").fk_column("canvas_id", "canvases"))
            .table(
                PublicTableSchemaBuilder::new("canvas_members")
                    .fk_column("canvas_id", "canvases")
                    .column("user_id", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("shapes")
                    .fk_column("layer_id", "layers")
                    .fk_column("canvas_id", "canvases")
                    .policies(PublicTablePolicies::new().with_insert(insert_policy)),
            ),
    );
    let (_alice_dir, mut alice_node) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);

    accept_global(
        &mut core,
        MergeableCommit::new("canvases", canvas_a, 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("canvas A".to_owned()),
        )])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("canvases", canvas_b, 11).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("canvas B".to_owned()),
        )])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("layers", layer_a, 12).cells(BTreeMap::from([(
            "canvas_id".to_owned(),
            Value::Uuid(canvas_a.0),
        )])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("canvas_members", editor_membership, 13).cells(BTreeMap::from([
            ("canvas_id".to_owned(), Value::Uuid(canvas_a.0)),
            ("user_id".to_owned(), Value::Uuid(alice.test_uuid())),
            ("role".to_owned(), Value::String("editor".to_owned())),
        ])),
    );

    let accepted = alice_node
        .commit_mergeable_unit_settled(
            MergeableCommit::new("shapes", accepted_shape, 14)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("layer_id".to_owned(), Value::Uuid(layer_a.0)),
                    ("canvas_id".to_owned(), Value::Uuid(canvas_a.0)),
                ])),
        )
        .unwrap();
    let [accepted_fate] = core
        .apply_sync_message_settled(accepted.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        accepted_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let rejected = alice_node
        .commit_mergeable_unit_settled(
            MergeableCommit::new("shapes", rejected_shape, 15)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("layer_id".to_owned(), Value::Uuid(layer_a.0)),
                    ("canvas_id".to_owned(), Value::Uuid(canvas_b.0)),
                ])),
        )
        .unwrap();
    let [rejected_fate] = core
        .apply_sync_message_settled(rejected.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        rejected_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));
}

/// `allowedTo.insert(release)` composes the release INSERT policy with the
/// assignment's tenant-correlated existence checks: an eligible owner is
/// accepted while that same owner cannot attach a foreign membership. The
/// compiler-plan regression separately asserts occurrence-carrier suppression.
/// A relation-backed `exists` must prove both the denormalized workspace and
/// the referenced block together.  The same owner is deliberately a member of
/// both workspaces, so authorizing the block reference without its workspace
/// correlation would accept the cross-workspace write.
#[test]
fn correlated_exists_rel_keeps_workspace_and_referenced_row_together_for_insert_and_update() {
    let owner = user(0xa1);
    let owner_claim_subject = owner.test_uuid();
    let workspace_a = row(0xb1);
    let workspace_b = row(0xb2);
    let owner_membership_a = row(0xc1);
    let owner_membership_b = row(0xc2);
    let block_a = row(0xd1);
    let block_b = row(0xd2);
    let accepted_task = row(0xe1);
    let rejected_task = row(0xe2);

    let column = |scope: &str, name: &str| PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: name.to_owned(),
    };
    let outer = |scope: &str, name: &str, outer_name: &str| PublicRelPredicateExpr::Cmp {
        left: column(scope, name),
        op: PublicRelPredicateCmpOp::Eq,
        right: PublicRelValueRef::OuterColumn(PublicRelColumnRef::unscoped(outer_name)),
    };
    let task_policy = PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::TableScan {
                    table: "blocks".into(),
                    alias: Some("blocks".to_owned()),
                }),
                right: Box::new(PublicRelExpr::Filter {
                    input: Box::new(PublicRelExpr::TableScan {
                        table: "members".into(),
                        alias: Some("members".to_owned()),
                    }),
                    predicate: PublicRelPredicateExpr::And(vec![
                        outer("members", "workspace", "workspace"),
                        PublicRelPredicateExpr::Cmp {
                            left: column("members", "subject"),
                            op: PublicRelPredicateCmpOp::Eq,
                            right: PublicRelValueRef::SessionRef(vec![
                                "claims".to_owned(),
                                "sub".to_owned(),
                            ]),
                        },
                    ]),
                }),
                on: vec![PublicRelJoinCondition {
                    left: column("blocks", "workspace"),
                    right: column("members", "workspace"),
                }],
                join_kind: PublicRelJoinKind::Inner,
            }),
            // The FK correlation is separate from the nested membership
            // conjunction, mirroring `allOf([exists, workspaceId + FK])`.
            predicate: PublicRelPredicateExpr::And(vec![outer("blocks", "id", "block")]),
        },
    };
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("workspaces")
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("members")
                    .fk_column("workspace", "workspaces")
                    .column("subject", PublicColumnType::Uuid),
            )
            .table(
                PublicTableSchemaBuilder::new("blocks")
                    .fk_column("workspace", "workspaces"),
            )
            .table(
                PublicTableSchemaBuilder::new("tasks")
                    .fk_column("workspace", "workspaces")
                    .fk_column("block", "blocks")
                    .column("title", PublicColumnType::Text)
                    .policies(
                        PublicTablePolicies::new()
                            .with_insert(task_policy.clone())
                            .with_update(Some(task_policy.clone()), task_policy.clone())
                            .with_delete(task_policy),
                    ),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(0x9a), schema);
    for (table, row_uuid, time, cells) in [
        (
            "workspaces",
            workspace_a,
            1,
            BTreeMap::from([("name".to_owned(), Value::String("A".to_owned()))]),
        ),
        (
            "workspaces",
            workspace_b,
            2,
            BTreeMap::from([("name".to_owned(), Value::String("B".to_owned()))]),
        ),
        (
            "members",
            owner_membership_a,
            3,
            BTreeMap::from([
                ("workspace".to_owned(), Value::Uuid(workspace_a.0)),
                ("subject".to_owned(), Value::Uuid(owner_claim_subject)),
            ]),
        ),
        (
            "members",
            owner_membership_b,
            4,
            BTreeMap::from([
                ("workspace".to_owned(), Value::Uuid(workspace_b.0)),
                ("subject".to_owned(), Value::Uuid(owner_claim_subject)),
            ]),
        ),
        (
            "blocks",
            block_a,
            5,
            BTreeMap::from([("workspace".to_owned(), Value::Uuid(workspace_a.0))]),
        ),
        (
            "blocks",
            block_b,
            6,
            BTreeMap::from([("workspace".to_owned(), Value::Uuid(workspace_b.0))]),
        ),
    ] {
        accept_global(&mut core, MergeableCommit::new(table, row_uuid, time).cells(cells));
    }
    core.set_test_provider_claims(
        owner,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(owner_claim_subject))]),
    );

    let accepted = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", accepted_task, 7)
                .made_by(owner)
                .cells(BTreeMap::from([
                    ("workspace".to_owned(), Value::Uuid(workspace_a.0)),
                    ("block".to_owned(), Value::Uuid(block_a.0)),
                    ("title".to_owned(), Value::String("same workspace".to_owned())),
                ])),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(accepted).unwrap();
    assert!(matches!(
        core.transaction_state_settled(accepted),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));

    let denied_insert = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", rejected_task, 8)
                .made_by(owner)
                .cells(BTreeMap::from([
                    ("workspace".to_owned(), Value::Uuid(workspace_a.0)),
                    ("block".to_owned(), Value::Uuid(block_b.0)),
                    ("title".to_owned(), Value::String("cross workspace".to_owned())),
                ])),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(denied_insert).unwrap();
    assert!(matches!(
        core.transaction_state_settled(denied_insert),
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), None, DurabilityTier::Local))
    ));

    let denied_update = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", accepted_task, 9)
                .made_by(owner)
                .cells(BTreeMap::from([
                    ("workspace".to_owned(), Value::Uuid(workspace_a.0)),
                    ("block".to_owned(), Value::Uuid(block_b.0)),
                    ("title".to_owned(), Value::String("foreign replacement".to_owned())),
                ])),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(denied_update).unwrap();
    assert!(matches!(
        core.transaction_state_settled(denied_update),
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), None, DurabilityTier::Local))
    ));

    // DELETE evaluates the persisted old row through USING, rather than a
    // candidate payload. The rejected UPDATE above must not replace that old
    // same-workspace authority.
    let accepted_delete = core
        .commit_mergeable_settled(
            MergeableCommit::new("tasks", accepted_task, 10)
                .made_by(owner)
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(accepted_delete)
        .unwrap();
    assert!(matches!(
        core.transaction_state_settled(accepted_delete),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));
}

#[test]
fn exists_rel_rejects_nested_outer_correlation_off_the_join_key() {
    let column = |scope: &str, name: &str| PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: name.to_owned(),
    };
    let policy = PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::TableScan {
                    table: "blocks".into(),
                    alias: Some("blocks".to_owned()),
                }),
                right: Box::new(PublicRelExpr::Filter {
                    input: Box::new(PublicRelExpr::TableScan {
                        table: "members".into(),
                        alias: Some("members".to_owned()),
                    }),
                    predicate: PublicRelPredicateExpr::Cmp {
                        // `subject` is not the blocks-members equality key;
                        // accepting this would silently retarget the proof.
                        left: column("members", "subject"),
                        op: PublicRelPredicateCmpOp::Eq,
                        right: PublicRelValueRef::OuterColumn(PublicRelColumnRef::unscoped(
                            "workspace",
                        )),
                    },
                }),
                on: vec![PublicRelJoinCondition {
                    left: column("blocks", "workspace"),
                    right: column("members", "workspace"),
                }],
                join_kind: PublicRelJoinKind::Inner,
            }),
            predicate: PublicRelPredicateExpr::Cmp {
                left: column("blocks", "id"),
                op: PublicRelPredicateCmpOp::Eq,
                right: PublicRelValueRef::OuterColumn(PublicRelColumnRef::unscoped("block")),
            },
        },
    };
    let public = PublicSchemaBuilder::new()
        .table(PublicTableSchemaBuilder::new("workspaces"))
        .table(
            PublicTableSchemaBuilder::new("members")
                .fk_column("workspace", "workspaces")
                .column("subject", PublicColumnType::Uuid),
        )
        .table(
            PublicTableSchemaBuilder::new("blocks").fk_column("workspace", "workspaces"),
        )
        .table(
            PublicTableSchemaBuilder::new("tasks")
                .fk_column("workspace", "workspaces")
                .fk_column("block", "blocks")
                .policies(PublicTablePolicies::new().with_insert(policy)),
        )
        .build();
    let error = crate::schema::JazzSchema::new(&public)
        .expect_err("mismatched nested correlation must fail closed");
    assert!(error
        .to_string()
        .contains("nested outer correlation must use its join key"));
}

#[test]
fn exists_rel_fails_closed_for_outer_correlation_beyond_one_nested_join() {
    let column = |scope: &str, name: &str| PublicRelColumnRef {
        scope: Some(scope.to_owned()),
        column: name.to_owned(),
    };
    let policy = PublicPolicyExpr::ExistsRel {
        rel: PublicRelExpr::Filter {
            input: Box::new(PublicRelExpr::Join {
                left: Box::new(PublicRelExpr::TableScan {
                    table: "blocks".into(),
                    alias: Some("blocks".to_owned()),
                }),
                right: Box::new(PublicRelExpr::Join {
                    left: Box::new(PublicRelExpr::TableScan {
                        table: "members".into(),
                        alias: Some("members".to_owned()),
                    }),
                    right: Box::new(PublicRelExpr::Filter {
                        input: Box::new(PublicRelExpr::TableScan {
                            table: "grants".into(),
                            alias: Some("grants".to_owned()),
                        }),
                        predicate: PublicRelPredicateExpr::Cmp {
                            left: column("grants", "workspace"),
                            op: PublicRelPredicateCmpOp::Eq,
                            right: PublicRelValueRef::OuterColumn(PublicRelColumnRef::unscoped(
                                "workspace",
                            )),
                        },
                    }),
                    on: vec![PublicRelJoinCondition {
                        left: column("members", "workspace"),
                        right: column("grants", "workspace"),
                    }],
                    join_kind: PublicRelJoinKind::Inner,
                }),
                on: vec![PublicRelJoinCondition {
                    left: column("blocks", "workspace"),
                    right: column("members", "workspace"),
                }],
                join_kind: PublicRelJoinKind::Inner,
            }),
            predicate: PublicRelPredicateExpr::Cmp {
                left: column("blocks", "id"),
                op: PublicRelPredicateCmpOp::Eq,
                right: PublicRelValueRef::OuterColumn(PublicRelColumnRef::unscoped("block")),
            },
        },
    };
    let public = PublicSchemaBuilder::new()
        .table(PublicTableSchemaBuilder::new("workspaces"))
        .table(
            PublicTableSchemaBuilder::new("blocks").fk_column("workspace", "workspaces"),
        )
        .table(
            PublicTableSchemaBuilder::new("members").fk_column("workspace", "workspaces"),
        )
        .table(
            PublicTableSchemaBuilder::new("grants").fk_column("workspace", "workspaces"),
        )
        .table(
            PublicTableSchemaBuilder::new("tasks")
                .fk_column("workspace", "workspaces")
                .fk_column("block", "blocks")
                .policies(PublicTablePolicies::new().with_insert(policy)),
        )
        .build();
    let error = crate::schema::JazzSchema::new(&public)
        .expect_err("deep outer correlation must fail closed until its scope is retained");
    assert!(error
        .to_string()
        .contains("does not yet support outer correlations beyond one nested join"));
}

#[test]
fn correlated_inherited_insert_policy_accepts_owner_and_denies_cross_tenant_membership() {
    let owner = user(0xa1);
    let outsider = user(0xb2);
    let organization = row(0xc1);
    let foreign_organization = row(0xc2);
    let owner_membership = row(0xd1);
    let foreign_membership = row(0xd2);
    let artist = row(0xd3);
    let release = row(0xe1);
    let owner_assignment = row(0xf1);
    let outsider_assignment = row(0xf2);

    let same_outer_organization = || {
        PublicPolicyExpr::eq_session(
            "organization",
            vec!["__jazz_outer_row".to_owned(), "organization".to_owned()],
        )
    };
    let release_insert_policy = PublicPolicyExpr::And(vec![
        public_outer_exists(
            "memberships",
            "organization",
            "organization",
            [
                public_claim_eq("user", "sub"),
                public_literal_eq("role", PublicValue::Text("admin".to_owned())),
            ],
        ),
        public_outer_exists(
            "artists",
            "id",
            "artist",
            [same_outer_organization()],
        ),
    ]);
    let assignment_insert_policy = PublicPolicyExpr::And(vec![
        PublicPolicyExpr::Inherits {
            operation: PublicOperation::Insert,
            via_column: "release".to_owned(),
            max_depth: None,
        },
        public_outer_exists("releases", "id", "release", [same_outer_organization()]),
        public_outer_exists(
            "memberships",
            "id",
            "membership",
            [same_outer_organization()],
        ),
    ]);
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("organizations")
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("memberships")
                    .fk_column("organization", "organizations")
                    .column("user", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("releases")
                    .fk_column("organization", "organizations")
                    .fk_column("artist", "artists")
                    .column("title", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_insert(release_insert_policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("artists")
                    .fk_column("organization", "organizations")
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("release_assignments")
                    .fk_column("organization", "organizations")
                    .fk_column("release", "releases")
                    .fk_column("membership", "memberships")
                    .policies(PublicTablePolicies::new().with_insert(assignment_insert_policy)),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);

    accept_global(
        &mut core,
        MergeableCommit::new("organizations", organization, 1).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("owner organization".to_owned()),
        )])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("organizations", foreign_organization, 2).cells(BTreeMap::from([(
            "name".to_owned(),
            Value::String("foreign organization".to_owned()),
        )])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("memberships", owner_membership, 3).cells(BTreeMap::from([
            ("organization".to_owned(), Value::Uuid(organization.0)),
            ("user".to_owned(), Value::Uuid(owner.test_uuid())),
            ("role".to_owned(), Value::String("admin".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("memberships", foreign_membership, 4).cells(BTreeMap::from([
            ("organization".to_owned(), Value::Uuid(foreign_organization.0)),
            ("user".to_owned(), Value::Uuid(outsider.test_uuid())),
            ("role".to_owned(), Value::String("admin".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("artists", artist, 5).cells(BTreeMap::from([
            ("organization".to_owned(), Value::Uuid(organization.0)),
            ("name".to_owned(), Value::String("artist".to_owned())),
        ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("releases", release, 6).cells(BTreeMap::from([
            ("organization".to_owned(), Value::Uuid(organization.0)),
            ("artist".to_owned(), Value::Uuid(artist.0)),
            ("title".to_owned(), Value::String("release".to_owned())),
        ])),
    );
    core.set_test_provider_claims(
        owner,
        BTreeMap::from([("sub".to_owned(), Value::Uuid(owner.test_uuid()))]),
    );

    let accepted = core
        .commit_mergeable_settled(
            MergeableCommit::new("release_assignments", owner_assignment, 7)
                .made_by(owner)
                .cells(BTreeMap::from([
                    ("organization".to_owned(), Value::Uuid(organization.0)),
                    ("release".to_owned(), Value::Uuid(release.0)),
                    ("membership".to_owned(), Value::Uuid(owner_membership.0)),
                ])),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(accepted).unwrap();
    assert!(matches!(
        core.transaction_state_settled(accepted),
        Some((Fate::Accepted, Some(_), DurabilityTier::Global))
    ));

    let denied = core
        .commit_mergeable_settled(
            MergeableCommit::new("release_assignments", outsider_assignment, 8)
                .made_by(owner)
                .cells(BTreeMap::from([
                    ("organization".to_owned(), Value::Uuid(organization.0)),
                    ("release".to_owned(), Value::Uuid(release.0)),
                    ("membership".to_owned(), Value::Uuid(foreign_membership.0)),
                ])),
        )
        .unwrap();
    core.finalize_local_mergeable_commit_settled(denied).unwrap();
    assert!(matches!(
        core.transaction_state_settled(denied),
        Some((
            Fate::Rejected(RejectionReason::AuthorizationDenied),
            None,
            DurabilityTier::Local
        ))
    ));
}

#[test]
fn write_policy_branch_or_join_allows_either_literal_branch_or_membership_join() {
    let invited = user(0xa1);
    let uninvited = user(0xb2);
    let public_canvas = row(8);
    let private_canvas = row(9);
    let blocked_canvas = row(11);
    let invite_row = row(10);
    let policy = PublicPolicyExpr::Or(vec![
        public_literal_eq("isPublic", PublicValue::Boolean(true)),
        public_outer_exists(
            "canvasInvites",
            "canvas",
            "id",
            [public_claim_eq("userID", "sub")],
        ),
    ]);
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("canvases")
                    .column("title", PublicColumnType::Text)
                    .column("isPublic", PublicColumnType::Boolean)
                    .policies(public_write_policies(policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("canvasInvites")
                    .fk_column("canvas", "canvases")
                    .column("userID", PublicColumnType::Uuid),
            ),
    );
    let (_invited_dir, mut invited_writer) = open_node_with_schema(node(1), schema.clone());
    let (_uninvited_dir, mut uninvited_writer) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, invited);
    install_test_uuid_sub_claim(&mut core, uninvited);

    let invite_tx = core
        .commit_mergeable_settled(MergeableCommit::new("canvasInvites", invite_row, 3).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(private_canvas.0)),
                ("userID".to_owned(), Value::Uuid(invited.test_uuid())),
            ]),
        ))
        .unwrap();
    core.apply_fate_update(
        invite_tx,
        Fate::Accepted,
        Some(GlobalTime(0)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    let public_tx = uninvited_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("canvases", public_canvas, 14)
                .made_by(uninvited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("public".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(true)),
                ])),
        )
        .unwrap();
    let [public_fate] = core.apply_sync_message_settled(public_tx.1).unwrap().try_into().unwrap();
    assert!(matches!(
        public_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let private_tx = invited_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("canvases", private_canvas, 15)
                .made_by(invited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("private".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(false)),
                ])),
        )
        .unwrap();
    let [private_fate] = core
        .apply_sync_message_settled(private_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        private_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let blocked_tx = uninvited_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("canvases", blocked_canvas, 16)
                .made_by(uninvited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("blocked".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(false)),
                ])),
        )
        .unwrap();
    let [blocked_fate] = core
        .apply_sync_message_settled(blocked_tx.1)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        blocked_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));
}

#[test]
fn read_policy_branch_or_join_allows_public_or_membership_reads() {
    let member = user(0xa1);
    let other = user(0xb2);
    let public_chat = row(0x18);
    let private_chat = row(0x19);
    let membership = row(0x1a);
    let policy = PublicPolicyExpr::Or(vec![
        public_literal_eq("isPublic", PublicValue::Boolean(true)),
        public_outer_exists(
            "chatMembers",
            "chatId",
            "id",
            [public_claim_eq("userId", "user_id")],
        ),
    ]);
    let schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("chats")
                    .column("title", PublicColumnType::Text)
                    .column("isPublic", PublicColumnType::Boolean)
                    .column("createdBy", PublicColumnType::Uuid)
                    .policies(public_all_policies().with_select(policy)),
            )
            .table(
                PublicTableSchemaBuilder::new("chatMembers")
                    .fk_column("chatId", "chats")
                    .column("userId", PublicColumnType::Text)
                    .policies(public_write_policies(PublicPolicyExpr::True)),
            ),
    );
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_member_dir, _member_reader) = open_node_with_schema(node(3), schema.clone());
    let (_other_dir, _other_reader) = open_node_with_schema(node(4), schema);
    core.set_test_provider_claims(
        member,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(member.test_uuid().to_string()),
        )]),
    );
    core.set_test_provider_claims(
        other,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(other.test_uuid().to_string()),
        )]),
    );

    accept_global(
        &mut core,
        MergeableCommit::new("chats", public_chat, 10)
            .made_by(member)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("public".to_owned())),
                ("isPublic".to_owned(), Value::Bool(true)),
                ("createdBy".to_owned(), Value::Uuid(member.test_uuid())),
            ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", private_chat, 11)
            .made_by(member)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("private".to_owned())),
                ("isPublic".to_owned(), Value::Bool(false)),
                ("createdBy".to_owned(), Value::Uuid(member.test_uuid())),
            ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", membership, 12).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(private_chat.0)),
            ("userId".to_owned(), Value::String(member.test_uuid().to_string())),
        ])),
    );
    let shape = Query::from("chats").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, member)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_chat, private_chat])
    );
    assert_eq!(
        core.query_rows_for_link(&shape, &binding, DurabilityTier::Global, other)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([public_chat])
    );
}
