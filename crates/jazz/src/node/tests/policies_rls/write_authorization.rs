// Write attribution, ownership, joins, rejection, and cleanup.

use crate::query::{Include, JoinMode, OrderDirection};
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
    fn get(
        &self,
        cf: &ColumnFamilyName,
        key: &Key,
    ) -> Result<Option<StorageValue>, groove::storage::Error> {
        if key
            .windows("jazz_transactions".len())
            .any(|window| window == b"jazz_transactions")
            && let Some(remaining) = self.fail_after_transaction_reads.get()
        {
            if remaining == 0 {
                self.fail_after_transaction_reads.set(None);
                return Err(groove::storage::Error::InvalidStorageLayout(
                    "injected transaction read failure".to_owned(),
                ));
            }
            self.fail_after_transaction_reads.set(Some(remaining - 1));
        }
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

    fn delete(
        &self,
        cf: &ColumnFamilyName,
        key: &Key,
    ) -> Result<(), groove::storage::Error> {
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
        self.inner.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        self.inner.column_family_names()
    }
}

impl ReopenableStorage for FailTransactionReadMemoryStorage {
    fn reopen(
        mut self,
        column_families: &[&str],
    ) -> Result<Self, groove::storage::Error> {
        self.inner = self.inner.reopen(column_families)?;
        Ok(self)
    }
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

    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x90), 10)
                .made_by(attributed_user)
                .permission_subject(backend)
                .cells(owner_cells(attributed_user, "attributed retry")),
        )
        .unwrap();

    assert!(matches!(
        core.transaction_state(tx_id),
        Some((Fate::Pending, None, DurabilityTier::Local))
    ));
    // The finalizer reads its pending transaction and the policy evaluator reads
    // its transaction provenance before `ingest_rejected_transaction` retries
    // that lookup to persist the rejection.
    storage.fail_after_transaction_reads(2);
    let error = core.finalize_local_mergeable_commit(tx_id).unwrap_err();
    assert!(error.to_string().contains("injected transaction read failure"));
    let pending_state = core.transaction_state(tx_id);
    assert!(matches!(
        pending_state,
        Some((Fate::Pending, None, DurabilityTier::Local))
    ), "failed finalization must leave the transaction pending, got {pending_state:?}");

    core.finalize_local_mergeable_commit(tx_id).unwrap();

    // The retry must still use the trusted backend as its authenticated subject.
    // `made_by` owns the row and would incorrectly accept this transaction.
    assert!(matches!(
        core.transaction_state(tx_id),
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

    let tx_id = core
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x90), 10)
                .made_by(attributed_user)
                .permission_subject(backend)
                .cells(owner_cells(backend, "checkpoint cleanup")),
        )
        .unwrap();

    // The first six transaction reads are part of validation and acceptance;
    // The seventh after Accepted persists.
    storage.fail_after_transaction_reads(6);
    let error = core.finalize_local_mergeable_commit(tx_id).unwrap_err();
    assert!(error.to_string().contains("injected transaction read failure"));
    let terminal_state = core.transaction_state(tx_id);
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(author)
                .cells(owner_cells(other, "wrong owner")),
        )
        .unwrap();

    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        }
    );
    writer.apply_sync_message(fate).unwrap();
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
    let row_uuid = row(0x51);
    let (tx_id, unit) = writer
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(author)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("owned".to_owned())),
                    ("owner_id".to_owned(), Value::String(author.0.to_string())),
                ])),
        )
        .unwrap();

    let [fate] = core.apply_sync_message(unit).unwrap().try_into().unwrap();
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
                ("owner_id".to_owned(), Value::String(author.0.to_string())),
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 11)
                .made_by(other)
                .parents(vec![create])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [bad_fate] = core
        .apply_sync_message(bad_unit)
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
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 12)
                .made_by(owner)
                .parents(vec![create])
                .deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let [good_fate] = core
        .apply_sync_message(good_unit)
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
    reader_a.apply_sync_message(update_a).unwrap();
    let update_b = link_b.current_rows_update(&mut core, "todos").unwrap();
    assert_view_update_only_references_rows(&update_b, BTreeSet::from([row(2)]));
    reader_b.apply_sync_message(update_b).unwrap();
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
        .commit_mergeable_many(vec![
            MergeableCommit::new("announcements", announcement_row, 10)
                .made_by(alice)
                .cells(BTreeMap::from([("title".to_owned(), v("public"))])),
            MergeableCommit::new("messages", private_message_row, 10)
                .made_by(alice)
                .cells(BTreeMap::from([
                    ("body".to_owned(), v("alice private")),
                    ("owner_id".to_owned(), Value::String(alice.0.to_string())),
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
    let SyncMessage::ViewUpdate {
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads, ..
            },
        result_member_adds,
        ..
    } = &update
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

    bob_node.apply_sync_message(update).unwrap();
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
    reader_a.apply_sync_message(update).unwrap();
    assert_eq!(
        reader_a
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row_uuid, owner_cells(author_a, "owned by A"))]
    );

    let tx_b = commit_core_owner_fixture(&mut core, row_uuid, author_b, "owned by B", 11);
    let update = link_a.current_rows_update(&mut core, "todos").unwrap();
    let SyncMessage::ViewUpdate {
        version_bundles,
        peer_payload_inventory:
            crate::protocol::PeerPayloadInventory {
                complete_tx_payloads: complete_tx_payload_refs, ..
            },
        result_member_adds,
        result_member_removes,
        ..
    } = &update
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
    reader_a.apply_sync_message(update).unwrap();
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
    reader_b.apply_sync_message(update).unwrap();
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

    let denied_tx = uninvited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", canvas_row, 10)
                .made_by(uninvited)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("blocked".to_owned()),
                )])),
        )
        .unwrap();
    let [denied] = core
        .apply_sync_message(denied_tx.1)
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
        .commit_mergeable(MergeableCommit::new("canvasInvites", invite_row, 11).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(canvas_row.0)),
                ("userID".to_owned(), Value::Uuid(invited.0)),
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
        .commit_mergeable_unit(
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
        .apply_sync_message(accepted_tx.1)
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
        core.transaction_state(accepted_id),
        Some((Fate::Accepted, _, DurabilityTier::Global))
    ));

    let mut invited_link = PeerState::client_link(invited);
    let invited_update = invited_link
        .current_rows_update(&mut core, "canvases")
        .unwrap();
    invited_reader.apply_sync_message(invited_update).unwrap();
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
        .apply_sync_message(uninvited_update)
        .unwrap();
    assert!(
        uninvited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let revoke_tx = core
        .commit_mergeable(
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
    let SyncMessage::ViewUpdate {
        result_member_removes, ..
    } = &revoked_update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_removes,
        &vec![("canvases".to_owned().into(), canvas_row, accepted_id)]
    );
    invited_reader.apply_sync_message(revoked_update).unwrap();
    assert!(
        invited_reader
            .subscription_current_rows("canvases", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
    // Closure-row policy revocation is still checked at emission; C2 composes
    // output-row policies into the subscription graph.
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

    let invite_tx = core
        .commit_mergeable(MergeableCommit::new("canvasInvites", invite_row, 3).cells(
            BTreeMap::from([
                ("canvas".to_owned(), Value::Uuid(private_canvas.0)),
                ("userID".to_owned(), Value::Uuid(invited.0)),
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
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", public_canvas, 14)
                .made_by(uninvited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("public".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(true)),
                ])),
        )
        .unwrap();
    let [public_fate] = core.apply_sync_message(public_tx.1).unwrap().try_into().unwrap();
    assert!(matches!(
        public_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let private_tx = invited_writer
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", private_canvas, 15)
                .made_by(invited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("private".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(false)),
                ])),
        )
        .unwrap();
    let [private_fate] = core
        .apply_sync_message(private_tx.1)
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
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", blocked_canvas, 16)
                .made_by(uninvited)
                .cells(BTreeMap::from([
                    ("title".to_owned(), Value::String("blocked".to_owned())),
                    ("isPublic".to_owned(), Value::Bool(false)),
                ])),
        )
        .unwrap();
    let [blocked_fate] = core
        .apply_sync_message(blocked_tx.1)
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

    accept_global(
        &mut core,
        MergeableCommit::new("chats", public_chat, 10)
            .made_by(member)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("public".to_owned())),
                ("isPublic".to_owned(), Value::Bool(true)),
                ("createdBy".to_owned(), Value::Uuid(member.0)),
            ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chats", private_chat, 11)
            .made_by(member)
            .cells(BTreeMap::from([
                ("title".to_owned(), Value::String("private".to_owned())),
                ("isPublic".to_owned(), Value::Bool(false)),
                ("createdBy".to_owned(), Value::Uuid(member.0)),
            ])),
    );
    accept_global(
        &mut core,
        MergeableCommit::new("chatMembers", membership, 12).cells(BTreeMap::from([
            ("chatId".to_owned(), Value::Uuid(private_chat.0)),
            ("userId".to_owned(), Value::String(member.0.to_string())),
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
