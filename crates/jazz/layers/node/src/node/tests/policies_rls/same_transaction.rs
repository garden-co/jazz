// Write-policy evidence inside one transaction (INV-RLS-9).
//
// These tests stay at the node boundary: they hand the fate authority exact
// commit units (including units that never reach it, and units a client would
// only produce in a particular order), and only the authority's settled fate
// shows which rows a write policy's `exists` saw.

/// `shows` belong to their chief; a task may be written only under a show its
/// writer is chief of.
fn show_task_schema() -> JazzSchema {
    let under_my_show =
        public_outer_exists("shows", "id", "show", [public_claim_eq("chief", "sub")]);
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("shows")
                    .column("chief", PublicColumnType::Uuid)
                    .policies(public_owner_policies("chief")),
            )
            .table(
                PublicTableSchemaBuilder::new("tasks")
                    .fk_column("show", "shows")
                    .column("title", PublicColumnType::Text)
                    .policies(
                        public_write_policies(under_my_show).with_select(PublicPolicyExpr::True),
                    ),
            ),
    )
}

fn show_insert(show: RowUuid, chief: AuthorSubject, now_ms: u64) -> MergeableCommit {
    MergeableCommit::new("shows", show, now_ms)
        .made_by(chief)
        .cells(BTreeMap::from([(
            "chief".to_owned(),
            Value::Uuid(chief.test_uuid()),
        )]))
}

fn task_insert(
    task: RowUuid,
    show: RowUuid,
    author: AuthorSubject,
    now_ms: u64,
) -> MergeableCommit {
    MergeableCommit::new("tasks", task, now_ms)
        .made_by(author)
        .cells(BTreeMap::from([
            ("show".to_owned(), Value::Uuid(show.0)),
            ("title".to_owned(), Value::String("Load-in".to_owned())),
        ]))
}

/// The fate the authority reported for `tx_id` in one ingest's replies.
fn reported_fate(updates: &[SyncMessage], tx_id: TxId) -> Fate {
    updates
        .iter()
        .find_map(|update| match update {
            SyncMessage::FateUpdate {
                tx_id: reported,
                fate,
                ..
            } if *reported == tx_id => Some(fate.clone()),
            _ => None,
        })
        .expect("authority reports a fate for the unit")
}

/// Commit `commits` as one mergeable transaction on `writer` and hand its
/// commit unit to `core`, returning the transaction and its fate.
fn deliver_mergeable_transaction(
    writer: &mut NodeState,
    core: &mut NodeState,
    commits: Vec<MergeableCommit>,
) -> (TxId, Fate) {
    let tx_id = writer.commit_mergeable_many_settled(commits).unwrap();
    let unit = writer.commit_unit_for(tx_id).unwrap();
    let updates = core.apply_sync_message_settled(unit).unwrap();
    (tx_id, reported_fate(&updates, tx_id))
}

/// A child's `exists` sees its parent inserted earlier in the same mergeable
/// transaction, so the authority accepts the transaction the client already
/// applied optimistically (garden-co/jazz#3755).
///
/// ```text
/// alice ──tx{ show, task(show) }──► core ──► Accepted
/// ```
#[test]
fn same_transaction_parent_satisfies_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let show = row(0x10);
    let tasks = [row(0x11), row(0x12)];

    let (tx_id, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            show_insert(show, alice, 10),
            task_insert(tasks[0], show, alice, 10),
            task_insert(tasks[1], show, alice, 10),
        ],
    );

    assert_eq!(fate, Fate::Accepted);
    assert!(matches!(
        core.transaction_state_settled(tx_id),
        Some((Fate::Accepted, _, DurabilityTier::Global))
    ));
}

/// Transactions stay isolated: a parent inserted by another transaction that
/// has not reached the authority is not evidence for a child, and the parent
/// arriving later does not retroactively accept the child.
///
/// ```text
/// alice ──tx1{ show }──────────────────────────────┐ (in flight)
/// alice ──tx2{ task(show) }──► core ──► Rejected    │
///                              core ◄───────────────┘ ──► tx1 Accepted
/// ```
#[test]
fn other_uncommitted_transaction_parent_does_not_satisfy_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let show = row(0x20);
    let task = row(0x21);

    let show_tx = writer
        .commit_mergeable_many_settled(vec![show_insert(show, alice, 10)])
        .unwrap();
    let show_unit = writer.commit_unit_for(show_tx).unwrap();
    let (task_tx, task_fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![task_insert(task, show, alice, 11)],
    );
    assert_eq!(
        task_fate,
        Fate::Rejected(RejectionReason::AuthorizationDenied)
    );

    let updates = core.apply_sync_message_settled(show_unit).unwrap();
    assert_eq!(reported_fate(&updates, show_tx), Fate::Accepted);
    assert!(matches!(
        core.transaction_state_settled(task_tx),
        Some((Fate::Rejected(RejectionReason::AuthorizationDenied), _, _))
    ));
}

/// A committed parent that the same transaction deletes is still evidence for
/// a child: a unit's deletes do not hide committed rows from its own checks.
/// Once the deletion commits, the show no longer justifies later tasks.
///
/// ```text
/// alice ──tx{ show }────────────────────────► core ──► Accepted
/// alice ──tx{ delete show, task(show) }─────► core ──► Accepted
/// alice ──tx{ task(show) }──────────────────► core ──► Rejected
/// ```
#[test]
fn committed_parent_deleted_in_same_transaction_still_satisfies_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let show = row(0x30);

    let (_, fate) =
        deliver_mergeable_transaction(&mut writer, &mut core, vec![show_insert(show, alice, 10)]);
    assert_eq!(fate, Fate::Accepted);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            MergeableCommit::new("shows", show, 11)
                .made_by(alice)
                .deletion(DeletionEvent::Deleted),
            task_insert(row(0x32), show, alice, 11),
        ],
    );
    assert_eq!(fate, Fate::Accepted);

    // The committed deletion now hides the show from later transactions.
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![task_insert(row(0x34), show, alice, 12)],
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));
}

/// A parent the same transaction both inserts and deletes never existed as
/// far as the unit's checks are concerned, so it justifies nothing.
///
/// ```text
/// alice ──tx{ show, task(show), delete show }► core ──► Rejected
/// alice ──tx{ show2, task(show2) }───────────► core ──► Accepted
/// ```
#[test]
fn parent_inserted_and_deleted_in_same_transaction_does_not_satisfy_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let transient_show = row(0x31);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            show_insert(transient_show, alice, 12),
            task_insert(row(0x33), transient_show, alice, 12),
            MergeableCommit::new("shows", transient_show, 12)
                .made_by(alice)
                .deletion(DeletionEvent::Deleted),
        ],
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));

    // Planted positive: without the delete, the same pair is accepted.
    let kept_show = row(0x35);
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            show_insert(kept_show, alice, 13),
            task_insert(row(0x36), kept_show, alice, 13),
        ],
    );
    assert_eq!(fate, Fate::Accepted);
}

/// An exclusive transaction's writes are evidence for its own later writes,
/// exactly as in a mergeable transaction.
///
/// ```text
/// alice ──exclusive{ show, task(show) }──► core ──► Accepted
/// ```
#[test]
fn exclusive_transaction_parent_satisfies_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let show = row(0x40);
    let task = row(0x41);

    let open = OpenTransactionId::new();
    writer.open_exclusive_for_identity(open, alice).unwrap();
    writer
        .tx_write(
            open,
            "shows",
            show,
            BTreeMap::from([("chief".to_owned(), Value::Uuid(alice.test_uuid()))]),
            None,
        )
        .unwrap();
    writer
        .tx_write(
            open,
            "tasks",
            task,
            BTreeMap::from([
                ("show".to_owned(), Value::Uuid(show.0)),
                ("title".to_owned(), Value::String("Load-in".to_owned())),
            ]),
            None,
        )
        .unwrap();
    let (tx_id, unit) = writer.commit_exclusive_settled(open, alice, 10).unwrap();

    let updates = core.apply_sync_message_settled(unit).unwrap();
    assert_eq!(reported_fate(&updates, tx_id), Fate::Accepted);
}

/// Folders nest under a folder their writer owns.
fn folder_schema() -> JazzSchema {
    let under_my_folder =
        public_outer_exists("folders", "id", "parent", [public_claim_eq("owner", "sub")]);
    build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("folders")
                .column("owner", PublicColumnType::Uuid)
                .fk_column("parent", "folders")
                .policies(
                    public_write_policies(under_my_folder).with_select(PublicPolicyExpr::True),
                ),
        ),
    )
}

fn folder_insert(
    folder: RowUuid,
    parent: RowUuid,
    owner: AuthorSubject,
    now_ms: u64,
) -> MergeableCommit {
    MergeableCommit::new("folders", folder, now_ms)
        .made_by(owner)
        .cells(BTreeMap::from([
            ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
            ("parent".to_owned(), Value::Uuid(parent.0)),
        ]))
}

/// A commit unit carries its writes as a set, so a chain of dependent rows is
/// accepted in whichever order the authority checks them. Rows cannot justify
/// each other in a cycle, though: each row becomes evidence only after its
/// own check passes.
///
/// ```text
/// system ──{ root }──────────────────────────► core ──► Accepted
/// alice  ──tx{ leaf(parent: mid), mid(parent: root) }► core ──► Accepted
/// mallory ──tx{ a(parent: b), b(parent: a) }──► core ──► Rejected
/// ```
#[test]
fn same_transaction_chain_is_order_independent_but_cycles_do_not_self_justify() {
    let alice = user(0xa1);
    let mallory = user(0x3d);
    let schema = folder_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    install_test_uuid_sub_claim(&mut core, mallory);
    let root = row(0x50);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            MergeableCommit::new("folders", root, 10).cells(BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
                ("parent".to_owned(), Value::Uuid(root.0)),
            ])),
        ],
    );
    assert_eq!(fate, Fate::Accepted);

    let (leaf, mid) = (row(0x51), row(0x52));
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            folder_insert(leaf, mid, alice, 11),
            folder_insert(mid, root, alice, 11),
        ],
    );
    assert_eq!(fate, Fate::Accepted);

    let (a, b) = (row(0x53), row(0x54));
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            folder_insert(a, b, mallory, 12),
            folder_insert(b, a, mallory, 12),
        ],
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));
}

/// Members hold a role; a doc may be written only by an admin member.
fn membership_doc_schema() -> JazzSchema {
    let i_am_admin = PublicPolicyExpr::Exists {
        table: "memberships".to_owned(),
        condition: Box::new(PublicPolicyExpr::And(vec![
            public_claim_eq("member", "sub"),
            public_literal_eq("role", PublicValue::Text("admin".to_owned())),
        ])),
    };
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("memberships")
                    .column("member", PublicColumnType::Uuid)
                    .column("role", PublicColumnType::Text)
                    .policies(public_owner_policies("member")),
            )
            .table(
                PublicTableSchemaBuilder::new("docs")
                    .column("title", PublicColumnType::Text)
                    .policies(public_write_policies(i_am_admin).with_select(PublicPolicyExpr::True)),
            ),
    )
}

fn membership_write(
    membership: RowUuid,
    member: AuthorSubject,
    role: &str,
    now_ms: u64,
) -> MergeableCommit {
    MergeableCommit::new("memberships", membership, now_ms)
        .made_by(member)
        .cells(BTreeMap::from([
            ("member".to_owned(), Value::Uuid(member.test_uuid())),
            ("role".to_owned(), Value::String(role.to_owned())),
        ]))
}

fn doc_insert(doc: RowUuid, author: AuthorSubject, now_ms: u64) -> MergeableCommit {
    MergeableCommit::new("docs", doc, now_ms)
        .made_by(author)
        .cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("Run of show".to_owned()),
        )]))
}

/// Every write must also hold in the state the whole transaction leaves
/// behind. Demoting my own membership while inserting an admin-only doc is
/// rejected, although the doc's check passes against my committed admin
/// membership before the demotion is grounded.
///
/// ```text
/// alice ──tx{ membership(alice, admin) }────────────► core ──► Accepted
/// alice ──tx{ membership → viewer, doc }────────────► core ──► Rejected
/// alice ──tx{ doc }─────────────────────────────────► core ──► Accepted
/// ```
#[test]
fn demoting_own_membership_while_inserting_admin_only_doc_is_rejected() {
    let alice = user(0xa1);
    let schema = membership_doc_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let membership = row(0x60);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![membership_write(membership, alice, "admin", 10)],
    );
    assert_eq!(fate, Fate::Accepted);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            membership_write(membership, alice, "viewer", 11),
            doc_insert(row(0x61), alice, 11),
        ],
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));

    // Planted positive: the demotion was rejected, so alice is still an
    // admin and the same doc insert on its own is accepted.
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![doc_insert(row(0x62), alice, 12)],
    );
    assert_eq!(fate, Fate::Accepted);
}

/// A row the transaction restores is evidence for the transaction's other
/// writes once the restore's own checks pass, like an insert.
///
/// ```text
/// alice ──tx{ show }───────────────────────► core ──► Accepted
/// alice ──tx{ delete show }────────────────► core ──► Accepted
/// alice ──tx{ task(show) }─────────────────► core ──► Rejected
/// alice ──tx{ restore show, task(show) }───► core ──► Accepted
/// ```
#[test]
fn parent_restored_in_same_transaction_satisfies_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let show = row(0x70);

    let (_, fate) =
        deliver_mergeable_transaction(&mut writer, &mut core, vec![show_insert(show, alice, 10)]);
    assert_eq!(fate, Fate::Accepted);
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            MergeableCommit::new("shows", show, 11)
                .made_by(alice)
                .deletion(DeletionEvent::Deleted),
        ],
    );
    assert_eq!(fate, Fate::Accepted);

    // Planted negative: the deleted show justifies nothing on its own.
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![task_insert(row(0x71), show, alice, 12)],
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            MergeableCommit::new("shows", show, 13)
                .made_by(alice)
                .deletion(DeletionEvent::Restored),
            task_insert(row(0x72), show, alice, 13),
        ],
    );
    assert_eq!(fate, Fate::Accepted);
}

/// Folder `index` of a chain whose ids sort children before their parents,
/// so the authority's canonical order checks every child before its parent.
fn chain_folder(index: u128) -> RowUuid {
    RowUuid(uuid::Uuid::from_u128(0x0f00_0000 + index))
}

/// Insert `len` folders as one chain under `root`: folder `i` nests in
/// folder `i + 1`, and the last folder nests in `root`.
fn adversarial_folder_chain(
    len: u128,
    root: RowUuid,
    owner: AuthorSubject,
    now_ms: u64,
) -> Vec<MergeableCommit> {
    (0..len)
        .map(|index| {
            let parent = if index + 1 == len {
                root
            } else {
                chain_folder(index + 1)
            };
            folder_insert(chain_folder(index), parent, owner, now_ms)
        })
        .collect()
}

fn committed_root_folder(
    writer: &mut NodeState,
    core: &mut NodeState,
    root: RowUuid,
    owner: AuthorSubject,
) {
    let (_, fate) = deliver_mergeable_transaction(
        writer,
        core,
        vec![
            MergeableCommit::new("folders", root, 10).cells(BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ("parent".to_owned(), Value::Uuid(root.0)),
            ])),
        ],
    );
    assert_eq!(fate, Fate::Accepted);
}

/// A dependent chain whose canonical order is the worst case (every child
/// before its parent) is still accepted: each round grounds one more level.
///
/// ```text
/// alice ──tx{ f0(parent: f1), f1(parent: f2), …, f23(parent: root) }► core ──► Accepted
/// ```
#[test]
fn same_transaction_chain_in_adversarial_canonical_order_is_accepted() {
    let alice = user(0xa1);
    let schema = folder_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let root = row(0x80);
    committed_root_folder(&mut writer, &mut core, root, alice);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        adversarial_folder_chain(24, root, alice, 11),
    );
    assert_eq!(fate, Fate::Accepted);
}

/// An import-sized unit whose policy reads its own table is decided at a
/// bounded cost: its checks would read far more of the unit's own rows than
/// the evidence budget allows, so the authority rejects it as not supported
/// yet after a bounded number of evaluations instead of re-checking a
/// thousands-deep chain round after round.
///
/// ```text
/// alice ──tx{ 2000-folder chain, children first }► core ──► Rejected (not supported yet)
/// ```
#[test]
fn import_sized_same_table_unit_is_decided_at_bounded_cost() {
    const FOLDERS: u128 = 2_000;
    let alice = user(0xa1);
    let schema = folder_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let root = row(0x90);
    committed_root_folder(&mut writer, &mut core, root, alice);

    let tx_id = writer
        .commit_mergeable_many_settled(adversarial_folder_chain(FOLDERS, root, alice, 11))
        .unwrap();
    let unit = writer.commit_unit_for(tx_id).unwrap();
    crate::node::WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.set(0));
    let updates = core.apply_sync_message_settled(unit).unwrap();
    let evaluations = crate::node::WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.get());

    let Fate::Rejected(RejectionReason::MalformedCommit(reason)) = reported_fate(&updates, tx_id)
    else {
        panic!("an over-budget unit is rejected as not supported yet");
    };
    assert!(reason.contains("is not supported yet"), "{reason}");
    // The first round runs every check on committed state at no charge;
    // the budget then stops the overlaid re-checks after a few hundred.
    assert!(
        evaluations <= FOLDERS as usize + 200,
        "the budget stops evaluation early, after {evaluations} checks"
    );
}

/// The common import shape: thousands of rows whose policy reads their own
/// table, all nesting under committed parents. Every check passes on
/// committed state in the first round, which reads no overlaid rows, and the
/// monotone policy needs no post-state pass because the unit updates nothing.
///
/// ```text
/// alice ──tx{ 2000 folders(parent: committed root) }► core ──► Accepted, 2000 checks
/// ```
#[test]
fn import_sized_unit_under_committed_parents_is_checked_once_per_row() {
    const FOLDERS: u128 = 2_000;
    let alice = user(0xa1);
    let schema = folder_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let root = row(0x91);
    committed_root_folder(&mut writer, &mut core, root, alice);

    let tx_id = writer
        .commit_mergeable_many_settled(
            (0..FOLDERS)
                .map(|index| folder_insert(chain_folder(index), root, alice, 11))
                .collect(),
        )
        .unwrap();
    let unit = writer.commit_unit_for(tx_id).unwrap();
    crate::node::WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.set(0));
    let updates = core.apply_sync_message_settled(unit).unwrap();
    let evaluations = crate::node::WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.get());

    assert_eq!(reported_fate(&updates, tx_id), Fate::Accepted);
    assert_eq!(evaluations, FOLDERS as usize, "one check per row, no final pass");
}

/// A bulk update that takes away the evidence the same unit's inserts rely
/// on is rejected: each child passes against its committed parent, and the
/// post-state pass re-checks it against the parent's new owner.
///
/// ```text
/// alice ──tx{ 20 folders(parent: root) }────────────────────► core ──► Accepted
/// alice ──tx{ 20 folders keep owner, 20 children }──────────► core ──► Accepted
/// alice ──tx{ 20 folders → owner bob, 20 children }─────────► core ──► Rejected
/// ```
#[test]
fn bulk_update_removing_evidence_for_same_unit_inserts_is_rejected() {
    const PARENTS: u128 = 20;
    let alice = user(0xa1);
    let bob = user(0xb0);
    let schema = folder_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let root = row(0x92);
    committed_root_folder(&mut writer, &mut core, root, alice);
    let parent = |index: u128| RowUuid(uuid::Uuid::from_u128(0x0a00_0000 + index));
    let child = |round: u128, index: u128| {
        RowUuid(uuid::Uuid::from_u128(0x0b00_0000 + round * 0x1000 + index))
    };
    let reparent = |index: u128, owner: AuthorSubject, now_ms: u64| {
        MergeableCommit::new("folders", parent(index), now_ms)
            .made_by(alice)
            .cells(BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
                ("parent".to_owned(), Value::Uuid(root.0)),
            ]))
    };

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        (0..PARENTS)
            .map(|index| folder_insert(parent(index), root, alice, 11))
            .collect(),
    );
    assert_eq!(fate, Fate::Accepted);

    // Planted positive: rewriting the parents without changing their owner
    // still passes the post-state pass.
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        (0..PARENTS)
            .flat_map(|index| {
                [
                    reparent(index, alice, 12),
                    folder_insert(child(0, index), parent(index), alice, 12),
                ]
            })
            .collect(),
    );
    assert_eq!(fate, Fate::Accepted);

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        (0..PARENTS)
            .flat_map(|index| {
                [
                    reparent(index, bob, 13),
                    folder_insert(child(1, index), parent(index), alice, 13),
                ]
            })
            .collect(),
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));
}

/// Folders nest under a folder their writer owns, and only while not
/// archived: the `NOT` makes the policy non-monotone as far as the fate
/// authority's static analysis is concerned.
fn archivable_folder_schema() -> JazzSchema {
    let policy = PublicPolicyExpr::And(vec![
        public_outer_exists("folders", "id", "parent", [public_claim_eq("owner", "sub")]),
        PublicPolicyExpr::Not(Box::new(public_literal_eq(
            "archived",
            PublicValue::Boolean(true),
        ))),
    ]);
    build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("folders")
                .column("owner", PublicColumnType::Uuid)
                .column("archived", PublicColumnType::Boolean)
                .fk_column("parent", "folders")
                .policies(public_write_policies(policy).with_select(PublicPolicyExpr::True)),
        ),
    )
}

/// Two writes, a mid folder under a committed root and a leaf under the mid
/// folder, checked with and without a `NOT` in the policy. The mid folder
/// passes in the first round while the leaf is still ungrounded. A monotone
/// policy can't lose that pass to an added row, so it is not re-checked
/// (3 checks). A `NOT` policy keeps the full post-state pass (4 checks).
#[test]
fn not_policy_keeps_the_full_post_state_pass() {
    let alice = user(0xa1);
    let (mid, leaf) = (row(0xa2), row(0xa3));
    for (schema, archivable, expected_evaluations) in [
        (folder_schema(), false, 3),
        (archivable_folder_schema(), true, 4),
    ] {
        let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
        install_test_uuid_sub_claim(&mut core, alice);
        let root = row(0x93);
        // Folder cells for either schema: `archived` exists only in the
        // archivable one.
        let folder = |folder: RowUuid, parent: RowUuid, now_ms: u64| {
            let mut cells = BTreeMap::from([
                ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
                ("parent".to_owned(), Value::Uuid(parent.0)),
            ]);
            if archivable {
                cells.insert("archived".to_owned(), Value::Bool(false));
            }
            MergeableCommit::new("folders", folder, now_ms).cells(cells)
        };
        let (_, fate) =
            deliver_mergeable_transaction(&mut writer, &mut core, vec![folder(root, root, 10)]);
        assert_eq!(fate, Fate::Accepted);

        let tx_id = writer
            .commit_mergeable_many_settled(vec![
                folder(mid, root, 11).made_by(alice),
                folder(leaf, mid, 11).made_by(alice),
            ])
            .unwrap();
        let unit = writer.commit_unit_for(tx_id).unwrap();
        crate::node::WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.set(0));
        let updates = core.apply_sync_message_settled(unit).unwrap();
        let evaluations = crate::node::WRITE_POLICY_VERSION_EVALUATIONS.with(|count| count.get());

        assert_eq!(reported_fate(&updates, tx_id), Fate::Accepted);
        assert_eq!(
            evaluations, expected_evaluations,
            "archivable={archivable}: checks including the post-state pass"
        );
    }
}
