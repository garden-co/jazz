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

/// A parent the same transaction deletes is not evidence for a child, whether
/// the parent was committed before the transaction or inserted by it.
///
/// ```text
/// alice ──tx{ show }──────────────────────────► core ──► Accepted
/// alice ──tx{ delete show, task(show) }───────► core ──► Rejected
/// alice ──tx{ show2, task(show2), delete show2 }► core ──► Rejected
/// ```
#[test]
fn parent_deleted_in_same_transaction_does_not_satisfy_child_exists_policy() {
    let alice = user(0xa1);
    let schema = show_task_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    install_test_uuid_sub_claim(&mut core, alice);
    let show = row(0x30);
    let later_show = row(0x31);

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
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));

    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![
            show_insert(later_show, alice, 12),
            task_insert(row(0x33), later_show, alice, 12),
            MergeableCommit::new("shows", later_show, 12)
                .made_by(alice)
                .deletion(DeletionEvent::Deleted),
        ],
    );
    assert_eq!(fate, Fate::Rejected(RejectionReason::AuthorizationDenied));

    // Planted positive: the committed show still authorizes a task on its own.
    let (_, fate) = deliver_mergeable_transaction(
        &mut writer,
        &mut core,
        vec![task_insert(row(0x34), show, alice, 13)],
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
