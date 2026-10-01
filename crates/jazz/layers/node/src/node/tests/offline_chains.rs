// Offline write chains and the ancestor merge (SPEC 4 §4.6).
//
// A writer that edits a row several times without hearing back from Core
// uploads a chain: each write's base names the settled image it rested on
// and the writer's own previous pending write. Core rebuilds what the writer
// saw from that image plus the writer's own patches, and applies a cell only
// when nothing else changed it since. These tests drive nodes directly so
// the seq Core assigns to every write is exact; the writers stay offline
// simply by not receiving Core's fates.

fn todo_edit(cells: &[(&str, &str)]) -> BTreeMap<String, Value> {
    cells
        .iter()
        .map(|(column, value)| ((*column).to_owned(), v(*value)))
        .collect()
}

/// An offline edit of `target` in `todos`: its commit unit, not yet sent.
fn offline_edit(
    writer: &mut NodeState,
    target: RowUuid,
    at_ms: u64,
    cells: &[(&str, &str)],
) -> (TxId, SyncMessage) {
    writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, at_ms).cells(todo_edit(cells)),
        )
        .unwrap()
}

/// The cells a write lost at Core, by column name (`_deletion` for the
/// deletion cell), read from the write's history record there.
fn lost_cells_at_core(core: &mut NodeState, tx: TxId) -> BTreeMap<String, Value> {
    let mut lost = BTreeMap::new();
    for version in core.query_versions_for_tx(tx).resolve().unwrap() {
        let schema_version = core
            .schema_version_for_alias(version.schema_version_alias())
            .unwrap();
        let table = core.table_in_schema(version.table(), schema_version).unwrap();
        let wire = core.lost_cells_for_wire(&version).unwrap();
        // Lost cells travel in the record's own authored spelling.
        let descriptor = crate::node::codec::history_record_descriptor(&table);
        for (slot, value) in crate::node::lost_cells::decode(&wire, |slot| {
            let field = match slot {
                0 => HistoryRowRecord::FIELD__DELETION_IDX,
                slot => HistoryRowRecord::USER_CELLS + slot as usize - 1,
            };
            Ok(descriptor.fields()[field].value_type.clone())
        })
        .unwrap()
        {
            let name = match slot {
                0 => DELETION_COLUMN_NAME.to_owned(),
                slot => table.columns[slot as usize - 1].name.clone(),
            };
            lost.insert(name, value);
        }
    }
    lost
}

fn lost_text(value: &str) -> Value {
    Value::Nullable(Some(Box::new(v(value))))
}

fn fate_of(message: &SyncMessage) -> &Fate {
    let SyncMessage::FateUpdate { fate, .. } = message else {
        panic!("expected a fate update, got {message:?}");
    };
    fate
}

fn assert_refused_base(message: &SyncMessage) {
    let Fate::Rejected(RejectionReason::MalformedCommit(reason)) = fate_of(message) else {
        panic!("Core must refuse a base it cannot resolve, got {message:?}");
    };
    assert!(reason.contains("not supported yet"), "{reason}");
}

/// The unit with every version's base replaced.
fn with_base(unit: SyncMessage, base: crate::protocol::RowBase) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected a commit unit");
    };
    SyncMessage::CommitUnit {
        tx,
        versions: versions
            .into_iter()
            .map(|version| version.with_base(base))
            .collect(),
    }
}

/// A Core holding `target` as {title: "base", body: "base"}, and `count`
/// writers that have synced it.
fn core_with_seeded_todo(
    target: RowUuid,
    writers: u8,
) -> (Vec<(tempfile::TempDir, NodeState)>, tempfile::TempDir, NodeState) {
    let schema = two_column_schema();
    let (core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_seed_dir, mut seeder) = open_node_with_schema(node(0x50), schema.clone());
    commit_mergeable_global(
        &mut seeder,
        &mut core,
        MergeableCommit::new("todos", target, 10).cells(todo_cells("base", "base")),
    );
    let mut nodes = Vec::new();
    for index in 0..writers {
        let (dir, mut writer) = open_node_with_schema(node(1 + index), schema.clone());
        sync_table_rows_to(&mut core, &mut writer, "todos");
        nodes.push((dir, writer));
    }
    (nodes, core_dir, core)
}

/// INV-HIST-8, INV-HIST-21: a 1000-edit offline chain merges against what
/// its writer saw, with other writers editing before, between and after it.
///
/// carol edits offline 1000 times: every edit retitles the row, and her
/// 500th also rewrites the body. dave changes the body before carol's chain
/// arrives, retitles between its two halves, and changes the body again
/// after it. carol's first 500 titles apply one after another (she saw each
/// of them); dave's title then changes a cell under her, so every later
/// carol title is lost, and so is her body (she never saw dave's body).
///
/// ```text
/// dave  ──body="dave before"──► core
/// carol ═offline═ title c0..c499 ──► core            title=c499
/// dave  ──title="dave between"──► core
/// carol ═offline═ title c500..c999, body ──► core     lost: title, body
/// dave  ──body="dave after"──► core
/// ```
#[test]
fn thousand_edit_offline_chain_merges_against_what_its_writer_saw() {
    let target = row(0x81);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (_, carol) = &mut writers[0];
    let mut units = Vec::new();
    for index in 0..1000_u64 {
        let title = format!("c{index}");
        let cells = if index == 500 {
            vec![("title", title.as_str()), ("body", "carol")]
        } else {
            vec![("title", title.as_str())]
        };
        units.push(offline_edit(carol, target, 100 + index, &cells));
    }
    assert_eq!(
        rows_at(carol, "todos", DurabilityTier::Local)[&target],
        todo_cells("c999", "carol")
    );
    let (_, dave) = &mut writers[1];

    let (_, before) = offline_edit(dave, target, 50, &[("body", "dave before")]);
    assert_accepted(&core_fate(&mut core, before));
    let mut units = units.into_iter();
    for (_, unit) in units.by_ref().take(500) {
        assert_accepted(&core_fate(&mut core, unit));
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c499", "dave before")
    );
    sync_table_rows_to(&mut core, dave, "todos");
    let (_, between) = offline_edit(dave, target, 2_000, &[("title", "dave between")]);
    assert_accepted(&core_fate(&mut core, between));
    let mut second_half = Vec::new();
    for (tx, unit) in units {
        assert_accepted(&core_fate(&mut core, unit));
        second_half.push(tx);
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave between", "dave before")
    );
    assert_eq!(
        lost_cells_at_core(&mut core, second_half[0]),
        BTreeMap::from([
            ("title".to_owned(), lost_text("c500")),
            ("body".to_owned(), lost_text("carol")),
        ])
    );
    assert_eq!(
        lost_cells_at_core(&mut core, second_half[499]),
        BTreeMap::from([("title".to_owned(), lost_text("c999"))])
    );

    sync_table_rows_to(&mut core, dave, "todos");
    let (_, after) = offline_edit(dave, target, 3_000, &[("body", "dave after")]);
    assert_accepted(&core_fate(&mut core, after));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave between", "dave after")
    );
}

/// INV-HIST-21: when the first write of a chain loses a cell, a later
/// chained write of the same cell is compared with the value its writer
/// wrote (the lost one), not with Core's winner, so it stays lost; a cell
/// nobody else touched still applies.
///
/// ```text
/// dave  ──title="dave"──► core
/// carol ═offline═ e1 title="c1" ──► core   lost: title
/// carol ═offline═ e2 title="c2", body="c2" ──► core   lost: title; body applies
/// ```
#[test]
fn chained_write_stays_lost_on_a_cell_its_predecessor_lost() {
    let target = row(0x82);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) =
        offline_edit(&mut writers[0].1, target, 101, &[("title", "c2"), ("body", "c2")]);
    let (_, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("title", "dave")]);

    assert_accepted(&core_fate(&mut core, dave_unit));
    let e1_fate = core_fate(&mut core, e1_unit);
    let e2_fate = core_fate(&mut core, e2_unit);
    assert_accepted(&e1_fate);
    assert_accepted(&e2_fate);
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave", "c2")
    );
    // carol predicts her own image when the fates arrive, then converges on
    // Core's post-image.
    let carol = &mut writers[0].1;
    carol.apply_sync_message_settled(e1_fate).unwrap();
    carol.apply_sync_message_settled(e2_fate).unwrap();
    sync_table_rows_to(&mut core, carol, "todos");
    for tier in [DurabilityTier::Local, DurabilityTier::Global] {
        assert_eq!(rows_at(carol, "todos", tier)[&target], todo_cells("dave", "c2"));
    }
    assert_eq!(
        lost_cells_at_core(&mut core, e1),
        BTreeMap::from([("title".to_owned(), lost_text("c1"))])
    );
    assert_eq!(
        lost_cells_at_core(&mut core, e2),
        BTreeMap::from([("title".to_owned(), lost_text("c2"))])
    );
}

/// A chained write whose pending predecessor Core rejected: the rejected
/// write is not in history and contributes nothing, so the chain's ancestor
/// is the settled image the writer started from.
///
/// erin's first offline edit retitles the row and pushes a counter past its
/// type's range over dave's concurrent increment, so Core rejects it. Her
/// second edit only rewrites the title again; nothing else changed the title
/// since the image she started from, so it applies.
///
/// ```text
/// dave ──count +1──► core                   count=i32::MAX
/// erin ═offline═ e1 count +1, title="e1" ──► core ──✗ Rejected (out of range)
/// erin ═offline═ e2 title="e2" ──► core     title="e2"
/// ```
#[test]
fn chained_write_over_a_rejected_predecessor_merges_against_the_settled_image() {
    let schema = counter_schema();
    let target = row(0x83);
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_seed_dir, mut seeder) = open_node_with_schema(node(0x50), schema.clone());
    commit_mergeable_global(
        &mut seeder,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(counter_cells(i32::MAX - 1, "base")),
    );
    let (_dave_dir, mut dave) = open_node_with_schema(node(1), schema.clone());
    let (_erin_dir, mut erin) = open_node_with_schema(node(2), schema);
    sync_table_rows_to(&mut core, &mut dave, "counters");
    sync_table_rows_to(&mut core, &mut erin, "counters");

    let (_, e1) = erin
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 100).cells(counter_cells(i32::MAX, "e1")),
        )
        .unwrap();
    let (_, e2) = erin
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 101)
                .cells(BTreeMap::from([("title".to_owned(), v("e2"))])),
        )
        .unwrap();
    let (_, dave_unit) = dave
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 50)
                .cells(BTreeMap::from([("count".to_owned(), Value::I32(i32::MAX))])),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, dave_unit));
    assert!(matches!(
        fate_of(&core_fate(&mut core, e1)),
        Fate::Rejected(RejectionReason::MalformedCommit(_))
    ));
    assert_accepted(&core_fate(&mut core, e2));
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, counter_cells(i32::MAX, "e2"))])
    );
}

/// A chain spanning several rows and transactions resolves each row's
/// ancestor on its own: a two-row transaction, then edits of each row, with
/// a concurrent writer touching only one cell of one row.
///
/// ```text
/// dave  ──B.title="dave"──► core
/// carol ═offline═ tx1 {A.title="c1", B.title="c1"}
///                 tx2 {A.body="c2", B.body="c2"}
///                 tx3 {B.title="c3"} ──► core
///       A = {c1, c2}; B = {dave, c2}, B.title lost by tx1 and tx3
/// ```
#[test]
fn offline_chain_over_several_rows_and_transactions_merges_each_row() {
    let first = row(0x84);
    let second = row(0x85);
    let schema = two_column_schema();
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_seed_dir, mut seeder) = open_node_with_schema(node(0x50), schema.clone());
    for target in [first, second] {
        commit_mergeable_global(
            &mut seeder,
            &mut core,
            MergeableCommit::new("todos", target, 10).cells(todo_cells("base", "base")),
        );
    }
    let (_carol_dir, mut carol) = open_node_with_schema(node(1), schema.clone());
    let (_dave_dir, mut dave) = open_node_with_schema(node(2), schema);
    sync_table_rows_to(&mut core, &mut carol, "todos");
    sync_table_rows_to(&mut core, &mut dave, "todos");

    let mut units = Vec::new();
    let mut txs = Vec::new();
    for (at_ms, writes) in [
        (100, vec![(first, ("title", "c1")), (second, ("title", "c1"))]),
        (101, vec![(first, ("body", "c2")), (second, ("body", "c2"))]),
        (102, vec![(second, ("title", "c3"))]),
    ] {
        let commits = writes
            .into_iter()
            .map(|(target, (column, value))| {
                MergeableCommit::new("todos", target, at_ms)
                    .cells(BTreeMap::from([(column.to_owned(), v(value))]))
            })
            .collect();
        let tx = carol.commit_mergeable_many_settled(commits).unwrap();
        let unit = carol.resident_commit_unit(tx).resolve().unwrap();
        txs.push(tx);
        units.push(unit);
    }
    let (_, dave_unit) = offline_edit(&mut dave, second, 50, &[("title", "dave")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    for unit in units {
        assert_accepted(&core_fate(&mut core, unit));
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([
            (first, todo_cells("c1", "c2")),
            (second, todo_cells("dave", "c2")),
        ])
    );
    for (tx, title) in [(txs[0], "c1"), (txs[2], "c3")] {
        assert_eq!(
            lost_cells_at_core(&mut core, tx),
            BTreeMap::from([("title".to_owned(), lost_text(title))])
        );
    }
    assert!(lost_cells_at_core(&mut core, txs[1]).is_empty());
}

/// INV-EDGE-16 for chains: a chain resent after a reconnect is the same set
/// of transactions. Core answers each with its stored fate and the row is
/// unchanged, also when another writer's edit was sequenced in between.
#[test]
fn offline_chain_resent_after_reconnect_is_idempotent() {
    let target = row(0x86);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let chain = (0..3)
        .map(|index| {
            offline_edit(
                &mut writers[0].1,
                target,
                100 + index,
                &[("title", &format!("c{index}"))],
            )
            .1
        })
        .collect::<Vec<_>>();
    let mut first_fates = Vec::new();
    for unit in chain.iter().take(2) {
        first_fates.push(core_fate(&mut core, unit.clone()));
    }
    let (_, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("body", "dave")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    // The connection drops before the third edit's fate; on reconnect the
    // whole chain is sent again.
    first_fates.push(core_fate(&mut core, chain[2].clone()));
    let settled = rows_at(&mut core, "todos", DurabilityTier::Global);
    assert_eq!(settled[&target], todo_cells("c2", "dave"));
    for (unit, first_fate) in chain.into_iter().zip(first_fates) {
        assert_eq!(core_fate(&mut core, unit), first_fate);
    }
    assert_eq!(rows_at(&mut core, "todos", DurabilityTier::Global), settled);
}

/// `_deletion` is merged like any cell: an offline chain that deletes and
/// then restores the row applies both (each saw the deletion state before
/// it), keeps a concurrent body edit, and applies its own title.
///
/// ```text
/// dave  ──body="dave"──► core
/// carol ═offline═ e1 delete; e2 restore; e3 title="c2" ──► core
///       = {c2, dave}, not deleted
/// ```
#[test]
fn offline_chain_deletes_and_restores_over_a_concurrent_edit() {
    let target = row(0x87);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (_, delete) = writers[0]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 100).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let (_, restore) = writers[0]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 101).deletion(DeletionEvent::Restored),
        )
        .unwrap();
    let (_, retitle) = offline_edit(&mut writers[0].1, target, 102, &[("title", "c2")]);
    let (_, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("body", "dave")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    assert_accepted(&core_fate(&mut core, delete));
    assert!(rows_at(&mut core, "todos", DurabilityTier::Global).is_empty());
    assert_accepted(&core_fate(&mut core, restore));
    assert_accepted(&core_fate(&mut core, retitle));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("c2", "dave"))])
    );
}

/// A concurrent delete wins over a chained update that did not see it: the
/// chain authors only titles, so the row's deletion state stays as Core
/// accepted it, while the titles still apply underneath.
///
/// ```text
/// dave  ──delete──► core
/// carol ═offline═ e1 title="c1"; e2 title="c2" ──► core   still deleted
/// ```
#[test]
fn concurrent_delete_survives_a_chained_update() {
    let target = row(0x88);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (_, e1) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2_tx, e2) = offline_edit(&mut writers[0].1, target, 101, &[("title", "c2")]);
    let (_, delete) = writers[1]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 50).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, delete));
    assert_accepted(&core_fate(&mut core, e1));
    assert_accepted(&core_fate(&mut core, e2));
    assert!(rows_at(&mut core, "todos", DurabilityTier::Global).is_empty());
    assert!(lost_cells_at_core(&mut core, e2_tx).is_empty());
}

/// A blind update (the writer holds no image of the row) has no base, so it
/// has no ancestor: arrival wins on every cell it authors, also over a
/// concurrent edit it never saw. An insert likewise applies whole.
///
/// ```text
/// dave  ──title="dave"──► core
/// frank (never loaded the row) ──title="frank"──► core   title="frank"
/// frank ──insert new row──► core
/// ```
#[test]
fn blind_update_and_insert_have_no_ancestor_and_apply() {
    let target = row(0x89);
    let fresh = row(0x8a);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (_frank_dir, mut frank) = open_node_with_schema(node(0x30), two_column_schema());
    let (frank_tx, blind) = offline_edit(&mut frank, target, 100, &[("title", "frank")]);
    let (_, dave_unit) = offline_edit(&mut writers[0].1, target, 50, &[("title", "dave")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    assert!(
        core.query_versions_for_tx(frank_tx).resolve().unwrap().is_empty(),
        "the blind update has not reached Core yet"
    );
    let SyncMessage::CommitUnit { versions, .. } = &blind else {
        unreachable!()
    };
    assert!(versions.iter().all(|version| version.base().is_empty()));
    assert_accepted(&core_fate(&mut core, blind));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target].get("title"),
        Some(&v("frank"))
    );

    let (_, insert) = frank
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", fresh, 101)
                .known_fresh_row()
                .cells(todo_cells("new", "row")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { versions, .. } = &insert else {
        unreachable!()
    };
    assert!(versions.iter().all(|version| version.base().is_empty()));
    assert_accepted(&core_fate(&mut core, insert));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&fresh],
        todo_cells("new", "row")
    );
}

/// INV-HIST-20: Core resolves a base exactly or refuses the write with a
/// not-supported-yet reason; it never guesses an ancestor. A base seq above
/// the row's current seq, a base seq of 0, a base seq naming a real seq that
/// holds no write of this row, a predecessor of another node and a
/// predecessor not older than the write are each refused, and the row is
/// unchanged.
#[test]
fn unresolvable_base_is_refused() {
    let target = row(0x8b);
    let other = row(0x8c);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 3);
    let (erin_tx, _) = offline_edit(&mut writers[2].1, target, 40, &[("body", "never sent")]);
    let (_, e1) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let SyncMessage::CommitUnit { tx: e1_tx, versions } = &e1 else {
        unreachable!()
    };
    let e1_tx = e1_tx.tx_id;
    let root = versions[0].base();
    assert!(root.seq.is_some() && root.pending.is_none());
    let refused = |core: &mut NodeState, base| {
        assert_refused_base(&core_fate(core, with_base(e1.clone(), base)));
    };

    // A base seq above the row's current seq.
    refused(
        &mut core,
        crate::protocol::RowBase {
            seq: Some(GlobalTime(root.seq.unwrap().0 + 1_000_000)),
            pending: None,
        },
    );
    // Seq 0 is where pending writes are kept; it names no accepted image.
    refused(
        &mut core,
        crate::protocol::RowBase {
            seq: Some(GlobalTime(0)),
            pending: None,
        },
    );
    // A predecessor written by another node.
    refused(
        &mut core,
        crate::protocol::RowBase {
            seq: root.seq,
            pending: Some(erin_tx),
        },
    );
    // A predecessor that is not older than the write itself.
    refused(
        &mut core,
        crate::protocol::RowBase {
            seq: root.seq,
            pending: Some(e1_tx),
        },
    );
    // A real seq below the row's current seq that holds a write of another
    // row, not of this one: Core holds no root there.
    let other_fate = core_fate(
        &mut core,
        offline_edit(&mut writers[1].1, other, 45, &[("title", "other row")]).1,
    );
    let SyncMessage::FateUpdate {
        fate: Fate::Accepted,
        global_time: Some(other_seq),
        ..
    } = other_fate
    else {
        panic!("the other row's insert is accepted: {other_fate:?}");
    };
    let (_, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("title", "dave")]);
    let dave_fate = core_fate(&mut core, dave_unit);
    let SyncMessage::FateUpdate {
        global_time: Some(dave_seq),
        ..
    } = dave_fate
    else {
        panic!("dave's title is accepted: {dave_fate:?}");
    };
    assert!(root.seq.unwrap() < other_seq && other_seq < dave_seq);
    let (_, e3) = offline_edit(&mut writers[0].1, target, 102, &[("body", "c3")]);
    assert_refused_base(&core_fate(
        &mut core,
        with_base(
            e3,
            crate::protocol::RowBase {
                seq: Some(other_seq),
                pending: None,
            },
        ),
    ));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave", "base")
    );
}

/// The fate update Core returned for `tx` among `messages`.
fn fate_for(messages: &[SyncMessage], tx: TxId) -> Option<&Fate> {
    messages.iter().find_map(|message| match message {
        SyncMessage::FateUpdate { tx_id, fate, .. } if *tx_id == tx => Some(fate),
        _ => None,
    })
}

/// INV-HIST-20: a chained write that reaches Core before its predecessor
/// is an ordering race, not an unresolvable base. Core parks it with no fate
/// and decides it right after the predecessor's fate is stored.
///
/// ```text
/// carol ═offline═ e1 title="c1", e2 title="c2"
/// carol ──e2──► core   parked, no fate
/// carol ──e1──► core   e1 accepted, then e2 accepted   title=c2
/// ```
#[test]
fn chained_write_arriving_before_its_predecessor_waits_for_it() {
    let target = row(0x90);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(&mut writers[0].1, target, 101, &[("title", "c2")]);
    let early = core.apply_sync_message_settled(e2_unit.clone()).unwrap();
    assert_eq!(fate_for(&early, e2), None, "e2 waits for e1: {early:?}");
    // A resend while it waits is still parked, not a conflict.
    let resent = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&resent, e2), None);
    let late = core.apply_sync_message_settled(e1_unit).unwrap();
    assert_eq!(fate_for(&late, e1), Some(&Fate::Accepted));
    assert_eq!(fate_for(&late, e2), Some(&Fate::Accepted));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c2", "base")
    );
    assert!(lost_cells_at_core(&mut core, e2).is_empty());
}

/// INV-HIST-20: Core may hold the predecessor as Pending (here relayed
/// before Core decided it). A chained write over it waits until Core
/// decides the predecessor, then merges against it.
///
/// ```text
/// core  ◄─relay── e1 (stored Pending)
/// carol ──e2──► core   parked
/// carol ──e1──► core   e1 accepted, then e2 accepted
/// ```
#[test]
fn chained_write_over_a_predecessor_core_holds_pending_waits_for_its_fate() {
    let target = row(0x91);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) =
        offline_edit(&mut writers[0].1, target, 101, &[("title", "c2"), ("body", "c2")]);
    let SyncMessage::CommitUnit { mut tx, versions } = e1_unit else {
        panic!("expected a commit unit");
    };
    // A relayed unit carries no permission subject; the same unit reaches
    // Core again later.
    tx.permission_subject = None;
    let e1_unit = SyncMessage::CommitUnit {
        tx: tx.clone(),
        versions: versions.clone(),
    };
    core.ingest_relay_commit_unit(tx, versions).resolve().unwrap();
    assert_eq!(
        core.transaction_state_settled(e1).map(|(fate, ..)| fate),
        Some(Fate::Pending)
    );
    let early = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&early, e2), None, "e2 waits for e1: {early:?}");
    let decided = core.apply_sync_message_settled(e1_unit).unwrap();
    assert_eq!(fate_for(&decided, e1), Some(&Fate::Accepted));
    assert_eq!(fate_for(&decided, e2), Some(&Fate::Accepted));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c2", "c2")
    );
}

/// Core's authority ingest of `unit` at `now_ms`, settled.
fn ingest_at(core: &mut NodeState, unit: SyncMessage, now_ms: u64) -> Vec<SyncMessage> {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected a commit unit");
    };
    let outcome =
        crate::local_executor::block_on(core.ingest_commit_unit(tx, versions, now_ms)).unwrap();
    settle_outcome(core, outcome).unwrap()
}

/// `unit` with its versions' row timestamps outside the HLC range: Core
/// refuses it before admission, as a malformed authored version.
fn with_malformed_timestamps(unit: SyncMessage) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected a commit unit");
    };
    let schema = two_column_schema();
    let table = schema.tables.iter().find(|table| table.name == "todos").unwrap();
    let versions = versions
        .into_iter()
        .map(|version| {
            VersionRecord::from_cells(
                table,
                version.schema_version(),
                version.row_uuid(),
                tx.made_by,
                u64::MAX,
                tx.made_by,
                u64::MAX,
                &todo_cells("bad", "bad"),
                None,
            )
            .unwrap()
            .with_base(version.base())
        })
        .collect();
    SyncMessage::CommitUnit { tx, versions }
}

/// INV-HIST-20: a predecessor that Core refuses still releases the write
/// parked on it, also when the refusal happens before admission (here a
/// malformed authored version). The released write merges against the
/// settled image, since a rejected predecessor contributes nothing. A
/// second parked write whose own copy was refused meanwhile gets no second
/// fate when its predecessor is decided.
///
/// ```text
/// carol ──e2──► core            parked on e1
/// carol ──e1 (malformed)──► core  e1 rejected, then e2 accepted
/// carol ──e4──► core            parked on e3
/// carol ──e4 (malformed)──► core  e4 rejected, parked copy dropped
/// carol ──e3──► core            e3 accepted, no fate for e4
/// ```
#[test]
fn rejected_predecessor_releases_the_write_parked_on_it() {
    let target = row(0x94);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    let (e1, e1_unit) = offline_edit(carol, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(carol, target, 101, &[("body", "c2")]);
    let parked = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&parked, e2), None);
    let decided = core
        .apply_sync_message_settled(with_malformed_timestamps(e1_unit))
        .unwrap();
    assert!(matches!(
        fate_for(&decided, e1),
        Some(Fate::Rejected(RejectionReason::MalformedCommit(_)))
    ));
    assert_eq!(fate_for(&decided, e2), Some(&Fate::Accepted));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("base", "c2")
    );

    let (e3, e3_unit) = offline_edit(carol, target, 102, &[("title", "c3")]);
    let (e4, e4_unit) = offline_edit(carol, target, 103, &[("title", "c4")]);
    assert_eq!(fate_for(&core.apply_sync_message_settled(e4_unit.clone()).unwrap(), e4), None);
    let refused = core
        .apply_sync_message_settled(with_malformed_timestamps(e4_unit))
        .unwrap();
    assert!(matches!(
        fate_for(&refused, e4),
        Some(Fate::Rejected(RejectionReason::MalformedCommit(_)))
    ));
    assert_eq!(core.parking.awaiting_predecessor.len(), 0);
    let decided = core.apply_sync_message_settled(e3_unit).unwrap();
    assert_eq!(fate_for(&decided, e3), Some(&Fate::Accepted));
    assert_eq!(fate_for(&decided, e4), None, "e4 was decided once already");
}

/// INV-HIST-20: a chain of parked writes drains in order once its first
/// predecessor is decided: e3 waits on e2, which waits on e1. A resend of a
/// parked unit under another authority is a conflict.
///
/// ```text
/// carol ──e3──► core   parked on e2
/// carol ──e2──► core   parked on e1
/// carol ──e1──► core   e1, e2, e3 accepted   title=c3
/// ```
#[test]
fn two_level_parked_chain_drains_in_order() {
    let target = row(0x95);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    let (e1, e1_unit) = offline_edit(carol, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(carol, target, 101, &[("body", "c2")]);
    let (e3, e3_unit) = offline_edit(carol, target, 102, &[("title", "c3")]);
    assert_eq!(fate_for(&core.apply_sync_message_settled(e3_unit.clone()).unwrap(), e3), None);
    assert_eq!(fate_for(&core.apply_sync_message_settled(e2_unit).unwrap(), e2), None);
    assert_eq!(core.parking.awaiting_predecessor.len(), 2);
    // The same unit resent under another authority conflicts with the
    // parked one, as in the schema parker.
    assert!(matches!(
        crate::local_executor::block_on(core.apply_sync_message_with_ingest_context(
            e3_unit,
            Some(CommitUnitIngestContext {
                identity: AuthorSubject::SYSTEM,
                trust: CommitUnitTrust::TrustedBackend,
                admitted_write_authorization: false,
                version_receipts_validated: false,
            }),
        )),
        Err(Error::ConflictingCommitUnit(tx)) if tx == e3
    ));
    assert_eq!(core.parking.awaiting_predecessor.len(), 2);
    let decided = core.apply_sync_message_settled(e1_unit).unwrap();
    let order = decided
        .iter()
        .filter_map(|message| match message {
            SyncMessage::FateUpdate { tx_id, fate: Fate::Accepted, .. } => Some(*tx_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(order, vec![e1, e2, e3]);
    assert_eq!(core.parking.awaiting_predecessor.len(), 0);
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c3", "c2")
    );
    assert!(lost_cells_at_core(&mut core, e3).is_empty());
}

/// Parked writes live in memory only. After a Core restart the writer
/// resends its pending writes, and the chain converges as if nothing had
/// been parked.
///
/// ```text
/// carol ──e2──► core   parked on e1
/// core restarts        parking empty
/// carol ──e2, e1──► core   e2 parked again, then e1 and e2 accepted
/// ```
#[test]
fn parked_writes_converge_after_a_core_restart() {
    let target = row(0x96);
    let (mut writers, core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    let (e1, e1_unit) = offline_edit(carol, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(carol, target, 101, &[("body", "c2")]);
    assert_eq!(fate_for(&core.apply_sync_message_settled(e2_unit.clone()).unwrap(), e2), None);
    drop(core);
    let mut core = reopen_node_at(&core_dir, node(9), two_column_schema());
    assert_eq!(core.parking.awaiting_predecessor.len(), 0);
    assert_eq!(core.transaction_state_settled(e2), None, "a parked write is never stored");
    let resent = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&resent, e2), None);
    let decided = core.apply_sync_message_settled(e1_unit).unwrap();
    assert_eq!(fate_for(&decided, e1), Some(&Fate::Accepted));
    assert_eq!(fate_for(&decided, e2), Some(&Fate::Accepted));
    let image = todo_cells("c1", "c2");
    assert_eq!(rows_at(&mut core, "todos", DurabilityTier::Global)[&target], image);
    // carol converges on Core's image once the fates arrive.
    for message in decided {
        carol.apply_sync_message_settled(message).unwrap();
    }
    sync_table_rows_to(&mut core, carol, "todos");
    for tier in [DurabilityTier::Local, DurabilityTier::Global] {
        assert_eq!(rows_at(carol, "todos", tier)[&target], image);
    }
}

/// N1: a writer cannot fill Core's parking. Writes naming a predecessor
/// Core never sees park only up to the per-writer-node cap; the next one is
/// refused with a fate. A session that may not make the write at all (its
/// identity is not the write's author, or it is anonymous) is refused
/// before it can park.
#[test]
fn predecessor_parking_is_bounded_and_admits_first() {
    let target = row(0x97);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    // Cheap admission runs before parking. Each session write below names
    // a predecessor Core never sees.
    let (_erin_dir, mut erin) = open_node_with_schema(node(0x30), two_column_schema());
    let anonymous = AuthorSubject::reserved(AuthorSubject::ANONYMOUS_ISSUER, "visitor").unwrap();
    assert!(anonymous.is_anonymous());
    let erin_author = AuthorSubject::for_test_bytes([0x30; 16]);
    for (index, (made_by, identity)) in [
        // The session is not the write's author.
        (erin_author, AuthorSubject::for_test_bytes([0x31; 16])),
        // The session is its author, but anonymous sessions are read-only.
        (anonymous, anonymous),
    ]
    .into_iter()
    .enumerate()
    {
        let at_ms = 2_000 + 2 * index as u64;
        offline_edit(&mut erin, target, at_ms, &[("title", "p")]);
        let (w, w_unit) = offline_edit(&mut erin, target, at_ms + 1, &[("title", "w")]);
        let SyncMessage::CommitUnit { mut tx, versions } = w_unit else {
            panic!("expected a commit unit");
        };
        assert!(versions[0].base().pending.is_some(), "w is chained");
        tx.made_by = made_by;
        let w_unit = SyncMessage::CommitUnit { tx, versions };
        let outcome = crate::local_executor::block_on(core.apply_sync_message_with_ingest_context(
            w_unit,
            Some(CommitUnitIngestContext {
                identity,
                trust: CommitUnitTrust::Session,
                admitted_write_authorization: false,
                version_receipts_validated: false,
            }),
        ));
        if made_by.is_anonymous() {
            // An anonymous author cannot even be recorded on a rejection.
            assert!(matches!(outcome, Err(Error::UnadmittedWriteAuthor)));
        } else {
            let messages = settle_outcome(&mut core, outcome.unwrap()).unwrap();
            assert_eq!(
                fate_for(&messages, w),
                Some(&Fate::Rejected(RejectionReason::AuthorizationDenied)),
                "{identity:?}"
            );
        }
        assert_eq!(core.parking.awaiting_predecessor.len(), 0);
    }

    let carol = &mut writers[0].1;
    let cap = crate::node::ingest::MAX_PREDECESSOR_PARKED_PER_WRITER_NODE;
    let mut last = None;
    for index in 0..=cap as u64 {
        let (tx, unit) = offline_edit(carol, target, 1_000 + 2 * index, &[("title", "flood")]);
        // Each write names a predecessor of carol's that never reaches Core.
        let fake = TxId::new(TxTime(tx.time.0 - 1), tx.node);
        let SyncMessage::CommitUnit { versions, .. } = &unit else {
            panic!("expected a commit unit");
        };
        let base = crate::protocol::RowBase {
            pending: Some(fake),
            ..versions[0].base()
        };
        let messages = core.apply_sync_message_settled(with_base(unit, base)).unwrap();
        if (index as usize) < cap {
            assert_eq!(fate_for(&messages, tx), None, "write {index} parks");
        } else {
            last = Some((tx, messages));
        }
    }
    let (tx, messages) = last.unwrap();
    let Some(Fate::Rejected(RejectionReason::MalformedCommit(reason))) = fate_for(&messages, tx)
    else {
        panic!("the write over the cap is refused: {messages:?}");
    };
    assert!(reason.contains("too many writes"), "{reason}");
    assert_eq!(core.parking.awaiting_predecessor.len(), cap);
}

/// N1/N4: a parked write whose predecessor never reaches Core gets a fate
/// once its wait expires, on the next authority ingest. The refusal is
/// stored, so the predecessor arriving afterwards does not decide it again.
///
/// ```text
/// carol ──e2──► core            t=1000, parked on e1
/// dave  ──other row──► core     t=1000+TTL: e2 refused
/// carol ──e1──► core            e1 accepted, no second fate for e2
/// ```
#[test]
fn parked_write_expires_with_a_fate() {
    let target = row(0x98);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(&mut writers[0].1, target, 101, &[("title", "c2")]);
    let (dave_tx, dave_unit) =
        offline_edit(&mut writers[1].1, row(0x99), 50, &[("title", "dave")]);
    assert_eq!(fate_for(&ingest_at(&mut core, e2_unit, 1_000), e2), None);
    let ttl = crate::node::ingest::PREDECESSOR_PARK_TTL_MS;
    let later = ingest_at(&mut core, dave_unit, 1_000 + ttl);
    assert_eq!(fate_for(&later, dave_tx), Some(&Fate::Accepted));
    let Some(Fate::Rejected(RejectionReason::MalformedCommit(reason))) = fate_for(&later, e2)
    else {
        panic!("the expired write gets a fate: {later:?}");
    };
    assert!(reason.contains("got no fate"), "{reason}");
    assert_eq!(core.parking.awaiting_predecessor.len(), 0);
    let decided = ingest_at(&mut core, e1_unit, 1_000 + ttl);
    assert_eq!(fate_for(&decided, e1), Some(&Fate::Accepted));
    assert_eq!(fate_for(&decided, e2), None);
}

/// `counters` with plain `title` and `body`, and in the descendant also
/// `notes`, all text.
fn plain_counters_schema(with_notes: bool) -> JazzSchema {
    let mut columns = vec![
        PublicColumnDescriptor::new("title", PublicColumnType::Text),
        PublicColumnDescriptor::new("body", PublicColumnType::Text),
    ];
    if with_notes {
        columns.push(PublicColumnDescriptor::new("notes", PublicColumnType::Text));
    }
    let source = [(
        PublicTableName::new("counters"),
        PublicTableSchema::new(PublicRowDescriptor::new(columns)),
    )]
    .into_iter()
    .collect::<PublicSchema>();
    compile_public_test_schema(&source)
}

/// The ancestor is compared with the current image by physical column
/// across schema versions: a v1 write's title, made over an image bob has
/// since retitled under v2, loses; its body, which nobody else changed,
/// applies. The post-image takes the v1 layout.
///
/// ```text
/// carol (v1) ──{title, body}="base"──► core          image v1
/// bob   (v2, blind) ──title="bob"──► core             image v2
/// carol (v1) ═offline═ title="c", body="c" ──► core   = {bob, c}, lost: title
/// ```
#[test]
fn ancestor_compares_cells_by_physical_column_across_schema_versions() {
    let target = row(0x8c);
    let (_core_dir, mut core) = core_with_descendant_schema(
        plain_counters_schema(false),
        plain_counters_schema(true),
        add_notes_lens(),
    );
    let (_carol_dir, mut carol) = open_node_with_schema(node(1), plain_counters_schema(false));
    let (_bob_dir, mut bob) = open_node_with_schema(node(2), plain_counters_schema(true));
    let base_cells = BTreeMap::from([("title".to_owned(), v("base")), ("body".to_owned(), v("base"))]);
    commit_mergeable_global(
        &mut carol,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(base_cells),
    );
    let (_, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20).cells(title_cells("bob")),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, bob_unit));
    let (carol_tx, carol_unit) = carol
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 30).cells(BTreeMap::from([
                ("title".to_owned(), v("c")),
                ("body".to_owned(), v("c")),
            ])),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, carol_unit));
    let rows = core
        .current_rows_for_schema(
            "counters",
            plain_counters_schema(false).version_id(),
            DurabilityTier::Global,
        )
        .resolve()
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(
        rows[&target],
        BTreeMap::from([("title".to_owned(), v("bob")), ("body".to_owned(), v("c"))])
    );
    let lost = core
        .query_versions_for_tx(carol_tx)
        .resolve()
        .unwrap()
        .into_iter()
        .map(|version| core.lost_cells_for_wire(&version).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(lost.len(), 1);
    assert!(!lost[0].is_empty(), "carol's title is recorded as lost");
}

/// The fast paths (a base at the current seq, and a chain with no
/// concurrent write) give exactly what the full ancestor rule gives: the
/// same post-images and the same lost cells, byte for byte.
#[test]
fn ancestor_fast_paths_match_the_full_rule() {
    fn run(full_rule: bool) -> (Vec<u8>, Vec<Vec<u8>>) {
        crate::node::ingest::FULL_ANCESTOR_RULE.with(|full| full.set(full_rule));
        let target = row(0x8d);
        let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
        let mut txs = Vec::new();
        // A chain nobody else interrupts: the second fast path.
        for index in 0..4 {
            let (tx, unit) = offline_edit(
                &mut writers[0].1,
                target,
                100 + index,
                &[("title", &format!("c{index}"))],
            );
            assert_accepted(&core_fate(&mut core, unit));
            txs.push(tx);
        }
        // A write over the current image: the first fast path.
        sync_table_rows_to(&mut core, &mut writers[1].1, "todos");
        let (tx, unit) = offline_edit(&mut writers[1].1, target, 200, &[("body", "dave")]);
        assert_accepted(&core_fate(&mut core, unit));
        txs.push(tx);
        // A chain continued after a concurrent write: the full rule.
        let (tx, unit) =
            offline_edit(&mut writers[0].1, target, 104, &[("title", "c4"), ("body", "c4")]);
        assert_accepted(&core_fate(&mut core, unit));
        txs.push(tx);
        crate::node::ingest::FULL_ANCESTOR_RULE.with(|full| full.set(false));
        let image = core
            .query_global_winner("todos", target)
            .resolve()
            .unwrap()
            .unwrap();
        let mut lost = Vec::new();
        for tx in txs {
            for version in core.query_versions_for_tx(tx).resolve().unwrap() {
                lost.push(version.lost_cells_raw().unwrap());
            }
        }
        (image.record.borrowed().raw().to_vec(), lost)
    }
    let fast = run(false);
    let full = run(true);
    assert_eq!(fast, full);
    assert!(fast.1.last().is_some_and(|lost| !lost.is_empty()));
    // A chain with no concurrent write after its base, whose predecessor
    // lost a cell: the chain fast path must not apply there.
    assert_eq!(predecessor_lost_after_sync(false), predecessor_lost_after_sync(true));
}

/// The chain scenario where a chained predecessor lost a cell after its
/// writer had already received the concurrent value, run with the fast paths
/// on or off. Returns Core's image, the title it holds and each write's
/// stored lost cells.
fn predecessor_lost_after_sync(full_rule: bool) -> (Vec<u8>, BTreeMap<RowUuid, BTreeMap<String, Value>>, Vec<Vec<u8>>) {
    crate::node::ingest::FULL_ANCESTOR_RULE.with(|full| full.set(full_rule));
    let target = row(0x8e);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    // carol edits offline over the seeded image.
    let (p1, p1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "a")]);
    // dave's title is accepted first, and carol receives Core's image.
    let (dave, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("title", "m")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    sync_table_rows_to(&mut core, &mut writers[0].1, "todos");
    // carol's next edit rests on dave's image plus her own pending title.
    let (w, w_unit) = offline_edit(&mut writers[0].1, target, 101, &[("title", "b")]);
    let SyncMessage::CommitUnit { versions, .. } = &w_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(versions[0].base().pending, Some(p1));
    assert!(versions[0].base().seq.is_some());
    // Core accepts carol's first edit after dave's, so it loses the title.
    assert_accepted(&core_fate(&mut core, p1_unit));
    assert_accepted(&core_fate(&mut core, w_unit));
    crate::node::ingest::FULL_ANCESTOR_RULE.with(|full| full.set(false));
    let image = core
        .query_global_winner("todos", target)
        .resolve()
        .unwrap()
        .unwrap();
    let mut lost = Vec::new();
    for tx in [dave, p1, w] {
        for version in core.query_versions_for_tx(tx).resolve().unwrap() {
            lost.push(version.lost_cells_raw().unwrap());
        }
    }
    (
        image.record.borrowed().raw().to_vec(),
        rows_at(&mut core, "todos", DurabilityTier::Global),
        lost,
    )
}

/// INV-HIST-21: a chained write whose predecessor lost a cell compares that
/// cell against the predecessor's own value, also when every write after its
/// base is its writer's own.
///
/// ```text
/// carol ═offline═ p1 title="a" (base: seed)
/// dave  ──title="m"──► core                        title=m
/// core  ──image(title=m)──► carol
/// carol ═offline═ w  title="b" (base: dave's seq, pending p1)
/// carol ──p1──► core   lost: title (seed ≠ m)
/// carol ──w ──► core   ancestor title=a ≠ m: lost: title
/// ```
#[test]
fn chained_write_over_a_predecessor_that_lost_a_cell_after_a_sync_stays_lost() {
    let (_, rows, lost) = predecessor_lost_after_sync(false);
    assert_eq!(rows[&row(0x8e)], todo_cells("m", "base"));
    assert!(lost[0].is_empty(), "dave's title applies");
    assert!(!lost[1].is_empty(), "carol's first title is lost");
    assert!(!lost[2].is_empty(), "carol's chained title is lost too");
}

/// History keeps pending writes at seq 0, ahead of every accepted write of
/// the row, so the row's last history key is its newest accepted write. The
/// local winner (which permission advice reads) must still be the newest
/// pending write: here a pending local delete of a settled row hides it, and
/// a pending restore over it shows it again.
///
/// ```text
/// core ──image(seq n)──► carol          settled, live
/// carol ── pending delete ──            local winner: deleted
/// carol ── pending restore ──           local winner: live
/// ```
#[test]
fn local_winner_is_the_newest_pending_write_over_a_settled_row() {
    let target = row(0x8f);
    let (mut writers, _core_dir, _core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    assert!(carol.local_current_row_exists("todos", target).resolve().unwrap());
    carol
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 100).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    assert_eq!(
        carol
            .query_local_winner("todos", target)
            .resolve()
            .unwrap()
            .unwrap()
            .deletion(),
        Some(DeletionEvent::Deleted)
    );
    assert!(!carol.local_current_row_exists("todos", target).resolve().unwrap());
    carol
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 101).deletion(DeletionEvent::Restored),
        )
        .unwrap();
    assert!(carol.local_current_row_exists("todos", target).resolve().unwrap());
}

/// A writer's base names its own newest pending write to the row, also when
/// a foreign pending write it relays is newer in its overlay: Core's chain
/// is the writer's own writes, so a foreign predecessor could never be
/// resolved.
///
/// ```text
/// relay ── p title="r1" ──                   own pending
/// client ── f body="f1" ──► relay            relayed, pending, newer
/// relay ── w title="r2" ──                   base.pending = p, not f
/// ```
#[test]
fn base_names_the_writers_own_pending_write_over_a_relayed_one() {
    let target = row(0x92);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (p, p_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "r1")]);
    let (f, f_unit) = offline_edit(&mut writers[1].1, target, 150, &[("body", "f1")]);
    let SyncMessage::CommitUnit { mut tx, versions } = f_unit else {
        panic!("expected a commit unit");
    };
    tx.permission_subject = None;
    let relay = &mut writers[0].1;
    relay
        .ingest_relay_commit_unit(tx.clone(), versions.clone())
        .resolve()
        .unwrap();
    let overlay = relay
        .query_local_winner_in_branch("todos", &BranchKey::default(), target)
        .resolve()
        .unwrap()
        .unwrap();
    assert_eq!(relay.version_tx_id(&overlay).unwrap(), f, "the relayed write is newest");
    let (_, w_unit) = offline_edit(relay, target, 200, &[("title", "r2")]);
    let SyncMessage::CommitUnit { versions: w_versions, .. } = &w_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(w_versions[0].base().pending, Some(p));
    // Core resolves the chain: every write applies.
    assert_accepted(&core_fate(&mut core, p_unit));
    assert_accepted(&core_fate(&mut core, SyncMessage::CommitUnit { tx, versions }));
    assert_accepted(&core_fate(&mut core, w_unit));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("r2", "f1")
    );
}

/// Lost cells are spelled with the write's own schema version's enum tags,
/// and a chained write's ancestor re-tags them into the lineage's physical
/// registry before comparing them with the stored image.
///
/// Core publishes `base -> A (+ a) -> A2 (+ a2)` and then `base -> B (+ b)`,
/// so B's `b` (authored tag 1) is physical tag 3. carol and dave write under
/// B; carol wrote the seed. carol's first edit sets `b` over the seed after
/// dave already did, so
/// it is lost and its lost cell reads as B's tag 1. Her chained edit then
/// sets `base` over what she saw (`b`, her own lost value), which is also
/// the current value, so it applies.
///
/// ```text
/// carol ──status=base──► core                  seed
/// dave  ──status=b──► core                     status=b (physical 3)
/// carol ═offline═ e1 status=b ──► core          lost: status=b (tag 1)
/// carol ═offline═ e2 status=base ──► core       ancestor b == current: applies
/// ```
#[test]
fn lost_enum_cells_use_the_writers_tags_and_compare_by_physical_tag() {
    let base = enum_projection_schema(&["base"]);
    let a = SchemaVersion::new(enum_projection_schema(&["base", "a"]));
    let a2 = SchemaVersion::new(enum_projection_schema(&["base", "a", "a2"]));
    let b_schema = enum_projection_schema(&["base", "b"]);
    let b = SchemaVersion::new(b_schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), base.clone());
    for (schema, lens) in [
        (a.clone(), enum_identity_lens(base.version_id(), a.id)),
        (a2.clone(), enum_identity_lens(a.id, a2.id)),
        (b.clone(), enum_identity_lens(base.version_id(), b.id)),
    ] {
        publish_schema_lineage(&mut core, schema, lens, Vec::<String>::new(), Vec::<String>::new())
            .unwrap();
    }
    core.activate_catalogue_schema_settled(CurrentWriteSchema { revision: 1, schema: b.id })
        .unwrap();
    let b_mapping = &core.catalogue.physical_mappings[&b.id].tables["items"];
    let physical_cases = core
        .physical_scalar_enum_cases(b_mapping.table_id, b_mapping.columns["status"])
        .unwrap();
    assert_eq!(physical_cases.len(), 4, "base, a, a2, b");
    assert_eq!(physical_cases[3].introducing_schema, b.id, "b is physical tag 3");

    let target = row(0x93);
    let status = |tag: u8| {
        BTreeMap::from([
            ("title".to_owned(), v("t")),
            ("status".to_owned(), Value::EnumTag(tag)),
        ])
    };
    // carol writes the seed, so she holds its settled image; dave writes
    // blind.
    let (_carol_dir, mut carol) = open_node_with_schema(node(1), b_schema.clone());
    let (_dave_dir, mut dave) = open_node_with_schema(node(2), b_schema.clone());
    commit_mergeable_global(
        &mut carol,
        &mut core,
        MergeableCommit::new("items", target, 10).cells(status(0)),
    );
    let edit = |writer: &mut NodeState, at_ms: u64, tag: u8| {
        writer
            .commit_mergeable_unit_settled(
                MergeableCommit::new("items", target, at_ms)
                    .cells(BTreeMap::from([("status".to_owned(), Value::EnumTag(tag))])),
            )
            .unwrap()
    };
    let (e1, e1_unit) = edit(&mut carol, 100, 1);
    let (e2, e2_unit) = edit(&mut carol, 101, 0);
    let (_, dave_unit) = edit(&mut dave, 50, 1);

    assert_accepted(&core_fate(&mut core, dave_unit));
    assert_accepted(&core_fate(&mut core, e1_unit));
    assert_accepted(&core_fate(&mut core, e2_unit));
    assert_eq!(
        lost_cells_at_core(&mut core, e1),
        BTreeMap::from([(
            "status".to_owned(),
            Value::Nullable(Some(Box::new(Value::EnumTag(1))))
        )]),
        "e1's lost status is B's authored tag for b"
    );
    assert_eq!(lost_cells_at_core(&mut core, e2), BTreeMap::new(), "e2 applies");
    let rows = core
        .current_rows_for_schema("items", b.id, DurabilityTier::Global)
        .resolve()
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows[&target], status(0));
}
