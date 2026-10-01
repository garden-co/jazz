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
        let descriptor = version.record.descriptor();
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
/// not-supported-yet reason; it never guesses an ancestor. A chained write
/// whose predecessor Core never received, a base seq Core holds no write of
/// the row at, and a predecessor of another node are each refused, and the
/// row is unchanged.
#[test]
fn unresolvable_base_is_refused() {
    let target = row(0x8b);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 3);
    let (erin_tx, _) = offline_edit(&mut writers[2].1, target, 40, &[("body", "never sent")]);
    let (_, e1) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (_, e2) = offline_edit(&mut writers[0].1, target, 101, &[("title", "c2")]);
    let SyncMessage::CommitUnit { versions, .. } = &e1 else {
        unreachable!()
    };
    let root = versions[0].base();
    assert!(root.seq.is_some() && root.pending.is_none());

    // e2 arrives without e1: its predecessor is unknown here.
    assert_refused_base(&core_fate(&mut core, e2.clone()));
    // A base seq above the row's current seq.
    let missing_root = crate::protocol::RowBase {
        seq: Some(GlobalTime(root.seq.unwrap().0 + 1_000_000)),
        pending: None,
    };
    assert_refused_base(&core_fate(&mut core, with_base(e1.clone(), missing_root)));
    // A predecessor written by another node.
    let foreign = crate::protocol::RowBase {
        seq: root.seq,
        pending: Some(erin_tx),
    };
    assert_refused_base(&core_fate(&mut core, with_base(e1, foreign)));
    // A base seq below the current one that names no write of the row.
    let (_, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("title", "dave")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    let (_, e3) = offline_edit(&mut writers[0].1, target, 102, &[("body", "c3")]);
    let no_write_there = crate::protocol::RowBase {
        seq: Some(GlobalTime(root.seq.unwrap().0 + 1)),
        pending: None,
    };
    assert_refused_base(&core_fate(&mut core, with_base(e3, no_write_there)));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave", "base")
    );
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
}
