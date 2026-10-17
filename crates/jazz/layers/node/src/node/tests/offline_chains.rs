// Offline write chains, arrival-order merge and the maybe-conflicting
// derivation (SPEC 4 §4.6).
//
// A writer that edits a row several times without hearing back from Core
// uploads a chain: each write's base names the settled image it rested on
// and the writer's own previous pending write. Core applies every write as a
// patch in the order it accepts them (last arrival wins per authored cell),
// keeps one writer's chain in order, and history derives from each write's
// base whether it was made over the row's latest accepted image. These tests
// drive nodes directly so the seq Core assigns to every write is exact; the
// writers stay offline simply by not receiving Core's fates.

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

/// What Core's history derives for `tx`'s write to `target` in `table`
/// (SPEC 4 §4.6, "Maybe conflicting").
fn conflict_in(
    core: &mut NodeState,
    table: &str,
    target: RowUuid,
    tx: TxId,
) -> crate::node::ingest::WriteConflict {
    core.write_conflict(table, &BranchKey::default(), target, tx)
        .resolve()
        .unwrap()
        .expect("Core holds the write's accepted record")
}

fn conflict(core: &mut NodeState, target: RowUuid, tx: TxId) -> crate::node::ingest::WriteConflict {
    conflict_in(core, "todos", target, tx)
}

/// Whether `tx`'s write to `target` in `todos` is maybe conflicting.
fn flagged(core: &mut NodeState, target: RowUuid, tx: TxId) -> bool {
    conflict(core, target, tx).maybe_conflicting
}

/// Every accepted write of `target` in `todos`, by transaction, with what
/// history derives for it.
fn row_conflicts(
    core: &mut NodeState,
    target: RowUuid,
) -> BTreeMap<TxId, crate::node::ingest::WriteConflict> {
    core.row_write_conflicts("todos", &BranchKey::default(), target)
        .resolve()
        .unwrap()
        .into_iter()
        .collect()
}

/// Core's post-image of `tx`'s single row: its deletion state and cells.
fn post_image_at_core(
    core: &mut NodeState,
    tx: TxId,
) -> (Option<DeletionEvent>, BTreeMap<String, Value>) {
    let versions = core.stored_versions_for_tx_for_test(tx).resolve().unwrap();
    assert_eq!(versions.len(), 1, "one row written");
    let version = &versions[0];
    let schema_version = core
        .schema_version_for_alias(version.schema_version_alias())
        .unwrap();
    let table = core.table_in_schema(version.table(), schema_version).unwrap();
    (version.deletion(), version.cells(&table).unwrap())
}

fn fate_of(message: &SyncMessage) -> &Fate {
    let SyncMessage::FateUpdate { fate, .. } = message else {
        panic!("expected a fate update, got {message:?}");
    };
    fate
}

/// The seq Core accepted a write at, from its fate.
fn accepted_seq(message: &SyncMessage) -> GlobalTime {
    let SyncMessage::FateUpdate {
        fate: Fate::Accepted,
        global_time: Some(seq),
        ..
    } = message
    else {
        panic!("expected an accepted fate, got {message:?}");
    };
    *seq
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

/// INV-HIST-8, INV-HIST-21: a 1000-edit offline chain applies in order, every
/// write of it, with other writers editing before, between and after it.
/// carol never receives Core's image while offline, so every write of her
/// chain was made over the seed and is maybe conflicting: dave's first body
/// was accepted before any of it. Each names the dave writes it overrides:
/// her 500th (title and body) both of his first two, the later ones his
/// title, the earlier ones nothing.
///
/// carol edits offline 1000 times: every edit retitles the row, and her
/// 500th also rewrites the body. dave changes the body before carol's chain
/// arrives, retitles between its two halves, and changes the body again
/// after it, each time over the latest image.
///
/// ```text
/// dave  ──body="dave before"──► core
/// carol ═offline═ title c0..c499 ──► core            title=c499   all flagged
/// dave  ──title="dave between"──► core
/// carol ═offline═ title c500..c999, body ──► core     title=c999   all flagged
/// dave  ──body="dave after"──► core                  body="dave after"
/// ```
#[test]
fn thousand_edit_offline_chain_applies_in_order_and_flags_only_interrupted_writes() {
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

    let (before_tx, before) = offline_edit(dave, target, 50, &[("body", "dave before")]);
    assert_accepted(&core_fate(&mut core, before));
    let mut units = units.into_iter();
    let mut first_half = Vec::new();
    for (tx, unit) in units.by_ref().take(500) {
        assert_accepted(&core_fate(&mut core, unit));
        first_half.push(tx);
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c499", "dave before")
    );
    sync_table_rows_to(&mut core, dave, "todos");
    let (between_tx, between) = offline_edit(dave, target, 2_000, &[("title", "dave between")]);
    assert_accepted(&core_fate(&mut core, between));
    let mut second_half = Vec::new();
    for (tx, unit) in units {
        assert_accepted(&core_fate(&mut core, unit));
        second_half.push(tx);
    }
    // Every chained write applied, in order, over dave's title.
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c999", "carol")
    );

    sync_table_rows_to(&mut core, dave, "todos");
    let (after_tx, after) = offline_edit(dave, target, 3_000, &[("body", "dave after")]);
    assert_accepted(&core_fate(&mut core, after));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c999", "dave after")
    );

    let conflicts = row_conflicts(&mut core, target);
    // The seed, dave's three writes and carol's 1000.
    assert_eq!(conflicts.len(), 1004);
    let flagged = conflicts
        .iter()
        .filter(|(_, conflict)| conflict.maybe_conflicting)
        .map(|(tx, _)| *tx)
        .collect::<BTreeSet<_>>();
    assert_eq!(
        flagged,
        first_half.iter().chain(&second_half).copied().collect::<BTreeSet<_>>(),
        "every chained write, none of dave's"
    );
    // The first half retitles only; dave changed only the body.
    for tx in &first_half {
        assert!(conflicts[tx].overlapping.is_empty());
    }
    // carol's 500th edit overrides dave's body, which she never saw, and his
    // title; the later ones his title. Her own writes never appear.
    assert_eq!(
        conflicts[&second_half[0]].overlapping,
        vec![before_tx, between_tx]
    );
    for tx in &second_half[1..] {
        assert_eq!(conflicts[tx].overlapping, vec![between_tx]);
    }
    for tx in [before_tx, between_tx, after_tx] {
        assert!(!conflicts[&tx].maybe_conflicting, "dave wrote over the latest image");
    }
}

/// A chained write whose pending predecessor Core rejected resolves to the
/// predecessor's own base: the rejected write is not in history.
///
/// erin's first offline edit retitles the row and pushes a counter past its
/// type's range over dave's concurrent increment, so Core rejects it. Her
/// second edit only rewrites the title again and applies. Its base resolves
/// to the seed erin's chain started from, and dave's increment came after
/// it, so it is maybe conflicting; dave touched only the counter, so its
/// conflict analysis is empty.
///
/// ```text
/// dave ──count +1──► core                   count=i32::MAX
/// erin ═offline═ e1 count +1, title="e1" ──► core ──✗ Rejected (out of range)
/// erin ═offline═ e2 title="e2" ──► core     title="e2"   flagged, no overlap
/// ```
#[test]
fn chained_write_over_a_rejected_predecessor_resolves_to_the_predecessors_base() {
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
    let (e2_tx, e2) = erin
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 101)
                .cells(BTreeMap::from([("title".to_owned(), v("e2"))])),
        )
        .unwrap();
    let (dave_tx, dave_unit) = dave
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 50)
                .cells(BTreeMap::from([("count".to_owned(), Value::I32(i32::MAX))])),
        )
        .unwrap();
    let dave_seq = accepted_seq(&core_fate(&mut core, dave_unit));
    assert!(matches!(
        fate_of(&core_fate(&mut core, e1)),
        Fate::Rejected(RejectionReason::MalformedCommit(_))
    ));
    assert_accepted(&core_fate(&mut core, e2));
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, counter_cells(i32::MAX, "e2"))])
    );

    let seed_seq = conflict_in(&mut core, "counters", target, dave_tx).previous;
    assert!(seed_seq.is_some_and(|seed| seed < dave_seq));
    let e2_conflict = conflict_in(&mut core, "counters", target, e2_tx);
    assert_eq!(e2_conflict.resolved_base, seed_seq, "the rejected e1's own base");
    assert_eq!(e2_conflict.previous, Some(dave_seq));
    assert!(e2_conflict.maybe_conflicting);
    assert!(e2_conflict.overlapping.is_empty(), "dave touched only the counter");
}

/// A chain spanning several rows and transactions is ordered and flagged per
/// row: a two-row transaction, then edits of each row, with a concurrent
/// writer touching only one cell of one row before the chain arrives. Every
/// chained write to that row is maybe conflicting; those retitling it name
/// dave's write.
///
/// ```text
/// dave  ──B.title="dave"──► core
/// carol ═offline═ tx1 {A.title="c1", B.title="c1"}
///                 tx2 {A.body="c2", B.body="c2"}
///                 tx3 {B.title="c3"} ──► core
///       A = {c1, c2}; B = {c3, c2}; every write to B is flagged
/// ```
#[test]
fn offline_chain_over_several_rows_and_transactions_flags_each_row() {
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
    let (dave_tx, dave_unit) = offline_edit(&mut dave, second, 50, &[("title", "dave")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    for unit in units {
        assert_accepted(&core_fate(&mut core, unit));
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([
            (first, todo_cells("c1", "c2")),
            (second, todo_cells("c3", "c2")),
        ])
    );
    for tx in &txs[..2] {
        assert!(!flagged(&mut core, first, *tx), "nobody else wrote row A");
    }
    for (tx, overlapping) in [
        (txs[0], vec![dave_tx]),
        (txs[1], Vec::new()),
        (txs[2], vec![dave_tx]),
    ] {
        let on_b = conflict(&mut core, second, tx);
        assert!(on_b.maybe_conflicting, "carol never saw dave's title");
        assert_eq!(on_b.overlapping, overlapping);
    }
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

/// `_deletion` is a cell like any other: an offline chain that deletes and
/// then restores the row applies both, keeps a concurrent body edit, and
/// applies its own title. Every write of the chain is maybe conflicting,
/// since dave's body was accepted after the seed it rests on; none overlaps
/// dave's body.
///
/// ```text
/// dave  ──body="dave"──► core
/// carol ═offline═ e1 delete; e2 restore; e3 title="c2" ──► core
///       = {c2, dave}, not deleted; e1, e2, e3 flagged
/// ```
#[test]
fn offline_chain_deletes_and_restores_over_a_concurrent_edit() {
    let target = row(0x87);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (delete_tx, delete) = writers[0]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 100).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    let (restore_tx, restore) = writers[0]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 101).deletion(DeletionEvent::Restored),
        )
        .unwrap();
    let (retitle_tx, retitle) = offline_edit(&mut writers[0].1, target, 102, &[("title", "c2")]);
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
    for tx in [delete_tx, restore_tx, retitle_tx] {
        let chained = conflict(&mut core, target, tx);
        assert!(chained.maybe_conflicting);
        assert!(chained.overlapping.is_empty(), "dave touched only the body");
    }
}

/// INV-HIST-8, INV-HIST-21: an edit whose base predates a delete Core
/// already accepted does not resurrect the row. An update does not author
/// `_deletion`, so the row stays deleted; the edits are accepted, recorded
/// in history and applied underneath the deletion, and both are maybe
/// conflicting (neither overlaps the delete). A later restore reveals them.
///
/// ```text
/// dave  ──delete──► core
/// carol ═offline═ e1 title="c1"; e2 title="c2" ──► core   still deleted, both flagged
/// erin  ──restore──► core                                 = {c2, base}
/// ```
#[test]
fn edit_after_an_accepted_delete_stays_deleted_and_is_flagged() {
    let target = row(0x88);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 3);
    let (e1_tx, e1) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2_tx, e2) = offline_edit(&mut writers[0].1, target, 101, &[("title", "c2")]);
    let (delete_tx, delete) = writers[1]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 50).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, delete));
    assert_accepted(&core_fate(&mut core, e1));
    assert_accepted(&core_fate(&mut core, e2));
    assert!(rows_at(&mut core, "todos", DurabilityTier::Global).is_empty());
    // Core's post-image of each edit holds its title under the deletion.
    assert_eq!(
        post_image_at_core(&mut core, e2_tx),
        (Some(DeletionEvent::Deleted), todo_cells("c2", "base"))
    );

    assert!(!flagged(&mut core, target, delete_tx));
    for tx in [e1_tx, e2_tx] {
        let edit = conflict(&mut core, target, tx);
        assert!(edit.maybe_conflicting, "the delete came after the chain's base");
        assert!(
            edit.overlapping.is_empty(),
            "the delete authored only `_deletion`"
        );
    }

    // erin restores the row over the image she holds, the seed: an
    // explicitly authored `_deletion` applies by arrival.
    let (restore_tx, restore) = writers[2]
        .1
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 200).deletion(DeletionEvent::Restored),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, restore));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("c2", "base"))])
    );
    let restore_conflict = conflict(&mut core, target, restore_tx);
    assert!(restore_conflict.maybe_conflicting);
    assert_eq!(restore_conflict.overlapping, vec![delete_tx]);
}

/// INV-HIST-21: a write with no base is never maybe conflicting. A blind
/// update (the writer holds no image of the row) applies over a concurrent
/// edit it never saw, and an insert applies whole.
///
/// ```text
/// dave  ──title="dave"──► core
/// frank (never loaded the row) ──title="frank"──► core   title="frank", not flagged
/// frank ──insert new row──► core                          not flagged
/// ```
#[test]
fn inserts_and_blind_updates_are_never_flagged() {
    let target = row(0x89);
    let fresh = row(0x8a);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (_frank_dir, mut frank) = open_node_with_schema(node(0x30), two_column_schema());
    let (frank_tx, blind) = offline_edit(&mut frank, target, 100, &[("title", "frank")]);
    let (_, dave_unit) = offline_edit(&mut writers[0].1, target, 50, &[("title", "dave")]);
    let dave_seq = accepted_seq(&core_fate(&mut core, dave_unit));
    assert!(
        core.stored_versions_for_tx_for_test(frank_tx).resolve().unwrap().is_empty(),
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
    let blind_conflict = conflict(&mut core, target, frank_tx);
    assert!(!blind_conflict.maybe_conflicting);
    assert_eq!(blind_conflict.resolved_base, None);
    assert!(blind_conflict.overlapping.is_empty());
    assert_eq!(
        blind_conflict.previous,
        Some(dave_seq),
        "a record precedes the blind update, yet it is not flagged"
    );

    let (insert_tx, insert) = frank
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
    let insert_conflict = conflict(&mut core, fresh, insert_tx);
    assert!(!insert_conflict.maybe_conflicting);
    assert_eq!(insert_conflict.previous, None, "the row's first record");
    // The seed is an insert too.
    let seed = row_conflicts(&mut core, target)
        .into_values()
        .find(|conflict| conflict.previous.is_none())
        .expect("the seed is the row's first record");
    assert!(!seed.maybe_conflicting);
}

/// INV-HIST-20: Core resolves a base exactly or refuses the write with a
/// not-supported-yet reason; it never guesses another base. A base seq above
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

/// Whether Core refused `tx` among `messages` as arriving after a later
/// write of its own writer (`INV-HIST-23`).
fn refused_as_late(messages: &[SyncMessage], tx: TxId) -> bool {
    matches!(
        fate_for(messages, tx),
        Some(Fate::Rejected(RejectionReason::MalformedCommit(reason)))
            if reason.contains("arrived after a later write of its writer")
    )
}

/// INV-HIST-20: Core never waits for a pending predecessor. A writer's
/// writes reach Core in order, so a predecessor Core has not decided never
/// arrives: the chained write applies as written over Core's row, like a
/// write whose predecessor Core rejected. The base is not guessed.
///
/// INV-HIST-23: the predecessor arriving afterwards anyway is a protocol
/// violation. Core refuses it and the row keeps the newer write.
///
/// ```text
/// carol ═offline═ e1 title="c1", e2 body="c2"
/// carol ──e2──► core   e2 accepted   title=base body=c2
/// carol ──e1──► core   e1 rejected (late)   row unchanged
/// ```
#[test]
fn chained_write_over_an_unknown_predecessor_applies_and_the_late_predecessor_is_refused() {
    let target = row(0x90);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(&mut writers[0].1, target, 101, &[("body", "c2")]);
    let SyncMessage::CommitUnit { versions, .. } = &e2_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(versions[0].base().pending, Some(e1), "e2 is chained on e1");
    let early = core.apply_sync_message_settled(e2_unit.clone()).unwrap();
    assert_eq!(fate_for(&early, e2), Some(&Fate::Accepted), "{early:?}");
    let image = todo_cells("base", "c2");
    assert_eq!(rows_at(&mut core, "todos", DurabilityTier::Global)[&target], image);
    assert!(core.parking.parked_commit_units.is_empty());
    // A redelivery of e2 is the same accepted write.
    let resent = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&resent, e2), Some(&Fate::Accepted));
    let late = core.apply_sync_message_settled(e1_unit).unwrap();
    assert!(refused_as_late(&late, e1), "{late:?}");
    assert_eq!(rows_at(&mut core, "todos", DurabilityTier::Global)[&target], image);
    assert!(!flagged(&mut core, target, e2), "nobody else wrote the row");
}

/// INV-HIST-23: the late-predecessor check also finds the writer's later
/// write when another writer's accepted write is newer than it, by reading
/// the row's history after the late write's base.
///
/// ```text
/// carol ──e2──► core   e2 accepted
/// dave  ──d───► core   d accepted (the row's current image)
/// carol ──e1──► core   e1 rejected (late)
/// ```
#[test]
fn late_predecessor_behind_another_writers_write_is_refused() {
    let target = row(0x9a);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(&mut writers[0].1, target, 101, &[("body", "c2")]);
    let accepted = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&accepted, e2), Some(&Fate::Accepted));
    let dave = accept_edit(&mut core, &mut writers[1].1, target, 102, &[("title", "dave")]);
    let late = core.apply_sync_message_settled(e1_unit).unwrap();
    assert!(refused_as_late(&late, e1), "{late:?}");
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave", "c2")
    );
    // dave had not seen carol's e2, which Core accepted before his write.
    assert!(flagged(&mut core, target, dave));
}

/// INV-HIST-20: Core may hold the predecessor as Pending (here relayed
/// before Core decided it). That is not a fate either: the chained write
/// applies as written, and the predecessor arriving for a decision later
/// is refused as late.
///
/// ```text
/// core  ◄─relay── e1 (stored Pending)
/// carol ──e2──► core   e2 accepted
/// carol ──e1──► core   e1 rejected (late)
/// ```
#[test]
fn chained_write_over_a_predecessor_core_holds_pending_applies() {
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
    let applied = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&applied, e2), Some(&Fate::Accepted), "{applied:?}");
    let late = core.apply_sync_message_settled(e1_unit).unwrap();
    assert!(refused_as_late(&late, e1), "{late:?}");
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c2", "c2")
    );
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

/// INV-HIST-20, INV-HIST-21: once Core refuses the predecessor (here before
/// admission, as a malformed authored version), the chained write applies
/// over the settled row, which the rejected predecessor never reached. Its
/// base resolves to the predecessor's own base, the seed, which is still the
/// record before it, so it is not maybe conflicting.
///
/// ```text
/// carol ──e1 (malformed)──► core  e1 rejected
/// carol ──e2──► core              e2 accepted   body=c2
/// ```
#[test]
fn chained_write_after_a_rejected_predecessor_applies_over_the_settled_row() {
    let target = row(0x94);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    let (e1, e1_unit) = offline_edit(carol, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(carol, target, 101, &[("body", "c2")]);
    let decided = core
        .apply_sync_message_settled(with_malformed_timestamps(e1_unit))
        .unwrap();
    assert!(matches!(
        fate_for(&decided, e1),
        Some(Fate::Rejected(RejectionReason::MalformedCommit(_)))
    ));
    let applied = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&applied, e2), Some(&Fate::Accepted));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("base", "c2")
    );
    let e2_conflict = conflict(&mut core, target, e2);
    assert!(!e2_conflict.maybe_conflicting);
    assert_eq!(e2_conflict.resolved_base, e2_conflict.previous);
}

/// INV-HIST-23: a two-level chain whose last write reaches Core first. It
/// applies as written; both of its predecessors then arrive late and are
/// refused, so the row never goes back to an older image.
///
/// ```text
/// carol ──e3──► core       e3 accepted   title=c3
/// carol ──e1, e2──► core   both rejected (late)
/// ```
#[test]
fn two_level_chain_arriving_backwards_keeps_the_newest_write() {
    let target = row(0x95);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    let (e1, e1_unit) = offline_edit(carol, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(carol, target, 101, &[("body", "c2")]);
    let (e3, e3_unit) = offline_edit(carol, target, 102, &[("title", "c3")]);
    let answer = core.apply_sync_message_settled(e3_unit).unwrap();
    assert_eq!(fate_for(&answer, e3), Some(&Fate::Accepted));
    for (tx, unit) in [(e2, e2_unit), (e1, e1_unit)] {
        let answer = core.apply_sync_message_settled(unit).unwrap();
        assert!(refused_as_late(&answer, tx), "{answer:?}");
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c3", "base")
    );
}

/// The ordinary case: a chain that reaches Core in order is accepted in
/// order, and nothing in it is refused as late.
#[test]
fn chain_arriving_in_order_is_accepted_in_order() {
    let target = row(0x96);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = &mut writers[0].1;
    let (e1, e1_unit) = offline_edit(carol, target, 100, &[("title", "c1")]);
    let (e2, e2_unit) = offline_edit(carol, target, 101, &[("body", "c2")]);
    let (e3, e3_unit) = offline_edit(carol, target, 102, &[("title", "c3")]);
    let mut order = Vec::new();
    for unit in [e1_unit, e2_unit, e3_unit] {
        for message in core.apply_sync_message_settled(unit).unwrap() {
            if let SyncMessage::FateUpdate { tx_id, fate, .. } = message {
                assert_eq!(fate, Fate::Accepted, "{tx_id:?}");
                order.push(tx_id);
            }
        }
    }
    assert_eq!(order, vec![e1, e2, e3]);
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c3", "c2")
    );
    for tx in [e1, e2, e3] {
        assert!(!flagged(&mut core, target, tx), "nobody else wrote the row");
    }
}

/// A session that may not make the write at all (its identity is not the
/// write's author, or it is anonymous) is refused, even though its write
/// names a predecessor Core never saw: an unknown predecessor admits nothing.
#[test]
fn unadmitted_writer_is_refused_over_an_unknown_predecessor() {
    let target = row(0x97);
    let (_writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
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
    }
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

/// Authored cells apply by physical column across schema versions: a v1
/// write made over an image bob has since retitled under v2 overrides bob's
/// title by arrival, and its conflict analysis names bob's write (the same
/// physical column). The post-image takes the v1 layout.
///
/// ```text
/// carol (v1) ──{title, body}="base"──► core          image v1
/// bob   (v2, blind) ──title="bob"──► core             image v2
/// carol (v1) ═offline═ title="c", body="c" ──► core   = {c, c}, flagged: bob
/// ```
#[test]
fn authored_cells_apply_by_physical_column_across_schema_versions() {
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
    let (bob_tx, bob_unit) = bob
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
        BTreeMap::from([("title".to_owned(), v("c")), ("body".to_owned(), v("c"))])
    );
    assert!(!conflict_in(&mut core, "counters", target, bob_tx).maybe_conflicting);
    let carol_conflict = conflict_in(&mut core, "counters", target, carol_tx);
    assert!(carol_conflict.maybe_conflicting);
    assert_eq!(carol_conflict.overlapping, vec![bob_tx]);
}

/// A chained write over a predecessor that Core accepted after a concurrent
/// edit: the predecessor is maybe conflicting (it overrides dave's title),
/// while the chained write, made over dave's image plus the predecessor,
/// only has its own predecessor after its seen seq (dave's): it resolves to
/// the predecessor's seq, the record before it, so it is not.
///
/// ```text
/// carol ═offline═ p1 title="a" (base: seed)
/// dave  ──title="m"──► core                        title=m
/// core  ──image(title=m)──► carol
/// carol ═offline═ w  title="b" (base: dave's seq, pending p1)
/// carol ──p1──► core   title=a, flagged: dave
/// carol ──w ──► core   title=b, not flagged
/// ```
#[test]
fn chained_write_over_a_predecessor_accepted_after_a_concurrent_edit() {
    let target = row(0x8e);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (p1, p1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "a")]);
    let (dave, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("title", "m")]);
    assert_accepted(&core_fate(&mut core, dave_unit));
    sync_table_rows_to(&mut core, &mut writers[0].1, "todos");
    let (w, w_unit) = offline_edit(&mut writers[0].1, target, 101, &[("title", "b")]);
    let SyncMessage::CommitUnit { versions, .. } = &w_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(versions[0].base().pending, Some(p1));
    assert!(versions[0].base().seq.is_some());
    assert_accepted(&core_fate(&mut core, p1_unit));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("a", "base")
    );
    assert_accepted(&core_fate(&mut core, w_unit));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("b", "base")
    );
    assert!(!flagged(&mut core, target, dave));
    let p1_conflict = conflict(&mut core, target, p1);
    assert!(p1_conflict.maybe_conflicting);
    assert_eq!(p1_conflict.overlapping, vec![dave]);
    let w_conflict = conflict(&mut core, target, w);
    assert_eq!(w_conflict.resolved_base, w_conflict.previous);
    assert!(!w_conflict.maybe_conflicting);
}

/// INV-HIST-21: a write's seen seq is the larger of its own base seq and its
/// chain root's settled base seq. carol's first edit is accepted, but its
/// fate has not reached her when she receives Core's newer image (which
/// counts it and dave's later edit); her next edit names that image's seq
/// and her still-pending first edit. She saw everything up to that seq, so
/// it is not maybe conflicting.
///
/// ```text
/// carol ═offline═ p title="p" ──► core          accepted; fate not delivered
/// dave  ──body="dave"──► core                   (after seeing p)
/// core  ──image(p, dave)──► carol
/// carol ═offline═ w body="w" (base: dave's seq, pending p) ──► core   not flagged
/// ```
#[test]
fn chained_write_over_an_image_newer_than_its_predecessor_is_not_flagged() {
    let target = row(0x8d);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (p, p_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "p")]);
    let p_seq = accepted_seq(&core_fate(&mut core, p_unit));
    sync_table_rows_to(&mut core, &mut writers[1].1, "todos");
    let (dave, dave_unit) = offline_edit(&mut writers[1].1, target, 150, &[("body", "dave")]);
    let dave_seq = accepted_seq(&core_fate(&mut core, dave_unit));
    sync_table_rows_to(&mut core, &mut writers[0].1, "todos");
    let (w, w_unit) = offline_edit(&mut writers[0].1, target, 200, &[("body", "w")]);
    let SyncMessage::CommitUnit { versions, .. } = &w_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(versions[0].base().pending, Some(p), "p is still pending at carol");
    assert_eq!(versions[0].base().seq, Some(dave_seq));
    assert_accepted(&core_fate(&mut core, w_unit));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("p", "w")
    );
    assert!(!flagged(&mut core, target, dave), "dave saw p");
    let w_conflict = conflict(&mut core, target, w);
    assert!(p_seq < dave_seq);
    assert_eq!(w_conflict.resolved_base, Some(dave_seq));
    assert_eq!(w_conflict.previous, Some(dave_seq));
    assert!(!w_conflict.maybe_conflicting);
    assert!(w_conflict.overlapping.is_empty());
}

/// INV-HIST-21: a chained write's seen seq is the settled image its chain
/// rests on, never its predecessor's later accepted seq. carol, offline over
/// the seed, retitles (p1) and then rewrites the body (w); dave's body edit
/// reaches Core first and never reaches carol. Core applies all three by
/// arrival, so w overwrites dave's body: w is maybe conflicting and names
/// dave's write, although the record right before it is carol's own p1.
///
/// ```text
/// carol ═offline═ p1 title="a" (base: seed)
/// carol ═offline═ w  body="w"  (base: seed, pending p1)
/// dave  ──body="dave"──► core                    body=dave
/// carol ──p1──► core   title=a, flagged, overlaps nothing
/// carol ──w ──► core   body=w,  flagged: dave
/// ```
#[test]
fn chained_write_over_a_foreign_edit_its_writer_never_saw_is_flagged() {
    let target = row(0x99);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (p1, p1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "a")]);
    let (w, w_unit) = offline_edit(&mut writers[0].1, target, 101, &[("body", "w")]);
    let (dave, dave_unit) = offline_edit(&mut writers[1].1, target, 50, &[("body", "dave")]);
    let dave_seq = accepted_seq(&core_fate(&mut core, dave_unit));
    let seed_seq = conflict(&mut core, target, dave).previous;
    let SyncMessage::CommitUnit { versions, .. } = &w_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(versions[0].base().pending, Some(p1));
    assert_eq!(versions[0].base().seq, seed_seq, "carol never saw dave's write");
    let p1_seq = accepted_seq(&core_fate(&mut core, p1_unit));
    assert_accepted(&core_fate(&mut core, w_unit));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("a", "w"),
        "w's body overwrites dave's by arrival"
    );

    assert!(!flagged(&mut core, target, dave));
    let p1_conflict = conflict(&mut core, target, p1);
    assert!(p1_conflict.maybe_conflicting);
    assert!(p1_conflict.overlapping.is_empty(), "dave did not touch the title");
    let w_conflict = conflict(&mut core, target, w);
    assert!(w_conflict.maybe_conflicting);
    assert_eq!(w_conflict.overlapping, vec![dave]);
    assert_eq!(w_conflict.resolved_base, seed_seq);
    assert_eq!(w_conflict.previous, Some(p1_seq));
    assert!(seed_seq.is_some_and(|seed| seed < dave_seq && dave_seq < p1_seq));
}

/// INV-HIST-21: every write of a long offline chain made over the seed is
/// maybe conflicting once another writer's write lands before the chain's
/// first, and each names every foreign write since the seed that it
/// overlaps; carol's own chain writes are never named. erin edits over the
/// latest image between the chain's second and third writes, so only the
/// chain writes after her name her.
///
/// ```text
/// dave  ──title="dave"──► core                         over the seed
/// carol ═offline═ c0 title; c1 body ──► core            (base: seed)
/// erin  ──body="erin"──► core                          over the latest image
/// carol ═offline═ c2 title; c3 title, body; c4 body ──► core
///       = {c3, c4}; c0: dave  c1: -  c2: dave  c3: dave, erin  c4: erin
/// ```
#[test]
fn every_write_of_an_interrupted_chain_names_the_foreign_writes_it_overlaps() {
    let target = row(0x9a);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 3);
    let edits: [&[(&str, &str)]; 5] = [
        &[("title", "c0")],
        &[("body", "c1")],
        &[("title", "c2")],
        &[("title", "c3"), ("body", "c3")],
        &[("body", "c4")],
    ];
    let mut chain = Vec::new();
    for (index, cells) in (0_u64..).zip(edits) {
        chain.push(offline_edit(&mut writers[0].1, target, 100 + index, cells));
    }
    let dave = accept_edit(&mut core, &mut writers[1].1, target, 50, &[("title", "dave")]);
    let seed_seq = conflict(&mut core, target, dave).previous;
    let mut carol = Vec::new();
    let mut units = chain.into_iter();
    for (tx, unit) in units.by_ref().take(2) {
        assert_accepted(&core_fate(&mut core, unit));
        carol.push(tx);
    }
    sync_table_rows_to(&mut core, &mut writers[2].1, "todos");
    let erin = accept_edit(&mut core, &mut writers[2].1, target, 200, &[("body", "erin")]);
    for (tx, unit) in units {
        assert_accepted(&core_fate(&mut core, unit));
        carol.push(tx);
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("c3", "c4")
    );

    let conflicts = row_conflicts(&mut core, target);
    assert!(!conflicts[&dave].maybe_conflicting);
    assert!(!conflicts[&erin].maybe_conflicting, "erin saw the latest image");
    let expected = [
        vec![dave],
        Vec::new(),
        vec![dave],
        vec![dave, erin],
        vec![erin],
    ];
    for (tx, overlapping) in carol.iter().zip(expected) {
        let chained = &conflicts[tx];
        assert!(chained.maybe_conflicting);
        assert_eq!(chained.resolved_base, seed_seq, "carol saw only the seed");
        assert_eq!(chained.overlapping, overlapping);
    }
}

/// Core accepts `writer`'s edit of `target` in `todos`.
fn accept_edit(
    core: &mut NodeState,
    writer: &mut NodeState,
    target: RowUuid,
    at_ms: u64,
    cells: &[(&str, &str)],
) -> TxId {
    let (tx, unit) = offline_edit(writer, target, at_ms, cells);
    assert_accepted(&core_fate(core, unit));
    tx
}

/// INV-HIST-21: the conflict analysis walks the row's history from a write's
/// resolved base to the write and names the intervening writes whose
/// authored columns overlap the write's, in seq order.
///
/// ```text
/// dave  ──title="dave"──► core          over the seed
/// erin  ──body="erin"──► core           over the seed: flagged, overlaps nothing
/// carol ──title="carol"──► core         over the seed: flagged, overlaps dave
/// frank ──title, body="frank"──► core   over the seed: flagged, overlaps dave, erin, carol
/// dave  ──body="dave"──► core           over the latest image: not flagged
/// ```
#[test]
fn conflict_walk_names_the_intervening_writes_that_overlap() {
    let target = row(0x98);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 4);
    let dave = accept_edit(&mut core, &mut writers[1].1, target, 50, &[("title", "dave")]);
    let erin = accept_edit(&mut core, &mut writers[2].1, target, 51, &[("body", "erin")]);
    let carol = accept_edit(&mut core, &mut writers[0].1, target, 52, &[("title", "carol")]);
    let frank = accept_edit(&mut core, &mut writers[3].1, target, 53, &[("title", "frank"), ("body", "frank")]);
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("frank", "frank")
    );
    let conflicts = row_conflicts(&mut core, target);
    assert!(!conflicts[&dave].maybe_conflicting);
    assert!(conflicts[&erin].maybe_conflicting);
    assert!(conflicts[&erin].overlapping.is_empty());
    assert!(conflicts[&carol].maybe_conflicting);
    assert_eq!(conflicts[&carol].overlapping, vec![dave]);
    assert!(conflicts[&frank].maybe_conflicting);
    assert_eq!(conflicts[&frank].overlapping, vec![dave, erin, carol]);

    // dave receives Core's image and edits over it.
    sync_table_rows_to(&mut core, &mut writers[1].1, "todos");
    let latest = accept_edit(&mut core, &mut writers[1].1, target, 1_000, &[("body", "dave")]);
    let latest_conflict = conflict(&mut core, target, latest);
    assert!(!latest_conflict.maybe_conflicting);
    assert!(latest_conflict.overlapping.is_empty());
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

/// Enum cells apply by arrival under a branched enum registry: an uploaded
/// write's authored tags are stored as the lineage's physical tags, and a
/// cell carried from the current image is re-tagged to the write's own.
///
/// Core publishes `base -> A (+ a) -> A2 (+ a2)` and then `base -> B (+ b)`,
/// so B's `b` (authored tag 1) is physical tag 3. carol and dave write under
/// B; carol wrote the seed. carol's first edit sets `b` over the seed after
/// dave already did, so it is maybe conflicting with dave's write; her
/// chained edit then sets `base` over the same seed, and is too.
///
/// ```text
/// carol ──status=base──► core                  seed
/// dave  ──status=b──► core                     status=b (physical 3)
/// carol ═offline═ e1 status=b ──► core          flagged: dave
/// carol ═offline═ e2 status=base ──► core       status=base, flagged: dave
/// ```
#[test]
fn enum_cells_apply_by_arrival_under_a_branched_registry() {
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
    let (dave_tx, dave_unit) = edit(&mut dave, 50, 1);

    assert_accepted(&core_fate(&mut core, dave_unit));
    assert_accepted(&core_fate(&mut core, e1_unit));
    assert_accepted(&core_fate(&mut core, e2_unit));
    let e1_conflict = conflict_in(&mut core, "items", target, e1);
    assert!(e1_conflict.maybe_conflicting);
    assert_eq!(e1_conflict.overlapping, vec![dave_tx]);
    let e2_conflict = conflict_in(&mut core, "items", target, e2);
    assert!(e2_conflict.maybe_conflicting);
    assert_eq!(e2_conflict.overlapping, vec![dave_tx], "never e1, carol's own");
    let rows = core
        .current_rows_for_schema("items", b.id, DurabilityTier::Global)
        .resolve()
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect::<BTreeMap<_, _>>();
    assert_eq!(rows[&target], status(0));
}

/// Core's superseded-writes marker of `writer` for `target` in `todos`.
fn superseded_marker(core: &NodeState, target: RowUuid, writer: NodeUuid) -> Option<TxTime> {
    let todos = core
        .physical_table_id_for_schema(two_column_schema().version_id(), "todos")
        .unwrap();
    let key = [
        Value::U64(todos.0),
        Value::Bytes(BranchKey::default().canonical_bytes()),
        Value::Uuid(target.0),
        Value::Uuid(writer.0),
    ];
    let store = core
        .database
        .direct_record_store(crate::schema::SUPERSEDED_WRITES_STORE)
        .unwrap();
    crate::local_executor::block_on(store.get(&key))
        .unwrap()
        .map(|record| match record.get_idx(0).unwrap() {
            Value::U64(time) => TxTime(time),
            other => panic!("marker is not a time: {other:?}"),
        })
}

/// Run Core's late-write check for `unit`'s single version against the
/// row's current image, and the history records it read.
fn late_check_cost(
    core: &mut NodeState,
    unit: &SyncMessage,
    current_seq: GlobalTime,
    current_writer: TxId,
) -> (bool, groove::db::StorageReadMetrics) {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected a commit unit");
    };
    let version = &versions[0];
    let batch = core.database.open_batch();
    core.reset_storage_read_metrics();
    let refusal = core
        .late_write_refusal(
            &batch,
            version.schema_version(),
            version.table(),
            version.branch_key(),
            version.row_uuid(),
            version.base(),
            tx.tx_id,
            Some(current_seq),
            Some(current_writer),
        )
        .resolve()
        .unwrap();
    (refusal.is_some(), core.take_storage_read_metrics())
}

/// INV-HIST-23, cost: the late-write check reads the writes Core accepted
/// since the write's base, never the row's whole history. dave edits the
/// row 60 times; carol syncs, edits twice offline, and her second edit
/// arrives first; dave edits twice more. Checking carol's late first edit
/// reads only the three records after her base, as does an on-time write
/// over the same stale base; a write without a base reads no history record
/// at all.
///
/// ```text
/// dave  ──d1..d60──► core                 60 records
/// carol (synced at d60) ═offline═ c1, c2
/// carol ──c2──► core   accepted           marker: carol through c1
/// dave  ──d61, d62──► core
/// carol ──c1──► core   late: reads c2, d61, d62 only
/// ```
#[test]
fn late_write_check_reads_only_the_writes_since_the_base() {
    let target = row(0x9b);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    for index in 0..60 {
        accept_edit(
            &mut core,
            &mut writers[1].1,
            target,
            200 + index,
            &[("body", &format!("d{index}"))],
        );
    }
    sync_table_rows_to(&mut core, &mut writers[0].1, "todos");
    let carol = writers[0].1.node_uuid;
    let (c1, c1_unit) = offline_edit(&mut writers[0].1, target, 300, &[("title", "c1")]);
    let (c2, c2_unit) = offline_edit(&mut writers[0].1, target, 301, &[("title", "c2")]);
    assert_eq!(fate_for(&core.apply_sync_message_settled(c2_unit).unwrap(), c2), Some(&Fate::Accepted));
    assert_eq!(
        superseded_marker(&core, target, carol),
        Some(c1.time),
        "c2 passed over c1, which Core had not decided"
    );
    accept_edit(&mut core, &mut writers[1].1, target, 302, &[("body", "d61")]);
    let d62 = accept_edit(&mut core, &mut writers[1].1, target, 303, &[("body", "d62")]);
    let current = core.transaction_state_settled(d62).unwrap().1.unwrap();
    assert!(row_conflicts(&mut core, target).len() > 60);

    let (late, metrics) = late_check_cost(&mut core, &c1_unit, current, d62);
    assert!(late, "c1 arrived after carol's c2");
    assert_eq!(metrics.history_rows.ranges, 1, "{metrics:?}");
    assert_eq!(metrics.history_rows.reads, 3, "c2, d61 and d62: {metrics:?}");

    // A write of dave's over the same stale base (d60) is not late, and
    // reads the same three records.
    let (_, on_time) = offline_edit(&mut writers[1].1, target, 304, &[("title", "d")]);
    let c1_base = match &c1_unit {
        SyncMessage::CommitUnit { versions, .. } => versions[0].base(),
        _ => unreachable!(),
    };
    let stale = with_base(
        on_time,
        crate::protocol::RowBase {
            seq: c1_base.seq,
            pending: None,
        },
    );
    let (late, metrics) = late_check_cost(&mut core, &stale, current, c2);
    assert!(!late);
    assert_eq!(metrics.history_rows.reads, 3, "{metrics:?}");

    // A write without a base seq reads the writer's marker, not history.
    let (_frank_dir, mut frank) = open_node_with_schema(node(0x31), two_column_schema());
    let (_, blind) = offline_edit(&mut frank, target, 305, &[("title", "frank")]);
    let (late, metrics) = late_check_cost(&mut core, &blind, current, d62);
    assert!(!late);
    assert_eq!(metrics.history_rows.reads, 0, "{metrics:?}");
    assert_eq!(metrics.history_rows.ranges, 0, "{metrics:?}");

    let answer = core.apply_sync_message_settled(c1_unit).unwrap();
    assert!(refused_as_late(&answer, c1), "{answer:?}");
}

/// INV-HIST-23: a write without a base (frank never loaded the row) that
/// reaches Core after its chained successor, with another writer's write
/// in between, is refused as late through the marker Core recorded when it
/// applied the successor over it.
///
/// ```text
/// frank (never loaded the row) ═offline═ f1 title="f1", f2 body="f2" (chained on f1)
/// frank ──f2──► core   accepted   marker: frank through f1
/// dave  ──d───► core   accepted   (the row's current image)
/// frank ──f1──► core   rejected (late)
/// ```
#[test]
fn late_write_without_a_base_is_refused_through_its_writers_marker() {
    let target = row(0x9c);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (_frank_dir, mut frank) = open_node_with_schema(node(0x32), two_column_schema());
    let (f1, f1_unit) = offline_edit(&mut frank, target, 100, &[("title", "f1")]);
    let (f2, f2_unit) = offline_edit(&mut frank, target, 101, &[("body", "f2")]);
    let SyncMessage::CommitUnit { versions, .. } = &f1_unit else {
        unreachable!()
    };
    assert!(versions[0].base().is_empty(), "f1 has no base");
    let SyncMessage::CommitUnit { versions, .. } = &f2_unit else {
        unreachable!()
    };
    assert_eq!(versions[0].base().pending, Some(f1));
    let answer = core.apply_sync_message_settled(f2_unit).unwrap();
    assert_eq!(fate_for(&answer, f2), Some(&Fate::Accepted));
    assert_eq!(superseded_marker(&core, target, f1.node), Some(f1.time));
    accept_edit(&mut core, &mut writers[0].1, target, 102, &[("title", "dave")]);
    let late = core.apply_sync_message_settled(f1_unit).unwrap();
    assert!(refused_as_late(&late, f1), "{late:?}");
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target].get("title"),
        Some(&v("dave"))
    );
}

/// The ordinary chain, each write over a predecessor Core accepted, records
/// no marker: only a write that passes over a predecessor Core never
/// decided does.
#[test]
fn chain_over_accepted_predecessors_records_no_marker() {
    let target = row(0x9d);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let carol = writers[0].1.node_uuid;
    let (_, e1_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c1")]);
    let (_, e2_unit) = offline_edit(&mut writers[0].1, target, 101, &[("body", "c2")]);
    assert_accepted(&core_fate(&mut core, e1_unit));
    // Another writer's write makes carol's chain stale, so the check looks
    // the predecessor up rather than reading it off the row's image.
    accept_edit(&mut core, &mut writers[1].1, target, 102, &[("title", "dave")]);
    assert_accepted(&core_fate(&mut core, e2_unit));
    assert_eq!(superseded_marker(&core, target, carol), None);
}

/// `todos` renamed to `tasks` (and `title` to `name`) by a later schema.
fn renamed_two_column_tasks_schema() -> JazzSchema {
    build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("tasks")
                    .column("name", PublicColumnType::Text)
                    .column("body", PublicColumnType::Text),
            )
            .allow_all(),
    )
}

/// INV-HIST-23 across a table rename: the marker names the row's table by
/// its physical id, which the rename keeps, so a write made under the old
/// schema is refused as late through the marker its successor recorded
/// under the new one.
///
/// ```text
/// frank (v1, todos) ═offline═ f1 title="f1"
/// frank (v2, tasks) ═offline═ f2 name="f2" (chained on f1)
/// frank ──f2──► core   accepted   marker: frank through f1
/// dave  ──d───► core   accepted   (the row's current image)
/// frank ──f1──► core   rejected (late)
/// ```
#[test]
fn superseded_marker_survives_a_table_rename() {
    let target = row(0x9e);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let base = two_column_schema();
    let renamed = SchemaVersion::new(renamed_two_column_tasks_schema());
    publish_schema_lineage(
        &mut core,
        renamed.clone(),
        MigrationLens::new(
            base.version_id(),
            renamed.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                ],
            }],
        )
        .expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    assert_eq!(
        core.physical_table_id_for_schema(renamed.id, "tasks").unwrap(),
        core.physical_table_id_for_schema(base.version_id(), "todos").unwrap(),
        "a rename keeps the physical table"
    );

    let frank = node(0x34);
    let (_v1_dir, mut frank_v1) = open_node_with_schema(frank, base);
    let (f1, f1_unit) = offline_edit(&mut frank_v1, target, 100, &[("title", "f1")]);
    let (_v2_dir, mut frank_v2) = open_node_with_schema(frank, renamed_two_column_tasks_schema());
    let (f2, f2_unit) = frank_v2
        .commit_mergeable_unit_settled(
            MergeableCommit::new("tasks", target, 101)
                .cells(BTreeMap::from([("name".to_owned(), v("f2"))])),
        )
        .unwrap();
    assert!(f1 < f2);
    let f2_unit = with_base(
        f2_unit,
        crate::protocol::RowBase {
            seq: None,
            pending: Some(f1),
        },
    );

    let answer = core.apply_sync_message_settled(f2_unit).unwrap();
    assert_eq!(fate_for(&answer, f2), Some(&Fate::Accepted), "{answer:?}");
    assert_eq!(superseded_marker(&core, target, f1.node), Some(f1.time));
    accept_edit(&mut core, &mut writers[0].1, target, 102, &[("title", "dave")]);
    let late = core.apply_sync_message_settled(f1_unit).unwrap();
    assert!(refused_as_late(&late, f1), "{late:?}");
}

/// `unit` with its base seq far above any seq Core assigned: Core refuses
/// it (`INV-HIST-20`) before it could pass anything over.
fn with_unnameable_base(unit: SyncMessage, pending: Option<TxId>) -> SyncMessage {
    with_base(
        unit,
        crate::protocol::RowBase {
            seq: Some(GlobalTime(1_000_000_000)),
            pending,
        },
    )
}

/// A write chained on a predecessor Core already refused records no
/// marker: the redelivery of the refused write is deduplicated, and its
/// writer's writes before it were decided before it.
#[test]
fn chain_over_a_refused_predecessor_records_no_marker() {
    let target = row(0x9f);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let carol = writers[0].1.node_uuid;
    let (x, x_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "x")]);
    let (z, z_unit) = offline_edit(&mut writers[0].1, target, 101, &[("body", "z")]);
    let refused = core
        .apply_sync_message_settled(with_unnameable_base(x_unit, None))
        .unwrap();
    assert!(
        matches!(fate_for(&refused, x), Some(Fate::Rejected(_))),
        "{refused:?}"
    );
    let answer = core.apply_sync_message_settled(z_unit).unwrap();
    assert_eq!(fate_for(&answer, z), Some(&Fate::Accepted), "{answer:?}");
    assert_eq!(superseded_marker(&core, target, carol), None);
}

/// INV-HIST-23 through a refused link: when Core refuses a write whose own
/// predecessor it never saw, the refusal records the marker, so the write
/// chained on the refused one needs none and the never-seen predecessor is
/// still refused as late when it arrives.
///
/// ```text
/// frank (never loaded the row) ═offline═ y, x (chained on y), z (chained on x)
/// frank ──x──► core   rejected (base unnameable)   marker: frank through y
/// frank ──z──► core   accepted                     no new marker
/// dave  ──d──► core   accepted
/// frank ──y──► core   rejected (late)
/// ```
#[test]
fn refusal_of_a_chained_write_records_its_predecessors_marker() {
    let target = row(0xa0);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 1);
    let (_frank_dir, mut frank) = open_node_with_schema(node(0x35), two_column_schema());
    let (y, y_unit) = offline_edit(&mut frank, target, 100, &[("title", "y")]);
    let (x, x_unit) = offline_edit(&mut frank, target, 101, &[("body", "x")]);
    let (z, z_unit) = offline_edit(&mut frank, target, 102, &[("title", "z")]);
    let refused = core
        .apply_sync_message_settled(with_unnameable_base(x_unit, Some(y)))
        .unwrap();
    assert!(
        matches!(fate_for(&refused, x), Some(Fate::Rejected(_))),
        "{refused:?}"
    );
    assert_eq!(superseded_marker(&core, target, y.node), Some(y.time));
    let z_unit = with_base(
        z_unit,
        crate::protocol::RowBase {
            seq: None,
            pending: Some(x),
        },
    );
    let answer = core.apply_sync_message_settled(z_unit).unwrap();
    assert_eq!(fate_for(&answer, z), Some(&Fate::Accepted), "{answer:?}");
    assert_eq!(
        superseded_marker(&core, target, y.node),
        Some(y.time),
        "z passed over a refused predecessor and wrote nothing"
    );
    accept_edit(&mut core, &mut writers[0].1, target, 103, &[("title", "dave")]);
    let late = core.apply_sync_message_settled(y_unit).unwrap();
    assert!(refused_as_late(&late, y), "{late:?}");
}
