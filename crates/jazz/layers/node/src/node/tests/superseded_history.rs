// History holds only superseded images; global current holds the latest
// (SPEC 2 §2.7.1). An accepted write first moves the row's previous current
// image into history at that image's own seq, then overwrites current. These
// tests pin that layout and the reads that depend on it: as-of reads, the
// late-write check, the conflict walk over bases, and the images a peer is
// sent for each transaction.
//
// Most assertions are public reads. The layout assertions read Core's
// history table directly: the layout itself is what these tests pin, and no
// public read distinguishes "held once" from "held twice".

/// Core's stored history records of `target` in `todos`, as
/// `(seq, transaction, cells)` in key order.
fn history_records(
    core: &mut NodeState,
    target: RowUuid,
) -> Vec<(GlobalTime, TxId, BTreeMap<String, Value>, crate::protocol::RowBase)> {
    let table_id = core
        .physical_table_id_for_schema(core.catalogue.local_schema_version_id, "todos")
        .unwrap();
    let history_table = physical_history_table_name(table_id);
    let records = core
        .database
        .primary_key_scan_raw(
            &history_table,
            &[
                Value::Bytes(BranchKey::default().canonical_bytes()),
                Value::Uuid(target.0),
            ],
        )
        .resolve()
        .unwrap()
        .into_iter()
        .map(|raw| raw.owned_record())
        .collect::<Vec<_>>();
    let table = core.table("todos").unwrap().clone();
    records
        .into_iter()
        .map(|record| {
            let version = core
                .decode_history_owned_record("todos", &history_table, record)
                .unwrap();
            let cells = ["title", "body"]
                .into_iter()
                .filter_map(|column| {
                    version
                        .cell(&table, column)
                        .unwrap()
                        .map(|value| (column.to_owned(), value))
                })
                .collect();
            (
                version.seq().unwrap(),
                core.version_tx_id(&version).unwrap(),
                cells,
                version.base().unwrap(),
            )
        })
        .collect()
}

/// Core's global-current image of `target`: its seq, transaction and base.
fn current_image(
    core: &mut NodeState,
    target: RowUuid,
) -> (GlobalTime, TxId, crate::protocol::RowBase) {
    let version = core
        .query_global_winner_in_branch("todos", &BranchKey::default(), target)
        .resolve()
        .unwrap()
        .expect("the row has a current image");
    (
        version.seq().unwrap(),
        core.version_tx_id(&version).unwrap(),
        version.base().unwrap(),
    )
}

/// Rows of `todos` at seq `position`, through the public as-of read.
fn todos_at(core: &mut NodeState, position: GlobalTime) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let shape = Query::from("todos").validate(&core.catalogue.schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.query_rows_at(&shape, &binding, position)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect()
}

/// SPEC 2 §2.7.1: after an insert and an update, Core's history holds
/// exactly one record of the row, the insert's image at the insert's seq,
/// and global current holds the update's image. An as-of read at each seq
/// returns that seq's image: the insert seq from history, the update seq
/// from current.
///
/// ```text
/// alice ──insert {a, a}──► core   history: —        current: insert@s1
/// alice ──update {b}─────► core   history: insert@s1 current: update@s2
/// ```
#[test]
fn update_moves_the_previous_image_into_history_and_current_holds_the_latest() {
    let schema = two_column_schema();
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0xa0);

    let (insert, insert_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 10).cells(todo_cells("a", "a")),
        )
        .unwrap();
    let insert_fate = core_fate(&mut core, insert_unit);
    let s1 = accepted_seq(&insert_fate);
    alice.apply_sync_message_settled(insert_fate).unwrap();
    assert!(
        history_records(&mut core, target).is_empty(),
        "the row's only image lives in current"
    );
    assert_eq!(current_image(&mut core, target).1, insert);

    sync_table_rows_to(&mut core, &mut alice, "todos");
    let (update, update_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20).cells(title_cells("b")),
        )
        .unwrap();
    let s2 = accepted_seq(&core_fate(&mut core, update_unit));
    assert!(s1 < s2);

    assert_eq!(
        history_records(&mut core, target),
        vec![(
            s1,
            insert,
            todo_cells("a", "a"),
            crate::protocol::RowBase::default()
        )],
        "history holds exactly the superseded insert image"
    );
    let (current_seq, current_tx, current_base) = current_image(&mut core, target);
    assert_eq!((current_seq, current_tx), (s2, update));
    assert_eq!(
        current_base.seq,
        Some(s1),
        "current keeps the update's base, as its history record would"
    );
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("b", "a"))])
    );

    // As-of reads: before the insert, at the insert seq, at the update seq.
    assert!(todos_at(&mut core, GlobalTime(s1.0 - 1)).is_empty());
    assert_eq!(
        todos_at(&mut core, s1),
        BTreeMap::from([(target, todo_cells("a", "a"))])
    );
    assert_eq!(
        todos_at(&mut core, s2),
        BTreeMap::from([(target, todo_cells("b", "a"))])
    );
    let bounded = |core: &mut NodeState, position| {
        core.current_rows_at("todos", position)
            .resolve()
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>()
    };
    assert_eq!(
        bounded(&mut core, s1),
        BTreeMap::from([(target, todo_cells("a", "a"))])
    );
    assert_eq!(
        bounded(&mut core, s2),
        BTreeMap::from([(target, todo_cells("b", "a"))])
    );
}

/// SPEC 4 §4.6 ("Maybe conflicting"), INV-HIST-21: a write's base survives
/// both while its image is current and after a later write supersedes it.
/// Current carries the base, and the move into history keeps it, so the
/// conflict walk derives the same answer before and after.
///
/// ```text
/// alice ──title over seed──► core   s2
/// bob   ──title over seed──► core   s3: maybe conflicting, overlaps alice
/// dave  ──body over s3─────► core   s4: bob's image moves into history
/// ```
#[test]
fn superseded_write_keeps_its_base_and_its_conflict_analysis() {
    let target = row(0xa1);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 3);
    let (seed_seq, ..) = current_image(&mut core, target);
    let alice = accept_edit(&mut core, &mut writers[0].1, target, 100, &[("title", "alice")]);
    let (bob, bob_unit) = offline_edit(&mut writers[1].1, target, 101, &[("title", "bob")]);
    let bob_seq = accepted_seq(&core_fate(&mut core, bob_unit));

    let before = conflict(&mut core, target, bob);
    assert!(before.maybe_conflicting);
    assert_eq!(before.overlapping, vec![alice]);
    assert_eq!(current_image(&mut core, target).2.seq, Some(seed_seq));

    sync_table_rows_to(&mut core, &mut writers[2].1, "todos");
    let dave = accept_edit(&mut core, &mut writers[2].1, target, 102, &[("body", "dave")]);
    assert_eq!(current_image(&mut core, target).1, dave);

    let bob_record = history_records(&mut core, target)
        .into_iter()
        .find(|(_, tx, ..)| *tx == bob)
        .expect("bob's superseded image is in history");
    assert_eq!(bob_record.0, bob_seq);
    assert_eq!(
        bob_record.3,
        crate::protocol::RowBase {
            seq: Some(seed_seq),
            pending: None
        },
        "the move into history keeps bob's base"
    );
    assert_eq!(conflict(&mut core, target, bob), before);
    assert!(!flagged(&mut core, target, dave), "dave wrote over the latest image");
}

/// INV-HIST-23: the late-write check still finds the writer's later write
/// whether that write is the row's current image or has been superseded
/// into history by another writer's write.
///
/// ```text
/// carol ══offline══ e0 title, e1 body, e2 body (a chain over the seed)
/// carol ──e2──► core   e2 accepted: current
/// carol ──e1──► core   refused (late; e2 read from current)
/// dave  ──d───► core   d accepted: e2 moves into history
/// carol ──e0──► core   refused (late; e2 read from history)
/// ```
#[test]
fn late_write_is_refused_when_the_later_write_is_current_or_superseded() {
    let target = row(0xa2);
    let (mut writers, _core_dir, mut core) = core_with_seeded_todo(target, 2);
    let (seed_seq, ..) = current_image(&mut core, target);
    let (e0, e0_unit) = offline_edit(&mut writers[0].1, target, 100, &[("title", "c0")]);
    let (e1, e1_unit) = offline_edit(&mut writers[0].1, target, 101, &[("body", "c1")]);
    let (e2, e2_unit) = offline_edit(&mut writers[0].1, target, 102, &[("body", "c2")]);
    let SyncMessage::CommitUnit { versions, .. } = &e0_unit else {
        panic!("expected a commit unit");
    };
    assert_eq!(versions[0].base().seq, Some(seed_seq), "e0 rests on the seed");

    let accepted = core.apply_sync_message_settled(e2_unit).unwrap();
    assert_eq!(fate_for(&accepted, e2), Some(&Fate::Accepted));
    assert_eq!(current_image(&mut core, target).1, e2);
    let late = core.apply_sync_message_settled(e1_unit).unwrap();
    assert!(refused_as_late(&late, e1), "{late:?}");

    sync_table_rows_to(&mut core, &mut writers[1].1, "todos");
    let dave = accept_edit(&mut core, &mut writers[1].1, target, 103, &[("title", "dave")]);
    assert_eq!(current_image(&mut core, target).1, dave);
    assert!(
        history_records(&mut core, target)
            .iter()
            .any(|(_, tx, ..)| *tx == e2),
        "e2 was superseded into history"
    );
    let late = core.apply_sync_message_settled(e0_unit).unwrap();
    assert!(refused_as_late(&late, e0), "{late:?}");
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global)[&target],
        todo_cells("dave", "c2")
    );
}

/// SPEC 2 §2.7.1: catching up from a seq still yields every accepted image
/// after it. The images of each transaction (what fate replay, relay
/// forwarding and view bundles send for it) come from history for the
/// superseded writes and from current for the latest, and the row's history
/// lists them all.
///
/// ```text
/// alice ──insert──► core s1 ──update──► s2 ──update──► s3
/// peer catching up from s1 receives s2 (history) and s3 (current)
/// ```
#[test]
fn catch_up_from_a_seq_returns_every_image_after_it() {
    let schema = two_column_schema();
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let target = row(0xa3);
    let mut writes = Vec::new();
    for (at, cells) in [
        (10, todo_cells("one", "one")),
        (20, title_cells("two")),
        (30, title_cells("three")),
    ] {
        let tx = commit_mergeable_global(
            &mut alice,
            &mut core,
            MergeableCommit::new("todos", target, at).cells(cells),
        );
        sync_table_rows_to(&mut core, &mut alice, "todos");
        let seq = core
            .query_transaction(tx)
            .resolve()
            .unwrap()
            .unwrap()
            .global_time
            .unwrap();
        writes.push((tx, seq));
    }
    assert_eq!(history_records(&mut core, target).len(), 2);

    let table = core.table("todos").unwrap().clone();
    let expected = [("two", "one"), ("three", "one")];
    let since = writes[0].1;
    for ((tx, seq), (title, body)) in writes[1..].iter().zip(expected) {
        assert!(*seq > since);
        let [version] = core
            .query_versions_for_tx(*tx)
            .resolve()
            .unwrap()
            .try_into()
            .expect("one image per transaction");
        assert_eq!(version.seq().unwrap(), *seq);
        assert_eq!(version.cell(&table, "title").unwrap(), Some(v(title)));
        assert_eq!(version.cell(&table, "body").unwrap(), Some(v(body)));
    }
    let history = core.row_history("todos", target).resolve().unwrap();
    assert_eq!(
        history.iter().map(|entry| entry.tx_id()).collect::<Vec<_>>(),
        writes.iter().map(|(tx, _)| *tx).collect::<Vec<_>>()
    );

    // A peer that joins now receives the latest image.
    let (_bob_dir, mut bob) = open_node_with_schema(node(2), schema);
    sync_table_rows_to(&mut core, &mut bob, "todos");
    assert_eq!(
        rows_at(&mut bob, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("three", "one"))])
    );
}
