// The local delta (`ahead_current`) is derived, in-memory state (SPEC 4
// §4.7, INV-REF-6): these tests drop a node and reopen it, and compare what
// incremental maintenance built with what the open-time fold rebuilds.

/// Every overlay row of `table`, raw, as the non-durable class holds it.
fn ahead_rows<S>(node: &mut NodeState<S>, table: &str) -> Vec<(Vec<u8>, Vec<u8>)>
where
    S: OrderedKvStorage,
{
    let physical_table = node
        .physical_table_id_for_schema(node.catalogue.local_schema_version_id, table)
        .unwrap();
    node.database
        .primary_key_scan_raw(&physical_ahead_current_table_name(physical_table), &[])
        .unwrap()
        .into_iter()
        .map(|raw| raw.into_parts())
        .collect()
}

fn derived_ahead_rows_at(
    node: &mut NodeState,
    table: &str,
    tier: DurabilityTier,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    node.current_rows(table, tier)
        .unwrap()
        .into_iter()
        .map(current_row_pair)
        .collect()
}

/// What a restart must preserve: local and global reads of every table, the
/// overlay's rows byte for byte, and the in-memory overlay key map.
#[derive(Debug, PartialEq)]
struct LocalSnapshot {
    reads: BTreeMap<String, [BTreeMap<RowUuid, BTreeMap<String, Value>>; 2]>,
    overlay: BTreeMap<String, Vec<(Vec<u8>, Vec<u8>)>>,
    overlay_keys: BTreeMap<(PhysicalTableId, Vec<u8>), TxId>,
    resident_bytes: usize,
}

fn local_snapshot(node: &mut NodeState, tables: &[&str]) -> LocalSnapshot {
    let mut reads = BTreeMap::new();
    let mut overlay = BTreeMap::new();
    for table in tables {
        reads.insert(
            (*table).to_owned(),
            [
                derived_ahead_rows_at(node, table, DurabilityTier::Local),
                derived_ahead_rows_at(node, table, DurabilityTier::Global),
            ],
        );
        overlay.insert((*table).to_owned(), ahead_rows(node, table));
    }
    LocalSnapshot {
        reads,
        overlay,
        overlay_keys: node.ahead_current_keys.snapshot_for_test(),
        resident_bytes: node.database.non_durable_residency().bytes(),
    }
}

/// INV-REF-6: pending writes in several tables, an edit over a settled row
/// and a pending delete of a settled row survive a restart exactly. Nothing
/// of the overlay is on disk; the reopened node folds history@0 over origin.
#[test]
fn restart_rebuilds_the_local_delta_across_tables_with_a_pending_delete() {
    let schema = todos_notes_schema();
    let tables = ["todos", "notes"];
    let (writer_dir, mut writer) = open_node_with_schema(node(0x31), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x39), schema.clone());

    // Two rows settle at origin first.
    for (row_uuid, title, at) in [(row(1), "settled one", 10), (row(2), "settled two", 11)] {
        let (_tx, unit) = writer
            .commit_mergeable_unit_settled(
                MergeableCommit::new("todos", row_uuid, at).cells(title_cells(title)),
            )
            .unwrap();
        let fate = core_fate(&mut core, unit);
        writer.apply_sync_message_settled(fate).unwrap();
    }
    assert_eq!(ahead_current_row_count(&mut writer, "todos"), 0);
    assert_eq!(writer.database.non_durable_residency().bytes(), 0);

    // Pending: an edit over settled row 1, a delete of settled row 2, a
    // fresh todo edited twice, and a note.
    writer
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(1), 20).cells(title_cells("edited one")),
        )
        .unwrap();
    writer
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(2), 21).deletion(DeletionEvent::Deleted),
        )
        .unwrap();
    writer
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(3), 22).cells(title_cells("fresh")),
        )
        .unwrap();
    writer
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(3), 23).cells(title_cells("fresh, edited")),
        )
        .unwrap();
    writer
        .commit_mergeable_settled(
            MergeableCommit::new("notes", row(4), 24)
                .cells(BTreeMap::from([("body".to_owned(), v("a note"))])),
        )
        .unwrap();

    let before = local_snapshot(&mut writer, &tables);
    assert_eq!(before.overlay["todos"].len(), 3);
    assert_eq!(before.overlay["notes"].len(), 1);
    assert!(before.resident_bytes > 0);
    let local_todos = &before.reads["todos"][0];
    assert_eq!(local_todos.len(), 2, "the pending delete hides row 2 locally");
    assert_eq!(local_todos[&row(1)], title_cells("edited one"));
    assert_eq!(local_todos[&row(3)], title_cells("fresh, edited"));
    assert_eq!(before.reads["todos"][1].len(), 2, "origin still has both rows");

    // Nothing of the overlay reached the backend.
    crate::local_executor::block_on(writer.database.close()).unwrap();
    drop(writer);
    let cfs = schema.column_families();
    assert!(
        !cfs.iter().any(|family| family.contains("ahead")),
        "no backend family holds the overlay: {cfs:?}"
    );

    let mut reopened = reopen_node_at(&writer_dir, node(0x31), schema);
    assert_eq!(local_snapshot(&mut reopened, &tables), before);
}

/// The non-durable class reports the overlay's bytes: they grow with pending
/// writes and return to zero once every pending write settles.
#[test]
fn resident_overlay_bytes_follow_pending_writes_and_return_to_zero_on_settle() {
    let schema = todos_notes_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x32), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x3a), schema);
    let process_before = groove::storage::process_non_durable_resident_bytes();
    assert_eq!(writer.database.non_durable_residency().bytes(), 0);

    let mut units = Vec::new();
    let mut last = 0;
    for index in 0..4u8 {
        let table = if index % 2 == 0 { "todos" } else { "notes" };
        let cells = if table == "todos" {
            title_cells(format!("pending {index}"))
        } else {
            BTreeMap::from([("body".to_owned(), v(format!("pending {index}")))])
        };
        let (_tx, unit) = writer
            .commit_mergeable_unit_settled(
                MergeableCommit::new(table, row(0x10 + index), 30 + u64::from(index)).cells(cells),
            )
            .unwrap();
        units.push(unit);
        let residency = writer.database.non_durable_residency();
        assert!(residency.bytes() > last, "each pending row adds bytes");
        last = residency.bytes();
        // One partition per (table, branch key): the main branch of each
        // table written so far.
        assert_eq!(
            residency.partitions().count(),
            usize::from(index.min(1)) + 1
        );
    }
    assert!(groove::storage::process_non_durable_resident_bytes() >= process_before + last);

    for unit in units {
        let fate = core_fate(&mut core, unit);
        writer.apply_sync_message_settled(fate).unwrap();
    }
    let residency = writer.database.non_durable_residency();
    assert_eq!(residency.bytes(), 0);
    assert_eq!(residency.partitions().count(), 0);
}

/// An exclusive transaction learned as an incomplete fragment is
/// history-only: neither incremental ingest nor the open-time fold puts it
/// in the overlay. Once its last write arrives it is.
#[test]
fn incomplete_exclusive_fragments_stay_out_of_the_rebuilt_overlay() {
    let schema = schema();
    let (dir, mut reader) = open_node_with_schema(node(0x33), schema.clone());
    let id = TxId::new(TxTime::from(100), node(0x82));
    let tx = |count| Transaction {
        kind: TxKind::Exclusive,
        ..settlement_fragment_tx(id, count)
    };
    let a = version_record(row(1), Vec::new(), title_cells("first"), None);
    let b = version_record(row(2), Vec::new(), title_cells("second"), None);
    reader
        .ingest_transaction_fragment_without_current_indexes(
            tx(2),
            vec![a.clone()],
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
    drop(reader);
    let mut reader = reopen_node_at(&dir, node(0x33), schema.clone());
    assert_eq!(ahead_current_row_count(&mut reader, "todos"), 0);
    assert!(
        derived_ahead_rows_at(&mut reader, "todos", DurabilityTier::Local).is_empty()
    );

    reader
        .ingest_known_transaction(
            tx(2),
            vec![a, b],
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut reader, "todos"), 2);
    let before = local_snapshot(&mut reader, &["todos"]);
    drop(reader);
    let mut reader = reopen_node_at(&dir, node(0x33), schema);
    assert_eq!(local_snapshot(&mut reader, &["todos"]), before);
}

/// INV-REF-6: across random local writes and deletes, settlements, rejections
/// and rebases under another writer's accepted writes, the overlay that
/// incremental maintenance keeps equals the one a reopened node folds from
/// history@0 over origin, byte for byte, after every step.
#[test]
fn incremental_overlay_equals_the_open_time_fold_under_random_operations() {
    for seed in [3_u64, 17, 41] {
        let schema = counter_schema();
        let tables = ["counters"];
        let rows = [row(0x51), row(0x52), row(0x53)];
        let (writer_dir, mut writer) = open_node_with_schema(node(0x34), schema.clone());
        let (_other_dir, mut other) = open_node_with_schema(node(0x35), schema.clone());
        let (_core_dir, mut core) = open_node_with_schema(node(0x3b), schema.clone());
        let mut rng = Lcg::new(seed);
        let mut queued = std::collections::VecDeque::<SyncMessage>::new();
        let mut now = 100_u64;
        for step in 0..28 {
            now += 1;
            let target = rows[rng.choose(rows.len())];
            match rng.choose(6) {
                0 | 1 => {
                    let commit = MergeableCommit::new("counters", target, now).cells(
                        counter_cells(rng.choose(50) as i32, &format!("w{seed}-{step}")),
                    );
                    let (_tx, unit) = writer.commit_mergeable_unit_settled(commit).unwrap();
                    queued.push_back(unit);
                }
                2 => {
                    let deletion = if rng.chance(1, 3) {
                        DeletionEvent::Restored
                    } else {
                        DeletionEvent::Deleted
                    };
                    let (_tx, unit) = writer
                        .commit_mergeable_unit_settled(
                            MergeableCommit::new("counters", target, now).deletion(deletion),
                        )
                        .unwrap();
                    queued.push_back(unit);
                }
                3 => {
                    // Settle the oldest pending write: Core decides units in
                    // arrival order.
                    if let Some(unit) = queued.pop_front() {
                        for message in core.apply_sync_message_settled(unit).unwrap() {
                            if matches!(message, SyncMessage::FateUpdate { .. }) {
                                writer.apply_sync_message_settled(message).unwrap();
                            }
                        }
                    }
                }
                4 => {
                    // Reject the newest pending write; the others stay.
                    if let Some(SyncMessage::CommitUnit { tx, .. }) = queued.pop_back() {
                        writer
                            .apply_sync_message_settled(SyncMessage::FateUpdate {
                                tx_id: tx.tx_id,
                                fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
                                global_time: None,
                                durability: None,
                            })
                            .unwrap();
                    }
                }
                _ => {
                    // Another writer's accepted write reaches origin, and the
                    // writer's pending patches on that row rebase over it.
                    let (_tx, unit) = other
                        .commit_mergeable_unit_settled(
                            MergeableCommit::new("counters", target, now)
                                .cells(counter_cells(rng.choose(50) as i32, "other")),
                        )
                        .unwrap();
                    let fate = core_fate(&mut core, unit);
                    other.apply_sync_message_settled(fate).unwrap();
                    sync_table_rows_to(&mut core, &mut writer, "counters");
                }
            }
            let before = local_snapshot(&mut writer, &tables);
            drop(writer);
            writer = reopen_node_at(&writer_dir, node(0x34), schema.clone());
            assert_eq!(
                local_snapshot(&mut writer, &tables),
                before,
                "seed {seed}, step {step}: the open-time fold must equal the maintained overlay"
            );
        }
    }
}
