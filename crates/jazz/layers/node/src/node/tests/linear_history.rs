// Linear Core-sequenced history: the user-visible guarantees that replaced the
// version DAG's merge heads, merge versions, parked parents and rejection
// cascades. Core sequences every accepted write, merges it into the row's
// post-image one column at a time (per-column stamps for plain columns, ops
// for merge-strategy columns), and every other node stores Core's post-images
// under its own pending overlay (SPEC/4 §4.6).

/// Replicates the current rows of `table` from `core` to `receiver` through
/// one whole-table view update, as a subscribed client would receive them.
fn sync_table_rows_to(core: &mut NodeState, receiver: &mut NodeState, table: &str) {
    let mut peer = PeerState::new();
    let update = peer.current_rows_update(core, table).unwrap();
    register_whole_table_receiver(receiver, table);
    receiver.apply_sync_message_settled(update).unwrap();
}

fn rows_at(
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

fn todo_cells(title: &str, body: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), v(title)), ("body".to_owned(), v(body))])
}

fn counter_cells(count: i32, title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("count".to_owned(), Value::I32(count)),
        ("title".to_owned(), v(title)),
    ])
}

/// Core's single fate for a commit unit it just ingested.
fn core_fate(core: &mut NodeState, unit: SyncMessage) -> SyncMessage {
    core.apply_sync_message_settled(unit)
        .unwrap()
        .into_iter()
        .find(|message| matches!(message, SyncMessage::FateUpdate { .. }))
        .expect("Core decides every complete commit unit")
}

/// INV-HIST-8: two writers edit different columns of one row without seeing
/// each other. Both columns survive at Core, and on the shared column the
/// higher stamp wins even though Core sequenced it first.
#[test]
fn concurrent_writes_to_different_columns_both_survive_at_core() {
    let schema = two_column_schema();
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x10);

    commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("todos", target, 10).cells(todo_cells("base", "base")),
    );
    sync_table_rows_to(&mut core, &mut bob, "todos");

    // alice retitles at t=30; bob, whose clock is behind, rewrites the body
    // and also the title at t=20.
    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 30).cells(title_cells("alice")),
        )
        .unwrap();
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20).cells(todo_cells("bob", "bob")),
        )
        .unwrap();

    // Core sequences alice first and bob second.
    let alice_fate = core_fate(&mut core, alice_unit);
    let bob_fate = core_fate(&mut core, bob_unit);
    for fate in [&alice_fate, &bob_fate] {
        assert!(matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ));
    }

    // bob's body has no competing writer and survives; bob's later-sequenced
    // title carries the lower stamp and loses to alice's.
    let expected = BTreeMap::from([(target, todo_cells("alice", "bob"))]);
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        expected
    );
    assert_eq!(rows_at(&mut core, "todos", DurabilityTier::Local), expected);

    // Both writers converge to Core's post-image once they hear back.
    alice.apply_sync_message_settled(alice_fate).unwrap();
    bob.apply_sync_message_settled(bob_fate).unwrap();
    for writer in [&mut alice, &mut bob] {
        sync_table_rows_to(&mut core, writer, "todos");
        assert_eq!(rows_at(writer, "todos", DurabilityTier::Global), expected);
    }
}

/// INV-HIST-10: two writers increment a counter from the same base without
/// seeing each other. Core sums both deltas exactly instead of keeping one
/// writer's absolute value; the plain column still takes the higher stamp.
#[test]
fn concurrent_counter_increments_sum_exactly_at_core() {
    let schema = counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x20);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(counter_cells(10, "base")),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    sync_table_rows_to(&mut core, &mut bob, "counters");

    // Each writer sets an absolute value over the base it holds: +3 and +5.
    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20).cells(counter_cells(13, "alice")),
        )
        .unwrap();
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 21).cells(counter_cells(15, "bob")),
        )
        .unwrap();
    // Before hearing from Core each writer sees only its own increment.
    assert_eq!(
        rows_at(&mut alice, "counters", DurabilityTier::Local)[&target].get("count"),
        Some(&Value::I32(13))
    );

    core_fate(&mut core, alice_unit);
    core_fate(&mut core, bob_unit);

    let expected = BTreeMap::from([(target, counter_cells(18, "bob"))]);
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        expected
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    assert_eq!(
        rows_at(&mut alice, "counters", DurabilityTier::Global),
        expected
    );
}

/// INV-HIST-15: the same concurrent writes, delivered to Core in every order,
/// produce the same post-image. Plain columns are decided by stamps, not by
/// the seq Core happens to assign, and counter ops commute.
#[test]
fn core_post_image_is_independent_of_write_arrival_order() {
    let schema = counter_schema();
    let target = row(0x30);
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_base_tx, base_unit) = base_writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 10).cells(counter_cells(100, "base")),
        )
        .unwrap();

    // One seeding Core supplies the base row every writer edits over.
    let (_seed_dir, mut seed_core) = open_node_with_schema(node(9), schema.clone());
    core_fate(&mut seed_core, base_unit.clone());
    let mut writer_dirs = Vec::new();
    let mut units = Vec::new();
    for (index, (delta, title, at_ms)) in [(1, "w1", 40_u64), (20, "w2", 20), (300, "w3", 30)]
        .into_iter()
        .enumerate()
    {
        let (dir, mut writer) = open_node_with_schema(node(0x21 + index as u8), schema.clone());
        sync_table_rows_to(&mut seed_core, &mut writer, "counters");
        let (_tx, unit) = writer
            .commit_mergeable_unit_settled(
                MergeableCommit::new("counters", target, at_ms)
                    .cells(counter_cells(100 + delta, title)),
            )
            .unwrap();
        writer_dirs.push((dir, writer));
        units.push(unit);
    }

    // w1 carries the highest stamp (t=40) whatever seq it is given.
    let expected = BTreeMap::from([(target, counter_cells(421, "w1"))]);
    let orders: [[usize; 3]; 6] = [
        [0, 1, 2],
        [0, 2, 1],
        [1, 0, 2],
        [1, 2, 0],
        [2, 0, 1],
        [2, 1, 0],
    ];
    for order in orders {
        let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
        core_fate(&mut core, base_unit.clone());
        for index in order {
            core_fate(&mut core, units[index].clone());
        }
        assert_eq!(
            rows_at(&mut core, "counters", DurabilityTier::Global),
            expected,
            "arrival order {order:?}"
        );
    }
}

/// INV-EDGE-16: a counter write that reaches Core again (a reconnect resend
/// or a relay retry) is the same transaction and is counted once, also when
/// another writer's increment was sequenced in between.
#[test]
fn redelivered_counter_write_is_counted_once() {
    let schema = counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x40);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(counter_cells(10, "base")),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    sync_table_rows_to(&mut core, &mut bob, "counters");

    let (alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20).cells(counter_cells(15, "base")),
        )
        .unwrap();
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 21).cells(counter_cells(13, "base")),
        )
        .unwrap();

    let first = core_fate(&mut core, alice_unit.clone());
    let SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Accepted,
        global_time: Some(first_seq),
        ..
    } = first
    else {
        panic!("alice's increment is accepted: {first:?}");
    };
    assert_eq!(tx_id, alice_tx);
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global)[&target].get("count"),
        Some(&Value::I32(15))
    );

    // An immediate resend returns the known fate and changes nothing.
    let resent = core_fate(&mut core, alice_unit.clone());
    assert!(matches!(
        resent,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            global_time: Some(seq),
            ..
        } if seq == first_seq
    ));
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global)[&target].get("count"),
        Some(&Value::I32(15))
    );

    // bob's +3 lands, then alice's unit arrives a third time.
    core_fate(&mut core, bob_unit);
    core_fate(&mut core, alice_unit);
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global)[&target].get("count"),
        Some(&Value::I32(18))
    );
}

/// INV-READ-7: a receiver's current row is Core's post-image with the highest
/// seq it holds. Receiving Core's images of one row out of seq order leaves
/// the same row as receiving them in order.
#[test]
fn receiver_keeps_the_newest_seq_whatever_order_core_images_arrive() {
    let schema = two_column_schema();
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(2), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let target = row(0x50);

    commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("todos", target, 10).cells(todo_cells("first", "first")),
    );
    let older = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    commit_mergeable_global(
        &mut bob,
        &mut core,
        MergeableCommit::new("todos", target, 20).cells(todo_cells("second", "second")),
    );
    let newer = PeerState::new()
        .current_rows_update(&mut core, "todos")
        .unwrap();
    let expected = BTreeMap::from([(target, todo_cells("second", "second"))]);
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        expected
    );

    for (label, first, second) in [
        ("in order", older.clone(), newer.clone()),
        ("out of order", newer, older),
    ] {
        let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema.clone());
        register_whole_table_receiver(&mut reader, "todos");
        reader.apply_sync_message_settled(first).unwrap();
        reader.apply_sync_message_settled(second).unwrap();
        for tier in [DurabilityTier::Local, DurabilityTier::Global] {
            assert_eq!(
                rows_at(&mut reader, "todos", tier),
                expected,
                "{label} {tier:?}"
            );
        }
    }
}

/// INV-TX-6: a node that has applied a row stamps its later edits at least as
/// high as the row's newest stamp, so an edit made after observing a value
/// overrides it even when the editing node's clock is far behind. A writer
/// with the same slow clock that never observed the value still loses.
#[test]
fn write_after_observing_a_value_overrides_it_despite_a_slow_clock() {
    let schema = two_column_schema();
    let (_fast_dir, mut fast) = open_node_with_schema(node(1), schema.clone());
    let (_observer_dir, mut observer) = open_node_with_schema(node(2), schema.clone());
    let (_stale_dir, mut stale) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x60);

    commit_mergeable_global(
        &mut fast,
        &mut core,
        MergeableCommit::new("todos", target, 5).cells(todo_cells("base", "base")),
    );
    sync_table_rows_to(&mut core, &mut observer, "todos");
    sync_table_rows_to(&mut core, &mut stale, "todos");

    commit_mergeable_global(
        &mut fast,
        &mut core,
        MergeableCommit::new("todos", target, 1_000).cells(title_cells("fast clock")),
    );
    // Only `observer` receives the fast-clock title.
    sync_table_rows_to(&mut core, &mut observer, "todos");

    let (observer_tx, observer_unit) = observer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 10).cells(title_cells("observed then edited")),
        )
        .unwrap();
    assert!(
        observer_tx.time.physical_ms() >= 1_000,
        "the edit is stamped at least as high as the value it observed"
    );
    let (stale_tx, stale_unit) = stale
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20).cells(title_cells("never observed")),
        )
        .unwrap();
    assert!(stale_tx.time.physical_ms() < 1_000);

    let accepted = |fate: &SyncMessage| {
        matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        )
    };
    assert!(accepted(&core_fate(&mut core, observer_unit)));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("observed then edited", "base"))])
    );
    // The unobserving writer is sequenced last and still loses.
    // Its slow clock is not a reason to reject it.
    assert!(accepted(&core_fate(&mut core, stale_unit)));
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("observed then edited", "base"))])
    );
}

/// INV-TX-8: when Core rejects a transaction, its effects leave the writer's
/// local view at once, while the writer's other pending edits of the same row
/// stay visible and later settle on their own. Nothing cascades.
#[test]
fn rejected_write_leaves_the_local_view_and_other_pending_writes_stay() {
    let schema = two_column_schema();
    let (_client_dir, mut client) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x70);

    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", target, 10).cells(todo_cells("base", "base")),
    );

    let (kept, kept_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 20)
                .cells(BTreeMap::from([("body".to_owned(), v("kept"))])),
        )
        .unwrap();
    // The next edit is made with a clock beyond Core's skew tolerance.
    let (rejected, rejected_unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 41 + SKEW_TOLERANCE_MS)
                .cells(title_cells("rejected")),
        )
        .unwrap();
    assert_eq!(
        rows_at(&mut client, "todos", DurabilityTier::Local),
        BTreeMap::from([(target, todo_cells("rejected", "kept"))])
    );

    let kept_fate = core_fate(&mut core, kept_unit);
    let SyncMessage::CommitUnit { tx, versions } = rejected_unit else {
        panic!("expected a commit unit");
    };
    let [rejected_fate] = core
        .ingest_commit_unit_settled(tx, versions, 40)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        rejected_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::ClientClockTooFarAhead),
            ..
        }
    ));

    // The rejection arrives while the body edit is still pending.
    client.apply_sync_message_settled(rejected_fate).unwrap();
    assert!(matches!(
        client.transaction_state_settled(rejected).unwrap().0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    ));
    assert_eq!(
        client.transaction_state_settled(kept).unwrap().0,
        Fate::Pending
    );
    assert_eq!(
        rows_at(&mut client, "todos", DurabilityTier::Local),
        BTreeMap::from([(target, todo_cells("base", "kept"))])
    );
    assert_eq!(
        rows_at(&mut client, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("base", "base"))])
    );

    client.apply_sync_message_settled(kept_fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(kept).unwrap().0,
        Fate::Accepted
    );
    for tier in [DurabilityTier::Local, DurabilityTier::Global] {
        assert_eq!(
            rows_at(&mut client, "todos", tier),
            BTreeMap::from([(target, todo_cells("base", "kept"))]),
            "{tier:?}"
        );
    }
    assert_eq!(
        rows_at(&mut core, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, todo_cells("base", "kept"))])
    );
}

/// INV-TX-5: Core holds a commit unit authored under a schema it does not yet
/// know. While parked the unit has no fate, no seq and no rows, and a resend
/// parks it only once; when the schema arrives Core decides it exactly once.
#[test]
fn unit_with_unknown_schema_waits_and_is_decided_once_it_arrives() {
    let base = schema();
    let evolved = catalogue_evolved_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x31), evolved.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x39), base.clone());
    let target = row(0x80);
    let cells = BTreeMap::from([
        ("title".to_owned(), v("waiting")),
        ("body".to_owned(), v("for schema")),
    ]);
    let (tx_id, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 50).cells(cells.clone()),
        )
        .unwrap();
    let committed_before = core.clock.committed_global_time;

    for _ in 0..2 {
        assert!(
            core.apply_sync_message_settled(unit.clone())
                .unwrap()
                .is_empty()
        );
    }
    assert_eq!(core.sync_metrics().parked_catalogue_orphans, 1);
    assert!(core.query_transaction(tx_id).unwrap().is_none());
    assert_eq!(core.clock.committed_global_time, committed_before);
    for tier in [DurabilityTier::Local, DurabilityTier::Global] {
        assert!(rows_at(&mut core, "todos", tier).is_empty(), "{tier:?}");
    }

    let updates = publish_schema_lineage(
        &mut core,
        SchemaVersion::new(evolved.clone()),
        MigrationLens::new(
            base.version_id(),
            evolved.version_id(),
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        )
        .expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let fates = updates
        .iter()
        .filter(|message| {
            matches!(message, SyncMessage::FateUpdate { tx_id: decided, .. } if *decided == tx_id)
        })
        .collect::<Vec<_>>();
    assert!(
        matches!(
            fates.as_slice(),
            [SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                global_time: Some(_),
                ..
            }]
        ),
        "decided exactly once: {fates:?}"
    );
    assert_eq!(core.sync_metrics().parked_catalogue_orphans_resolved, 1);
    assert!(core.parking.parked_commit_units.is_empty());
    let shape = Query::from("todos").validate(&evolved).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    assert_eq!(
        core.query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(target, cells)])
    );
}

/// INV-TX-23: a node that relays a client's commit unit upstream is not the
/// fate authority. It keeps the unit pending: no fate, no seq, nothing at the
/// Global tier. Only Core's fate settles it there.
#[test]
fn relay_keeps_a_relayed_unit_pending_until_core_decides_it() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_relay_dir, mut relay) = open_node_with_uuid(node(5));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let target = row(0x90);

    let (tx_id, unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", target, 10).cells(title_cells("relayed")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
        panic!("expected a commit unit");
    };
    let relay_committed_before = relay.clock.committed_global_time;
    relay
        .ingest_relay_commit_unit(tx, versions)
        .resolve()
        .unwrap();

    let (fate, global_time, _durability) = relay.transaction_state_settled(tx_id).unwrap();
    assert_eq!(fate, Fate::Pending);
    assert_eq!(global_time, None);
    assert_eq!(relay.clock.committed_global_time, relay_committed_before);
    assert!(rows_at(&mut relay, "todos", DurabilityTier::Global).is_empty());

    let core_decision = core_fate(&mut core, unit);
    let SyncMessage::FateUpdate {
        fate: Fate::Accepted,
        global_time: Some(core_seq),
        ..
    } = core_decision
    else {
        panic!("Core accepts the unit: {core_decision:?}");
    };
    relay.apply_sync_message_settled(core_decision).unwrap();
    let (fate, global_time, durability) = relay.transaction_state_settled(tx_id).unwrap();
    assert_eq!(fate, Fate::Accepted);
    assert_eq!(global_time, Some(core_seq));
    assert_eq!(durability, DurabilityTier::Global);
    assert_eq!(
        rows_at(&mut relay, "todos", DurabilityTier::Global),
        BTreeMap::from([(target, title_cells("relayed"))])
    );
}

/// INV-SYNC-25: serving a view under known-state dedup, including the full
/// resend after a known-state miss, gives the reader the same rows as serving
/// it without dedup.
#[test]
fn deduped_view_and_its_miss_resend_match_the_undeduped_view() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    let serve = |core: &mut NodeState, declaration| {
        let mut peer = PeerState::relay();
        peer.set_subscription_policy_binding(
            subscription,
            (AuthorSubject::SYSTEM, BTreeMap::new()),
        );
        if declaration {
            peer.declare_known_state(
                subscription,
                Some(crate::protocol::KnownStateDeclaration::Fast {
                    completeness: crate::protocol::KnownStateCompleteness::FastCurrentMembership,
                    position: GlobalTime::new(20, 0).unwrap(),
                }),
            );
        }
        peer.rehydrate_query_for_subscription_with_opts(
            core,
            subscription,
            &shape,
            &binding,
            RegisterShapeOptions::default(),
        )
        .unwrap()
        .expect("a view update")
    };
    let commit = |writer: &mut NodeState, core: &mut NodeState, row_uuid, at_ms, title: &str| {
        let (_tx, unit) = writer
            .commit_mergeable_unit_settled(
                MergeableCommit::new("todos", row_uuid, at_ms).cells(title_cells(title)),
            )
            .unwrap();
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("expected a commit unit");
        };
        core.ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
            .unwrap();
    };
    let read = |reader: &mut NodeState| {
        reader
            .query_rows_for_client(
                &shape,
                &binding,
                DurabilityTier::Global,
                AuthorSubject::SYSTEM,
            )
            .resolve()
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>()
    };

    let (unchanged, updated, added) = (row(0xa1), row(0xa2), row(0xa3));
    commit(&mut writer, &mut core, unchanged, 10, "unchanged");
    commit(&mut writer, &mut core, updated, 20, "before");

    // `warm` holds everything settled up to the declared position.
    let (_warm_dir, mut warm) = open_node_with_uuid(node(3));
    register_shape_binding(&mut warm, &shape, &binding);
    let reset = serve(&mut core, false);
    warm.apply_sync_message_settled(reset).unwrap();

    commit(&mut writer, &mut core, updated, 30, "after");
    commit(&mut writer, &mut core, added, 40, "added");

    let (_plain_dir, mut plain) = open_node_with_uuid(node(4));
    register_shape_binding(&mut plain, &shape, &binding);
    let undeduped = serve(&mut core, false);
    let undeduped_bodies = version_bundles_for_update(&undeduped).len();
    plain.apply_sync_message_settled(undeduped).unwrap();
    let expected = read(&mut plain);
    assert_eq!(
        expected,
        BTreeMap::from([
            (unchanged, title_cells("unchanged")),
            (updated, title_cells("after")),
            (added, title_cells("added")),
        ])
    );

    // The warm reader's deduped stream omits a body it holds and applies.
    let deduped = serve(&mut core, true);
    assert!(
        version_bundles_for_update(&deduped).len() < undeduped_bodies,
        "the declaration lets Core omit the unchanged row's body"
    );
    assert!(
        warm.missing_known_state_row_version_refs(&deduped)
            .unwrap()
            .is_empty()
    );
    warm.apply_sync_message_settled(deduped).unwrap();
    assert_eq!(read(&mut warm), expected);

    // `cold` makes the same declaration without holding the bodies: a
    // known-state miss. It reopens the view without known state and the
    // full resend converges to the same rows.
    let (_cold_dir, mut cold) = open_node_with_uuid(node(5));
    register_shape_binding(&mut cold, &shape, &binding);
    let deduped = serve(&mut core, true);
    assert!(
        !cold
            .missing_known_state_row_version_refs(&deduped)
            .unwrap()
            .is_empty()
    );
    let resend = serve(&mut core, false);
    assert!(
        cold.missing_known_state_row_version_refs(&resend)
            .unwrap()
            .is_empty()
    );
    cold.apply_sync_message_settled(resend).unwrap();
    assert_eq!(read(&mut cold), expected);
}

/// `counters` with an unsigned counter. The public schema API admits only
/// INTEGER and BIGINT counters, but a catalogue schema may declare a counter
/// on any integer type `INV-HIST-9` admits, `U64` included.
fn unsigned_counter_schema() -> JazzSchema {
    JazzSchema::new_with_branch_columns([TableSchema::new(
        "counters",
        [
            ColumnSchema::new("count", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_column_merge_strategy("count", MergeStrategy::Counter)])
}

fn unsigned_counter_cells(count: u64, title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("count".to_owned(), Value::U64(count)),
        ("title".to_owned(), v(title)),
    ])
}

/// INV-HIST-10 for an unsigned counter: a decrement is a negative delta, which
/// the column's own type cannot hold. It must still travel as an op and sum
/// with a concurrent increment: from 10, a concurrent -3 and +2 merge to 9.
#[test]
fn unsigned_counter_decrement_sums_with_a_concurrent_increment_at_core() {
    let schema = unsigned_counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x70);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(unsigned_counter_cells(10, "base")),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    sync_table_rows_to(&mut core, &mut bob, "counters");

    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20)
                .cells(unsigned_counter_cells(7, "alice")),
        )
        .expect("an unsigned counter can be decremented");
    assert_eq!(
        rows_at(&mut alice, "counters", DurabilityTier::Local)[&target].get("count"),
        Some(&Value::U64(7))
    );
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 21)
                .cells(unsigned_counter_cells(12, "bob")),
        )
        .unwrap();

    core_fate(&mut core, alice_unit);
    core_fate(&mut core, bob_unit);

    let expected = BTreeMap::from([(target, unsigned_counter_cells(9, "bob"))]);
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        expected
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    assert_eq!(
        rows_at(&mut alice, "counters", DurabilityTier::Global),
        expected
    );
}

/// `counter_schema()` plus a `notes` column, published as a descendant.
fn counter_schema_with_notes() -> JazzSchema {
    let source = [(
        PublicTableName::new("counters"),
        PublicTableSchema::new(PublicRowDescriptor::new(vec![
            PublicColumnDescriptor::new("count", PublicColumnType::Integer)
                .merge_strategy(PublicColumnMergeStrategy::Counter),
            PublicColumnDescriptor::new("title", PublicColumnType::Text),
            PublicColumnDescriptor::new("notes", PublicColumnType::Text),
        ])),
    )]
    .into_iter()
    .collect::<PublicSchema>();
    compile_public_test_schema(&source)
}

/// A merge-column write whose schema differs from the schema the row's
/// current image is stored under. Core cannot apply the op yet: across
/// layouts the row is still whole-row last-writer-wins (#3899), which would
/// store the counter's delta as its value, or drop it when the write loses.
/// Core refuses the write with an explicit reason instead, and the row keeps
/// its value.
///
/// ```text
/// alice (v1) ──count=1──► core            row stored under v1
/// bob   (v2) ──count +5──► core ──✗ Rejected("… not supported yet …")
/// ```
#[test]
fn merge_column_write_over_a_row_of_another_schema_is_refused() {
    let base = counter_schema();
    let evolved_schema = counter_schema_with_notes();
    let evolved = SchemaVersion::new(evolved_schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "counters".to_owned(),
                target_table: "counters".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "notes".to_owned(),
                    default: v(""),
                }],
            }],
        )
        .expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    let (_alice_dir, mut alice) = open_node_with_schema(node(1), base);
    let (_bob_dir, mut bob) = open_node_with_schema(node(2), evolved_schema);
    let target = row(0x71);

    commit_mergeable_global(
        &mut alice,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(counter_cells(1, "base")),
    );

    // bob has not seen the row: his +5 is made over an empty base.
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20)
                .cells(BTreeMap::from([("count".to_owned(), Value::I32(5))])),
        )
        .unwrap();
    let fate = core_fate(&mut core, bob_unit);
    let SyncMessage::FateUpdate {
        fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
        ..
    } = &fate
    else {
        panic!("Core must refuse a cross-schema merge op, got {fate:?}");
    };
    assert!(reason.contains("not supported yet"), "{reason}");
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, counter_cells(1, "base"))])
    );
}

/// A Core with `base` as its schema and `evolved` published as a descendant
/// through `ops` on the `counters` table.
fn core_with_descendant_schema(
    base: JazzSchema,
    evolved: JazzSchema,
    ops: Vec<LensOp>,
) -> (tempfile::TempDir, NodeState) {
    let evolved_version = SchemaVersion::new(evolved);
    let (core_dir, mut core) = open_node_with_schema(node(9), base.clone());
    publish_schema_lineage(
        &mut core,
        evolved_version.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved_version.id,
            vec![TableLens {
                source_table: "counters".to_owned(),
                target_table: "counters".to_owned(),
                ops,
            }],
        )
        .expect("valid migration lens"),
        Vec::<String>::new(),
        Vec::<String>::new(),
    )
    .unwrap();
    (core_dir, core)
}

fn add_notes_lens() -> Vec<LensOp> {
    vec![LensOp::AddColumn {
        column: "notes".to_owned(),
        default: v(""),
    }]
}

fn assert_accepted(fate: &SyncMessage) {
    assert!(
        matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ),
        "expected Accepted, got {fate:?}"
    );
}

/// INV-HIST-10 across schema versions: a plain-column write made under a
/// descendant schema must not carry its stale snapshot of a merge column over
/// ops Core already accepted. Across layouts the row is whole-row
/// last-writer-wins (#3899), and bob's patch holds the counter he saw as an
/// absolute value; Core keeps the counter's settled value instead.
///
/// ```text
/// base  (v1) ──count=1──────────► core   count=1
/// alice (v1) ──count +5─────────► core   count=6
/// bob   (v2) ──notes="bob" (saw count=1)──► core   count=6, notes="bob"
/// ```
#[test]
fn plain_column_write_across_schemas_keeps_accepted_counter_ops() {
    let base = counter_schema();
    let evolved = counter_schema_with_notes();
    let (_core_dir, mut core) =
        core_with_descendant_schema(base.clone(), evolved.clone(), add_notes_lens());
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), base.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), base);
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), evolved);
    let target = row(0x72);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(counter_cells(1, "base")),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 15).cells(counter_cells(6, "base")),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, alice_unit));
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global)[&target].get("count"),
        Some(&Value::I32(6))
    );

    // bob saw the row at count=1 and edits only `notes`: his patch carries
    // the rest of his snapshot, the counter as the absolute value 1.
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20)
                .cells(BTreeMap::from([
                    ("count".to_owned(), Value::I32(1)),
                    ("title".to_owned(), v("base")),
                    ("notes".to_owned(), v("bob")),
                ]))
                .authored_columns(BTreeSet::from(["notes".to_owned()])),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, bob_unit));

    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, counter_cells(6, "base"))]),
        "alice's accepted +5 must survive bob's cross-schema plain write"
    );
}

/// `tagged` rows under v1: a g-set column and a title.
fn gset_schema(with_notes: bool) -> JazzSchema {
    let mut columns = vec![
        ColumnSchema::new("tags", ColumnType::Array(Box::new(ColumnType::String))),
        ColumnSchema::new("title", ColumnType::String),
    ];
    if with_notes {
        columns.push(ColumnSchema::new("notes", ColumnType::String));
    }
    JazzSchema::new_with_branch_columns([TableSchema::new("counters", columns)
        .with_column_merge_strategy("tags", MergeStrategy::GSet)])
}

fn tags(values: &[&str]) -> Value {
    Value::Array(values.iter().map(|value| v(*value)).collect())
}

/// The g-set form of `plain_column_write_across_schemas_keeps_accepted_counter_ops`:
/// alice's accepted addition survives bob's cross-schema plain write made
/// over a snapshot without it.
#[test]
fn plain_column_write_across_schemas_keeps_accepted_gset_additions() {
    let base = gset_schema(false);
    let evolved = gset_schema(true);
    let (_core_dir, mut core) =
        core_with_descendant_schema(base.clone(), evolved.clone(), add_notes_lens());
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), base.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), base);
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), evolved);
    let target = row(0x73);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(BTreeMap::from([
            ("tags".to_owned(), tags(&["a"])),
            ("title".to_owned(), v("base")),
        ])),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 15)
                .cells(BTreeMap::from([("tags".to_owned(), tags(&["a", "b"]))]))
                .authored_columns(BTreeSet::from(["tags".to_owned()])),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, alice_unit));

    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20)
                .cells(BTreeMap::from([
                    ("tags".to_owned(), tags(&["a"])),
                    ("title".to_owned(), v("base")),
                    ("notes".to_owned(), v("bob")),
                ]))
                .authored_columns(BTreeSet::from(["notes".to_owned()])),
        )
        .unwrap();
    assert_accepted(&core_fate(&mut core, bob_unit));

    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global)[&target].get("tags"),
        Some(&tags(&["a", "b"])),
        "alice's accepted addition must survive bob's cross-schema plain write"
    );
}

/// A cross-schema plain write whose schema maps the merge column to another
/// name cannot carry the settled value without guessing: Core refuses it
/// with the explicit not-supported-yet reason, and the row keeps its counter.
#[test]
fn plain_column_write_across_schemas_with_a_renamed_merge_column_is_refused() {
    let base = counter_schema();
    let evolved = {
        let source = [(
            PublicTableName::new("counters"),
            PublicTableSchema::new(PublicRowDescriptor::new(vec![
                PublicColumnDescriptor::new("total", PublicColumnType::Integer)
                    .merge_strategy(PublicColumnMergeStrategy::Counter),
                PublicColumnDescriptor::new("title", PublicColumnType::Text),
                PublicColumnDescriptor::new("notes", PublicColumnType::Text),
            ])),
        )]
        .into_iter()
        .collect::<PublicSchema>();
        compile_public_test_schema(&source)
    };
    let mut ops = vec![LensOp::RenameColumn {
        from: "count".to_owned(),
        to: "total".to_owned(),
    }];
    ops.extend(add_notes_lens());
    let (_core_dir, mut core) = core_with_descendant_schema(base.clone(), evolved.clone(), ops);
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), base);
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), evolved);
    let target = row(0x74);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(counter_cells(6, "base")),
    );
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20)
                .cells(BTreeMap::from([
                    ("total".to_owned(), Value::I32(1)),
                    ("title".to_owned(), v("base")),
                    ("notes".to_owned(), v("bob")),
                ]))
                .authored_columns(BTreeSet::from(["notes".to_owned()])),
        )
        .unwrap();
    let fate = core_fate(&mut core, bob_unit);
    let SyncMessage::FateUpdate {
        fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
        ..
    } = &fate
    else {
        panic!("Core must refuse the write, got {fate:?}");
    };
    assert!(reason.contains("not supported yet (#3899)"), "{reason}");
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, counter_cells(6, "base"))])
    );
}

/// A counter op that would take the column outside its type's range is
/// rejected at Core with an explicit reason instead of wrapping. From 1 on a
/// `U64` counter two concurrent -1s: the first settles to 0, the second is
/// rejected and the counter stays 0.
#[test]
fn unsigned_counter_op_below_zero_is_rejected_at_core() {
    let schema = unsigned_counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x75);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10).cells(unsigned_counter_cells(1, "base")),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    sync_table_rows_to(&mut core, &mut bob, "counters");
    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20)
                .cells(unsigned_counter_cells(0, "alice")),
        )
        .unwrap();
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 21).cells(unsigned_counter_cells(0, "bob")),
        )
        .unwrap();

    assert_accepted(&core_fate(&mut core, alice_unit));
    let fate = core_fate(&mut core, bob_unit);
    let SyncMessage::FateUpdate {
        fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
        ..
    } = &fate
    else {
        panic!("Core must reject a counter op that leaves the range, got {fate:?}");
    };
    assert!(reason.contains("out of range"), "{reason}");
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, unsigned_counter_cells(0, "alice"))])
    );
}

/// The signed form: from `i32::MAX - 1` two concurrent +1s on an `INTEGER`
/// counter. The first settles to `i32::MAX`; the second is rejected.
#[test]
fn signed_counter_op_past_its_maximum_is_rejected_at_core() {
    let schema = counter_schema();
    let (_base_dir, mut base_writer) = open_node_with_schema(node(1), schema.clone());
    let (_alice_dir, mut alice) = open_node_with_schema(node(2), schema.clone());
    let (_bob_dir, mut bob) = open_node_with_schema(node(3), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let target = row(0x76);

    commit_mergeable_global(
        &mut base_writer,
        &mut core,
        MergeableCommit::new("counters", target, 10)
            .cells(counter_cells(i32::MAX - 1, "base")),
    );
    sync_table_rows_to(&mut core, &mut alice, "counters");
    sync_table_rows_to(&mut core, &mut bob, "counters");
    let (_alice_tx, alice_unit) = alice
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 20).cells(counter_cells(i32::MAX, "alice")),
        )
        .unwrap();
    let (_bob_tx, bob_unit) = bob
        .commit_mergeable_unit_settled(
            MergeableCommit::new("counters", target, 21).cells(counter_cells(i32::MAX, "bob")),
        )
        .unwrap();

    assert_accepted(&core_fate(&mut core, alice_unit));
    let fate = core_fate(&mut core, bob_unit);
    assert!(
        matches!(
            &fate,
            SyncMessage::FateUpdate {
                fate: Fate::Rejected(RejectionReason::MalformedCommit(reason)),
                ..
            } if reason.contains("out of range")
        ),
        "Core must reject a counter op past i32::MAX, got {fate:?}"
    );
    assert_eq!(
        rows_at(&mut core, "counters", DurabilityTier::Global),
        BTreeMap::from([(target, counter_cells(i32::MAX, "alice"))])
    );
}
