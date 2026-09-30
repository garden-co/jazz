/// Slots 5-8 of `jazz_transactions` for `tx_id`, as stored.
fn stored_exclusive_evidence_slots(node: &NodeState, tx_id: TxId) -> Vec<Value> {
    let alias = node.node_aliases[&tx_id.node];
    let raw = crate::local_executor::block_on(node.database.primary_key_get_raw(
        "jazz_transactions",
        &[Value::U64(tx_id.time.0), Value::U64(alias.0)],
    ))
    .unwrap()
    .unwrap();
    let record = raw.record();
    [
        TransactionRowRecord::FIELD_BASE_SNAPSHOT_IDX,
        TransactionRowRecord::FIELD_ROW_READ_SET_IDX,
        TransactionRowRecord::FIELD_ABSENT_READ_SET_IDX,
        TransactionRowRecord::FIELD_PREDICATE_READ_SET_IDX,
    ]
    .into_iter()
    .map(|index| record.get_idx(index).unwrap())
    .collect()
}

/// Internal because the contract is the durable audit row: a pending
/// exclusive transaction keeps its read evidence, the unit rebuilt from
/// storage after a reopen is the unit that was published, and the evidence
/// is dropped once the authority settles the fate.
///
/// ```
/// client ──read / query / absent / write──► commit (pending, evidence stored)
///    │ reopen                                          │
///    └── commit_unit_for == published unit ──► core ──accepted──► evidence cleared
/// ```
#[test]
fn exclusive_evidence_is_stored_while_pending_and_cleared_at_settlement() {
    let (client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    commit_mergeable_global(
        &mut client,
        &mut core,
        MergeableCommit::new("todos", row(1), 10).cells(title_cells("base")),
    );
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("title")))
        .validate(&client.catalogue.schema)
        .unwrap();
    let binding = shape
        .bind(BTreeMap::from([("title".to_owned(), v("base"))]))
        .unwrap();

    let open_id = OpenTransactionId::new();
    client.open_exclusive(open_id).unwrap();
    assert!(client.tx_read(open_id, "todos", row(1)).unwrap().is_some());
    assert!(client.tx_read(open_id, "todos", row(2)).unwrap().is_none());
    assert_eq!(client.tx_query(open_id, &shape, &binding).unwrap().len(), 1);
    client
        .tx_write(open_id, "todos", row(1), title_cells("next"), None)
        .unwrap();
    let (tx_id, unit) = client
        .commit_exclusive_settled(open_id, AuthorSubject::SYSTEM, 11)
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = &unit else {
        panic!("expected commit unit");
    };
    assert!(
        tx.row_read_set
            .as_ref()
            .is_some_and(|reads| !reads.is_empty())
    );
    assert!(
        tx.absent_read_set
            .as_ref()
            .is_some_and(|reads| !reads.is_empty())
    );
    assert!(
        tx.predicate_read_set
            .as_ref()
            .is_some_and(|reads| !reads.is_empty())
    );
    assert!(
        stored_exclusive_evidence_slots(&client, tx_id)
            .iter()
            .all(|slot| matches!(slot, Value::Nullable(Some(_))))
    );

    // A duplicate of the same pending unit without evidence must not erase
    // the evidence this node still needs to retransmit.
    let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
        unreachable!()
    };
    let redacted = Transaction {
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        ..tx
    };
    client
        .apply_sync_message_settled(SyncMessage::CommitUnit {
            tx: redacted,
            versions,
        })
        .unwrap();

    drop(client);
    let mut client = reopen_node_at(&client_dir, node(1), schema());
    assert_eq!(client.commit_unit_for(tx_id).unwrap(), unit);

    let updates = core.apply_sync_message_settled(unit).unwrap();
    let [
        fate @ SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        },
    ] = updates.as_slice()
    else {
        panic!("expected accepted fate update, got {updates:?}");
    };
    client.apply_sync_message_settled(fate.clone()).unwrap();
    assert_eq!(
        client.transaction_state_settled(tx_id).unwrap().0,
        Fate::Accepted
    );
    assert!(
        stored_exclusive_evidence_slots(&client, tx_id)
            .iter()
            .all(|slot| *slot == Value::Nullable(None))
    );
    let SyncMessage::CommitUnit { tx, .. } = client.commit_unit_for(tx_id).unwrap() else {
        panic!("expected commit unit");
    };
    assert_eq!(tx.base_snapshot, None);
}

/// Internal fixture: no current writer can put evidence on a mergeable row,
/// so plant one and check that reading it is refused as corrupt storage.
#[test]
fn mergeable_transaction_row_with_exclusive_evidence_is_rejected() {
    let (dir, mut writer) = open_node_with_uuid(node(1));
    let tx_id = writer
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(1), 10).cells(title_cells("plain")),
        )
        .unwrap();
    let stored = writer.query_transaction(tx_id).unwrap().unwrap();
    let mut values = transaction_values(
        stored.node_alias,
        &stored.tx,
        stored.fate.clone(),
        stored.global_time,
        stored.durability,
        Value::Nullable(None),
    )
    .unwrap();
    let snapshot =
        crate::tx::Snapshot::exclusive_base(node(1), GlobalTime(0), TxTime::from(1), Vec::new())
            .unwrap();
    let mut exclusive = stored.tx.clone();
    exclusive.kind = TxKind::Exclusive;
    exclusive.base_snapshot = Some(snapshot);
    let [base, ..] =
        super::super::exclusive_read_evidence::evidence_slot_values(&exclusive, true).unwrap();
    values[TransactionRowRecord::FIELD_BASE_SNAPSHOT_IDX] = base;
    let mut batch = writer.database.open_batch();
    batch.update("jazz_transactions", values);
    let applied = crate::local_executor::block_on(writer.database.apply_batch(batch)).unwrap();
    let persisted = crate::local_executor::block_on(applied.persist());
    writer.database.finish_persistence(persisted).unwrap();
    drop(writer);

    let mut reopened = reopen_node_at(&dir, node(1), schema());
    assert!(matches!(
        crate::local_executor::block_on(reopened.query_transaction(tx_id)),
        Err(Error::InvalidStoredValue(
            "mergeable transaction carries exclusive read evidence"
        ))
    ));
}

/// The author of a pending exclusive transaction keeps its read evidence, but
/// view carriers never ship it. When Core's view of the accepted transaction
/// reaches the author (as a browser worker relays it back to the tab), the
/// stored evidence must not make the bundle look like a conflicting payload.
///
/// ```
/// alice ──commit exclusive (pending, evidence stored)──► core ──accept
///   ▲                                                      │
///   └──────────── view update (bundle, no evidence) ◄──────┘
/// ```
#[test]
fn view_bundle_for_a_pending_exclusive_transaction_matches_its_stored_evidence() {
    let (_alice_dir, mut alice) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let shape = Query::from("todos").validate(&schema()).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();

    let open_id = OpenTransactionId::new();
    alice.open_exclusive(open_id).unwrap();
    assert!(alice.tx_read(open_id, "todos", row(1)).unwrap().is_none());
    alice
        .tx_write(open_id, "todos", row(1), title_cells("one"), None)
        .unwrap();
    let (tx_id, unit) = alice
        .commit_exclusive_settled(open_id, AuthorSubject::SYSTEM, 10)
        .unwrap();
    assert!(
        stored_exclusive_evidence_slots(&alice, tx_id)
            .iter()
            .any(|slot| matches!(slot, Value::Nullable(Some(_))))
    );
    let [fate] = core
        .apply_sync_message_settled(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let mut peer = relay_with_system_binding(crate::protocol::SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    });
    let update = peer.rehydrate_query(&mut core, &shape, &binding).unwrap();
    assert!(
        version_bundles_for_update(&update)
            .iter()
            .all(|bundle| bundle.tx.base_snapshot.is_none())
    );
    register_shape_binding_for_receiver(&mut alice, &shape, &binding);
    alice.apply_sync_message_settled(update).unwrap();
    alice.apply_sync_message_settled(fate).unwrap();
    assert_eq!(
        alice.transaction_state_settled(tx_id).unwrap().0,
        Fate::Accepted
    );
    assert_eq!(
        alice
            .current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<Vec<_>>(),
        vec![(row(1), title_cells("one"))]
    );
}
