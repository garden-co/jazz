// Internal storage accounting is necessary here: unchanged public rows cannot
// distinguish a duplicate receipt from a needless durable rewrite. The fixture
// otherwise uses the public schema builder and ordinary commit/fate ingress.

#[test]
fn accepted_fate_replay_does_not_write_and_still_validates_metadata() {
    use groove::storage::{TestStorage, TestStorageOperation};
    let schema = JazzSchema::new(
        &PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("tasks").column("title", PublicColumnType::Text))
            .build(),
    )
    .unwrap();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let mut alice = NodeState::new_with_shared_test_catalogue(node(1), schema, storage).unwrap();
    let (tx_id, _) = alice
        .commit_mergeable_unit_settled(MergeableCommit::new("tasks", row(1), 10).cells(
            BTreeMap::from([("title".to_owned(), Value::String("one".to_owned()))]),
        ))
        .unwrap();
    alice
        .apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    let expected = alice.transaction_state(tx_id).unwrap();
    control.take_observed();

    for fate in [Fate::Accepted, Fate::Pending, Fate::Accepted] {
        alice
            .apply_fate_update(
                tx_id,
                fate,
                Some(GlobalTime(1)),
                Some(DurabilityTier::Global),
            )
            .unwrap();
    }
    let replay_operations = control.take_observed();
    assert_eq!(alice.transaction_state(tx_id).unwrap(), expected);
    assert_eq!(
        alice
            .current_rows("tasks", DurabilityTier::Global)
            .unwrap()
            .len(),
        1
    );
    assert!(
        !replay_operations.iter().any(|operation| matches!(
            operation,
            TestStorageOperation::WriteMany
                | TestStorageOperation::Set
                | TestStorageOperation::Delete
        )),
        "an identical accepted receipt must not issue a durable mutation: {replay_operations:?}",
    );
    assert!(matches!(
        alice
            .apply_fate_update(
                tx_id,
                Fate::Accepted,
                Some(GlobalTime(0)),
                Some(DurabilityTier::Global)
            )
            .unwrap_err(),
        Error::NonMonotoneState(_),
    ));
    assert!(matches!(
        alice
            .apply_fate_update(
                tx_id,
                Fate::Rejected(RejectionReason::Cascade { root: tx_id }),
                None,
                None
            )
            .unwrap_err(),
        Error::ConflictingFate,
    ));
    // New authoritative metadata still takes the ordinary persistence path.
    control.take_observed();
    alice
        .apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(2)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    assert!(
        control
            .observed()
            .contains(&TestStorageOperation::WriteMany)
    );
    assert_eq!(
        alice.transaction_state(tx_id).unwrap().1,
        Some(GlobalTime(2))
    );
}

/// Contract: a mergeable transaction first learned as a Pending view-scoped
/// fragment and later completed as globally accepted installs global current
/// and clears ahead-current for *every* version, including the one the
/// fragment stored earlier. An identical accepted receipt replayed afterwards
/// then leaves the settled state unchanged and issues no durable mutation.
///
/// This drives node-internal ingress (`ingest_view_scoped_transaction_*`,
/// `ingest_known_transaction`) and reads ahead-current tables directly: the
/// bug is that the fragment's version stayed ahead-only, and the only visible
/// symptom through public rows is a missing global row, which cannot show
/// whether the replay wrote.
///
/// ```
/// bob's tx (rows A, B)
///   alice ◄── view fragment {A}, Pending/Local ── A ahead-only
///   alice ◄── complete {A, B}, Accepted GT1 ───── A and B global, none ahead
///   alice ◄── receipt Accepted GT1 (replay) ───── no durable write
/// ```
#[test]
fn accepted_completion_of_pending_fragment_settles_every_version_before_receipt_replay() {
    use groove::storage::{TestStorage, TestStorageOperation};
    let schema = schema();
    let table = schema.tables[0].name.clone();
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, control) = TestStorage::controlled(&refs);
    let mut alice = NodeState::new_with_shared_test_catalogue(node(0x91), schema, storage).unwrap();
    let bob = node(0x92);
    let tx_id = TxId::new(TxTime::from(70), bob);
    let tx = |n_total_writes: u32| Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes,
        made_by: AuthorSubject::system_at(bob),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let a = version_record(row(0xa1), Vec::new(), title_cells("a"), None);
    let b = version_record(row(0xa2), Vec::new(), title_cells("b"), None);

    alice
        .ingest_view_scoped_transaction_with_current_indexes(
            tx(1),
            vec![a.clone()],
            Fate::Pending,
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    assert_eq!(
        alice
            .current_rows(&table, DurabilityTier::Global)
            .unwrap()
            .len(),
        0
    );
    assert_eq!(ahead_current_row_count(&mut alice, &table), 1);

    alice
        .ingest_known_transaction(
            tx(2),
            vec![a, b],
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();
    let settled = alice.query_transaction(tx_id).unwrap().unwrap();
    assert_eq!(settled.fate, Fate::Accepted);
    assert_eq!(settled.global_time, Some(GlobalTime(1)));
    assert!(!settled.view_scoped_cardinality);
    assert_eq!(
        alice
            .current_rows(&table, DurabilityTier::Global)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(ahead_current_row_count(&mut alice, &table), 0);
    let expected = alice.transaction_state(tx_id).unwrap();

    control.take_observed();
    alice
        .apply_fate_update(
            tx_id,
            Fate::Accepted,
            Some(GlobalTime(1)),
            Some(DurabilityTier::Global),
        )
        .unwrap();
    let replay_operations = control.take_observed();
    assert_eq!(alice.transaction_state(tx_id).unwrap(), expected);
    assert_eq!(
        alice
            .current_rows(&table, DurabilityTier::Global)
            .unwrap()
            .len(),
        2
    );
    assert_eq!(ahead_current_row_count(&mut alice, &table), 0);
    assert!(
        !replay_operations.iter().any(|operation| matches!(
            operation,
            TestStorageOperation::WriteMany
                | TestStorageOperation::Set
                | TestStorageOperation::Delete
        )),
        "an identical accepted receipt must not issue a durable mutation: {replay_operations:?}",
    );
}
