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
