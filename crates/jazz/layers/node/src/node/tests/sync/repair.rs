// Known-state miss detection: which referenced rows lack a local body.

/// A logical name can be reused after its old physical table was dropped. An
/// inline body from the old lineage must not silently cover a current result
/// member merely because their table names, row UUIDs, and transactions agree.
#[test]
fn inline_known_state_witness_rejects_reused_logical_table_name() {
    let original = renamed_tasks_schema();
    let without_tasks = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("notes").column("body", PublicColumnType::Text)),
    );
    let reintroduced = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("notes").column("body", PublicColumnType::Text))
            .table(PublicTableSchemaBuilder::new("tasks").column("name", PublicColumnType::Text)),
    );
    let without_tasks_version = SchemaVersion::new(without_tasks.clone());
    let reintroduced_version = SchemaVersion::new(reintroduced.clone());
    let (_dir, mut receiver) = open_node_with_schema(node(0x96), original.clone());
    publish_schema_lineage(
        &mut receiver,
        without_tasks_version.clone(),
        MigrationLens::new(original.version_id(), without_tasks_version.id, Vec::new())
            .expect("valid migration lens"),
        ["notes"],
        ["tasks"],
    )
    .unwrap();
    publish_schema_lineage(
        &mut receiver,
        reintroduced_version.clone(),
        MigrationLens::new(
            without_tasks_version.id,
            reintroduced_version.id,
            vec![TableLens {
                source_table: "notes".to_owned(),
                target_table: "notes".to_owned(),
                ops: Vec::new(),
            }],
        )
        .expect("valid migration lens"),
        ["tasks"],
        Vec::<String>::new(),
    )
    .unwrap();
    receiver
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 2,
            schema: reintroduced_version.id,
        })
        .unwrap();
    let old_table_id = receiver
        .physical_table_id_for_schema(original.version_id(), "tasks")
        .unwrap();
    let new_table_id = receiver
        .physical_table_id_for_schema(reintroduced_version.id, "tasks")
        .unwrap();
    assert_ne!(
        old_table_id, new_table_id,
        "the active catalogue deliberately reuses `tasks` for a new physical lineage"
    );

    let shape = Query::from("tasks").validate(&reintroduced).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut receiver, &shape, &binding);
    let tx_id = TxId::new(TxTime(60), node(0x97));
    let transaction = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 1,
        made_by: AuthorSubject::system_at(tx_id.node),
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let task_row = row(0x98);
    let old_inline_task = VersionRecord::from_cells(
        &original.tables[0],
        original.version_id(),
        task_row,
        AuthorSubject::system_at(node(1)),
        tx_id.time.physical_ms(),
        AuthorSubject::system_at(node(1)),
        tx_id.time.physical_ms(),
        &BTreeMap::from([("name".to_owned(), v("old physical task"))]),
        None,)
    .unwrap();
    let update = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: crate::protocol::SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        settled_through: GlobalTime::default(),

        version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
            scope: crate::protocol::VersionBundleScope::CompleteTransaction,
            tx: transaction,
            versions: vec![old_inline_task],
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: DurabilityTier::Global,
        })],
        peer_payload_inventory: Default::default(),
        supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(vec![crate::protocol::SupportingRow {
                physical_table: receiver.local_availability_table_id(reintroduced.version_id(), "tasks").unwrap(),
                version_table: "tasks".to_owned().into(),
                row: task_row,
                version: crate::protocol::RowVersionRefEntry {
                    tx: tx_id,
                    schema_version: Some(reintroduced.version_id()),
                    layer: crate::protocol::ResultRowLayer::Content,
                    batch: Some(tx_id),
                    branch_or_prefix: None,
                    row_digest: None,
                },
            }]),
    });
    assert_eq!(
        receiver
            .missing_known_state_row_version_refs(&update)
            .unwrap(),
        vec![RowVersionRef::new("tasks", task_row, tx_id)],
        "an old same-named inline body does not cover the registered shape's reintroduced lineage"
    );
}
/// Alice restores a row with new content in one transaction. Bob receives the
/// content body but needs the distinct deletion-register body before accepting
/// the complete supporting set. A matching content body is not that witness.
///
/// Alice ──content + restore──► Core ──content only──► Bob
/// Bob ──exact row/transaction repair──► Core ──both layers──► Bob
#[test]
fn supporting_snapshot_repairs_missing_same_transaction_deletion_layer() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(0xc7);
    let open = OpenTransactionId::new();
    core.open_exclusive(open).unwrap();
    core.tx_write(open, "todos", row_uuid, title_cells("restored"), None).unwrap();
    core.tx_write(open, "todos", row_uuid, BTreeMap::<String, Value>::new(), Some(DeletionEvent::Restored)).unwrap();
    let (tx_id, _) = core.commit_exclusive_settled(open, AuthorSubject::SYSTEM, 10).unwrap();
    core.apply_fate_update(tx_id, Fate::Accepted, Some(GlobalTime(1)), Some(DurabilityTier::Global)).unwrap();
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);
    let subscription = reader.whole_table_subscription_key("todos").unwrap();
    let mut update = system_authority_reset(&mut core, &shape, &binding, subscription);
    let SyncMessage::ViewUpdate(payload) = &mut update else { panic!("expected supporting snapshot") };
    // A complete input set may retain both independently stored layers.
    let mut restore = payload.supporting_rows.added_rows().iter().find(|row| row.version.tx == tx_id).unwrap().clone();
    restore.version.layer = crate::protocol::ResultRowLayer::Deletion;
    payload.supporting_rows = crate::protocol::SupportingRowsUpdate::snapshot(vec![restore]);
    let mut bundles = crate::protocol::expand_version_carriers(&payload.version_carriers).unwrap();
    for bundle in &mut bundles {
        bundle.scope = crate::protocol::VersionBundleScope::ViewScoped;
        bundle.versions.retain(|version| version.deletion().is_none());
        bundle.tx.n_total_writes = bundle.versions.len().try_into().unwrap();
    }
    payload.version_carriers = bundles.into_iter().map(crate::protocol::VersionCarrier::Bundle).collect();
    let expected = vec![crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id)];
    assert_eq!(reader.missing_known_state_row_version_refs(&update).unwrap(), expected,
        "an inline content sibling must not hide the missing deletion witness");
    let SyncMessage::ViewUpdate(payload) = &mut update else { unreachable!() };
    let content_only = crate::protocol::expand_version_carriers(&payload.version_carriers).unwrap();
    reader.apply_row_version_payloads_for_requests(&expected, content_only).unwrap();
    payload.version_carriers.clear();
    assert_eq!(reader.missing_known_state_row_version_refs(&update).unwrap(), expected,
        "a resident content sibling must not hide the missing deletion witness");
}

