// Internal tests are necessary here: public APIs cannot manufacture corrupt
// resolver metadata or hold a native witness across the precise eviction point.
// These tests use real RocksDB bodies and the existing public schema builder.

fn witness_test_cells(title: &str) -> BTreeMap<String, Value> {
    crate::row_input! { "title" => title }
        .into_iter()
        .map(|(name, value)| match value {
            PublicValue::Text(text) => (name, Value::String(text)),
            other => panic!("unexpected fixture value: {other:?}"),
        })
        .collect()
}

fn witness_test_reference(
    node: &NodeState<RocksDbStorage>,
    version: &VersionRow,
) -> crate::node::maintained_version::NativeVersionRef {
    crate::node::maintained_version::NativeVersionRef {
        physical_table: node.physical_table_id_for_version(version).unwrap(),
        table: version.table,
        branch: version.branch_key().clone(),
        row: version.row_uuid(),
        time: version.tx_time(),
        node: version.tx_node_alias(),
        schema: version.schema_version_alias(),
        deletion: version.deletion(),
    }
}

fn assert_witness_test_version(actual: VersionRow, expected: &VersionRow) {
    assert_eq!(actual.table, expected.table);
    assert_eq!(actual.branch_key(), expected.branch_key());
    assert_eq!(actual.record.raw(), expected.record.raw());
}

fn witness_test_native(
    reference: crate::node::maintained_version::NativeVersionRef,
) -> crate::node::maintained_version::MaintainedVersion {
    crate::node::maintained_version::MaintainedVersion::Native(std::sync::Arc::new(reference))
}

/// Alice's immutable reference must fail after real body eviction and recover
/// only after Bob's authority redelivers the exact body and fresh coverage.
/// Alice -> Bob/Core -> reader; reader evicts -> Bob redelivers -> reader.
/// Internal because the public client cannot retain the resolver's private ref.
#[test]
#[ignore = "#2960: exact body redelivery is rejected after eviction before native reference recovery"]
fn native_witness_ref_eviction_and_authority_repair() {
    assert_native_witness_ref_eviction(true);
}

/// Bob's live reference loses its body and coverage after either explicit
/// eviction path. It must return missing-body, never retained or unrelated data.
/// Internal because the native witness and the cache boundary are private.
#[test]
fn native_witness_ref_eviction_fails_closed() {
    assert_native_witness_ref_eviction(false);
}

fn assert_native_witness_ref_eviction(repair: bool) {
    for budgeted in [false, true] {
        let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
        let (_core_dir, mut core) = open_node_with_uuid(node(9));
        let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
        let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
        let subscription = core.whole_table_subscription_key("todos").unwrap();
        let row_uuid = row(0x71);
        commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("todos", row_uuid, 19)
                .cells(witness_test_cells("Alice's original")),
        );
        register_shape_binding(&mut reader, &shape, &binding);
        let update = system_authority_reset(&mut core, &shape, &binding, subscription);
        reader.apply_sync_message_settled(update).unwrap();
        let version = reader
            .query_row_versions("todos", row_uuid)
            .unwrap()
            .remove(0);
        let reference = witness_test_native(witness_test_reference(&reader, &version));
        assert_witness_test_version(
            reader.resolve_maintained_version(&reference).unwrap(),
            &version,
        );
        let key = reader
            .authority_result_key_for_subscription(subscription)
            .unwrap();
        let generation = reader.applied_authority_result_generation(&key);
        assert!(reader.has_settled_authority_result(&key));
        if budgeted {
            reader
                .enforce_client_cache_budget(ClientCacheBudget::new(0))
                .resolve()
                .unwrap();
        } else {
            reader.evict_cold().resolve().unwrap();
        }
        assert!(reader.row_history("todos", row_uuid).unwrap().is_empty());
        assert!(!reader.has_settled_authority_result(&key));
        assert!(matches!(
            reader.resolve_maintained_version(&reference).resolve(),
            Err(Error::MaintainedViewMissingBundleWitness(
                "native witness has no canonical history body"
            ))
        ));
        if !repair {
            continue;
        }
        let repair = system_authority_reset(&mut core, &shape, &binding, subscription);
        reader.apply_sync_message_settled(repair).unwrap();
        assert_witness_test_version(
            reader.resolve_maintained_version(&reference).unwrap(),
            &version,
        );
        assert!(reader.has_settled_authority_result(&key));
        assert!(reader.applied_authority_result_generation(&key) > generation);
        let rows = receiver_rows(&mut reader, &shape, &binding, DurabilityTier::Global);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), row_uuid);
        assert_eq!(
            rows[0].cell(reader.table("todos").unwrap(), "title"),
            Some(v("Alice's original"))
        );
    }
}

/// Alice updates, deletes and restores a row; its old witness must still name
/// its original immutable body, while the visible row follows the new events.
/// Internal because a public result cannot distinguish private witness identity.
#[test]
fn native_witness_ref_stays_exact_across_update_delete_restore() {
    let (_directory, mut writer) = open_node_with_uuid(node(1));
    let row_uuid = row(0x72);
    let first = accept_global(
        &mut writer,
        MergeableCommit::new("todos", row_uuid, 10).cells(witness_test_cells("old body")),
    );
    let version = writer.query_versions_for_tx(first).unwrap().remove(0);
    let reference = witness_test_native(witness_test_reference(&writer, &version));
    accept_global(
        &mut writer,
        MergeableCommit::new("todos", row_uuid, 11).cells(witness_test_cells("new body")),
    );
    let deleted = accept_global(
        &mut writer,
        MergeableCommit::new("todos", row_uuid, 12).deletion(DeletionEvent::Deleted),
    );
    assert!(
        writer
            .visible_current_cells("todos", row_uuid)
            .unwrap()
            .is_none()
    );
    assert_witness_test_version(
        writer.resolve_maintained_version(&reference).unwrap(),
        &version,
    );
    let deletion = writer.query_versions_for_tx(deleted).unwrap().remove(0);
    let deletion_ref = witness_test_native(witness_test_reference(&writer, &deletion));
    assert_witness_test_version(
        writer.resolve_maintained_version(&deletion_ref).unwrap(),
        &deletion,
    );
    accept_global(
        &mut writer,
        MergeableCommit::new("todos", row_uuid, 13).deletion(DeletionEvent::Restored),
    );
    assert_eq!(
        writer
            .visible_current_cells("todos", row_uuid)
            .unwrap()
            .unwrap()
            .get("title"),
        Some(&v("new body"))
    );
    assert_witness_test_version(
        writer.resolve_maintained_version(&reference).unwrap(),
        &version,
    );
    assert_witness_test_version(
        writer.resolve_maintained_version(&deletion_ref).unwrap(),
        &deletion,
    );
}

/// A corrupted reference cannot substitute Mallory's row or another authored
/// schema. Internal because only the resolver creates native metadata.
#[test]
fn native_witness_ref_rejects_schema_mismatch_and_missing_coordinate() {
    let (_directory, mut writer) = open_node_with_uuid(node(1));
    let first = accept_global(
        &mut writer,
        MergeableCommit::new("todos", row(0x73), 10).cells(witness_test_cells("Alice")),
    );
    accept_global(
        &mut writer,
        MergeableCommit::new("todos", row(0x74), 11).cells(witness_test_cells("Mallory")),
    );
    let version = writer.query_versions_for_tx(first).unwrap().remove(0);
    let reference = witness_test_reference(&writer, &version);
    let mut bad_schema = reference.clone();
    bad_schema.schema = SchemaVersionAlias(u64::MAX);
    assert!(matches!(
        writer
            .resolve_maintained_version(&witness_test_native(bad_schema))
            .resolve(),
        Err(Error::InvalidStoredValue(
            "native witness disagrees with canonical history"
        ))
    ));
    let mut wrong_row = reference.clone();
    wrong_row.row = row(0x74);
    assert!(matches!(
        writer
            .resolve_maintained_version(&witness_test_native(wrong_row))
            .resolve(),
        Err(Error::MaintainedViewMissingBundleWitness(
            "native witness has no canonical history body"
        ))
    ));
    assert_witness_test_version(
        writer
            .resolve_maintained_version(&witness_test_native(reference))
            .unwrap(),
        &version,
    );
}

/// Deleted and restored events share the deletion layer, but cannot substitute
/// for one another at the same exact coordinate. Internal metadata fault test.
#[test]
fn native_witness_ref_rejects_deletion_event_mismatch() {
    let (_directory, mut writer) = open_node_with_uuid(node(1));
    accept_global(
        &mut writer,
        MergeableCommit::new("todos", row(0x75), 10).cells(witness_test_cells("Alice")),
    );
    let deleted = accept_global(
        &mut writer,
        MergeableCommit::new("todos", row(0x75), 11).deletion(DeletionEvent::Deleted),
    );
    let version = writer.query_versions_for_tx(deleted).unwrap().remove(0);
    let mut reference = witness_test_reference(&writer, &version);
    assert_eq!(reference.deletion, Some(DeletionEvent::Deleted));
    reference.deletion = Some(DeletionEvent::Restored);
    assert!(matches!(
        writer
            .resolve_maintained_version(&witness_test_native(reference))
            .resolve(),
        Err(Error::InvalidStoredValue(
            "native witness disagrees with canonical history"
        ))
    ));
}
