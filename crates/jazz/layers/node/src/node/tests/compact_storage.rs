// Compact storage columns: node aliases stored as a fixed U32 and row
// provenance timestamps stored in 6-byte slots (INV-DATA-2, INV-DATA-27).
//
// These tests read RocksDB directly: the stored widths are a durable layout
// below every row API, and a round trip alone cannot tell a 4-byte alias or a
// 6-byte timestamp apart from the 8-byte forms they replaced.

/// Every raw entry of a closed RocksDB store whose class-CF key frame
/// (`u32be len | logical name | key`) names `logical`, as `(key, value)` with
/// the frame removed.
fn compact_storage_raw_entries(
    path: &std::path::Path,
    logical: &str,
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let options = rocksdb::Options::default();
    let families = rocksdb::DB::list_cf(&options, path).unwrap();
    let db = rocksdb::DB::open_cf_for_read_only(&options, path, &families, false).unwrap();
    let mut frame = (logical.len() as u32).to_be_bytes().to_vec();
    frame.extend_from_slice(logical.as_bytes());
    let mut entries = Vec::new();
    for family in &families {
        let handle = db.cf_handle(family).unwrap();
        for entry in db.iterator_cf(&handle, rocksdb::IteratorMode::Start) {
            let (key, value) = entry.unwrap();
            if let Some(rest) = key.strip_prefix(frame.as_slice()) {
                entries.push((rest.to_vec(), value.to_vec()));
            }
        }
    }
    entries
}

/// The stored bytes of `field` in one raw row of `table`, located through the
/// table's physical (storage) record descriptor.
fn compact_storage_field_bytes(
    storage_table: &groove::schema::TableSchema,
    value: &[u8],
    field: &str,
) -> Vec<u8> {
    let (tag, payload) = groove::records::split_variant_record(value).unwrap();
    let descriptor = storage_table
        .storage_record_schema_for_variant(tag)
        .expect("stored variant has a storage descriptor");
    let index = descriptor.field_index(field).unwrap();
    payload[descriptor.field_span(payload, index).unwrap()].to_vec()
}

fn compact_storage_commit_accepted(state: &mut NodeState, now_ms: u64) -> TxId {
    let tx_id = state
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x5a), now_ms)
                .cells(BTreeMap::from([("title".to_owned(), Value::String("compact".to_owned()))])),
        )
        .unwrap();
    state.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    tx_id
}

/// Supersede the row `compact_storage_commit_accepted` wrote with a second
/// accepted write at seq 2, so the first image moves into history (history
/// holds only superseded images; SPEC 2 §2.7.1).
fn compact_storage_supersede(state: &mut NodeState, now_ms: u64) -> TxId {
    let tx_id = state
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x5a), now_ms)
                .cells(BTreeMap::from([("title".to_owned(), Value::String("compact 2".to_owned()))])),
        )
        .unwrap();
    state.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(2)),
        Some(DurabilityTier::Global),
    )
    .unwrap();
    tx_id
}

/// Contract: transaction, history and global-current rows store the writer's
/// node alias as exactly 4 bytes (key and payload), and history and
/// global-current rows store `created_at` / `updated_at` as exactly 6
/// little-endian bytes of Unix milliseconds.
#[test]
fn stored_rows_keep_node_aliases_in_four_bytes_and_provenance_in_six() {
    let (temp_dir, mut state) = open_node();
    let now_ms = 0x0102_0304_0506;
    let tx_id = compact_storage_commit_accepted(&mut state, now_ms);
    let latest = compact_storage_supersede(&mut state, now_ms);
    let alias = *state.node_aliases.get(&tx_id.node).unwrap();
    let physical_table = state
        .physical_table_id_for_schema(state.catalogue.local_schema_version_id, "todos")
        .unwrap();
    let history_name = physical_history_table_name(physical_table);
    let global_name = physical_global_current_table_name(physical_table);
    let history_table = state.database.table_schema(&history_name).unwrap().clone();
    let global_table = state.database.table_schema(&global_name).unwrap().clone();
    state.database.close().unwrap();
    drop(state);

    let alias_bytes = [&[0x02][..], &alias.0.to_be_bytes()].concat();
    let ms_bytes = now_ms.to_le_bytes()[..6].to_vec();

    // jazz_transactions key: tx_time (03 + 8 BE bytes), node alias (02 + 4).
    let transactions = compact_storage_raw_entries(temp_dir.path(), "jazz_transactions");
    assert_eq!(transactions.len(), 2);
    let tx_time_bytes = [&[0x03][..], &tx_id.time.0.to_be_bytes()].concat();
    let (key, _) = transactions
        .iter()
        .find(|(key, _)| key.starts_with(&tx_time_bytes))
        .expect("the first transaction is stored");
    assert_eq!(key.len(), 1 + 8 + 1 + 4);
    assert_eq!(key[9..], alias_bytes[..]);

    // History holds the superseded first image; current holds the second.
    let history = compact_storage_raw_entries(temp_dir.path(), &history_name);
    assert_eq!(history.len(), 1);
    let (key, value) = &history[0];
    assert!(key.ends_with(&alias_bytes), "history keys end with the 4-byte alias");
    assert_eq!(
        compact_storage_field_bytes(&history_table, value, "tx_node_id"),
        alias.0.to_le_bytes()
    );
    for field in ["created_at", "updated_at"] {
        assert_eq!(compact_storage_field_bytes(&history_table, value, field), ms_bytes);
    }

    let global = compact_storage_raw_entries(temp_dir.path(), &global_name);
    assert_eq!(global.len(), 1);
    let (_, value) = &global[0];
    assert_eq!(
        compact_storage_field_bytes(&global_table, value, "tx_node_id"),
        alias.0.to_le_bytes()
    );
    for field in ["created_at", "updated_at"] {
        assert_eq!(compact_storage_field_bytes(&global_table, value, field), ms_bytes);
    }

    // Reads above the storage layer see the same milliseconds as U64 and the
    // same transaction id.
    let mut reopened = reopen_node_at(&temp_dir, node(1), schema());
    let versions = reopened.query_table_versions("todos").unwrap();
    let tx_ids = versions
        .iter()
        .map(|version| reopened.version_tx_id(version).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(tx_ids, [tx_id, latest]);
    for version in &versions {
        assert_eq!(version.created_at().physical_ms(), now_ms);
        assert_eq!(version.updated_at().physical_ms(), now_ms);
    }
}

/// Contract: a history or global-current row whose provenance timestamp does
/// not fit the 6-byte slot is refused with a typed error, never truncated.
#[test]
fn provenance_timestamps_above_the_six_byte_slot_are_refused() {
    let (_temp_dir, mut state) = open_node();
    compact_storage_commit_accepted(&mut state, 1_000);
    compact_storage_supersede(&mut state, 1_000);
    let physical_table = state
        .physical_table_id_for_schema(state.catalogue.local_schema_version_id, "todos")
        .unwrap();
    for name in [
        physical_history_table_name(physical_table),
        physical_global_current_table_name(physical_table),
    ] {
        let stored = state.database.primary_key_scan_raw(&name, &[]).unwrap();
        assert_eq!(stored.len(), 1);
        let record = stored[0].record();
        let created_at = record.descriptor().field_index("created_at").unwrap();
        let mut values = record.to_values().unwrap();
        assert_eq!(values[created_at], Value::U64(1_000));
        values[created_at] = Value::U64(groove::records::U48_MAX + 1);
        let descriptor = record.descriptor();
        let raw = descriptor.create(&values).unwrap();
        let mut batch = state.database.open_batch();
        batch.insert(
            &name,
            groove::records::VariantRecord::new(
                stored[0].variant_tag(),
                groove::records::OwnedRecord::new(raw, descriptor),
            ),
        );
        let refused = crate::local_executor::block_on(state.database.apply_batch(batch));
        assert!(
            matches!(
                refused,
                Err(groove::db::Error::RecordEncoding(
                    groove::records::Error::U48OutOfRange(_)
                ))
            ),
            "{name}: {:?}",
            refused.err()
        );
        let after = state.database.primary_key_scan_raw(&name, &[]).unwrap();
        assert_eq!(
            after[0].record().get_u64(created_at).unwrap(),
            1_000,
            "{name}: the refused write left the stored row untouched"
        );
    }
}

/// Contract: alias allocation refuses with `NodeAliasExhausted` once
/// `u32::MAX` is allocated, and records no alias for the refused state.
#[test]
fn node_alias_allocation_refuses_after_u32_max() {
    let (_temp_dir, mut state) = open_node();
    // Plant the last alias directly: public APIs cannot allocate 2^32 nodes.
    let mut batch = state.database.open_batch();
    batch.insert(
        "jazz_nodes",
        vec![Value::U32(u32::MAX), Value::Uuid(node(0x7e).0)],
    );
    let applied = crate::local_executor::block_on(state.database.apply_batch(batch)).unwrap();
    let persisted = crate::local_executor::block_on(applied.persist());
    state.database.finish_persistence(persisted).unwrap();

    let newcomer = node(0x7f);
    assert!(matches!(
        crate::local_executor::block_on(state.ensure_node_alias(newcomer)),
        Err(Error::NodeAliasExhausted)
    ));
    assert!(!state.node_aliases.contains_key(&newcomer));
    assert_eq!(
        state.database.primary_key_scan_raw("jazz_nodes", &[]).unwrap().len(),
        2,
        "the refused node gets no durable alias"
    );
}
