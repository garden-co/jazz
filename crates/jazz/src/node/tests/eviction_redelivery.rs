// Internal because the public client does not expose the exact cache-eviction
// boundary. Real writer/Core/reader nodes use RocksDB and public schema builders.

/// Alice's accepted row reaches Bob, is evicted locally, then is requested from
/// Core again. A newly generated authority reset must restore the original body.
/// Alice -> Core -> Bob; Bob evicts -> fresh Core reset -> Bob.
#[test]
#[ignore = "#2960: post-eviction authority redelivery is rejected as a conflicting transaction"]
fn authority_redelivery_repairs_manually_evicted_complete_transaction_body() {
    assert_authority_redelivery_repairs_evicted_body(false);
}

/// Alice's accepted body is removed by an explicit zero-byte cache budget;
/// Bob must recover it from a freshly generated Core response.
#[test]
#[ignore = "#2960: post-eviction authority redelivery is rejected as a conflicting transaction"]
fn authority_redelivery_repairs_budget_evicted_complete_transaction_body() {
    assert_authority_redelivery_repairs_evicted_body(true);
}

fn assert_authority_redelivery_repairs_evicted_body(budgeted: bool) {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let (shape, binding) = core.whole_table_shape_binding("todos").unwrap();
    let subscription = core.whole_table_subscription_key("todos").unwrap();
    let row_uuid = row(0x71);
    let cells: BTreeMap<String, Value> = crate::row_input! { "title" => "Alice's original" }
        .into_iter()
        .map(|(name, value)| match value {
            PublicValue::Text(text) => (name, Value::String(text)),
            other => panic!("unexpected fixture value: {other:?}"),
        })
        .collect();
    commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", row_uuid, 19).cells(cells),
    );
    register_shape_binding(&mut reader, &shape, &binding);
    let update = system_authority_reset(&mut core, &shape, &binding, subscription);
    reader.apply_sync_message_settled(update).unwrap();
    let original = reader.row_history("todos", row_uuid).unwrap();
    assert_eq!(original.len(), 1);
    let key = reader
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let generation = reader.applied_authority_result_generation(&key);
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
    let repair = system_authority_reset(&mut core, &shape, &binding, subscription);
    reader.apply_sync_message_settled(repair).unwrap();
    assert_eq!(reader.row_history("todos", row_uuid).unwrap(), original);
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
