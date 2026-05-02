use super::*;

#[test]
fn direct_query_manager_bootstrap_persists_canonical_schema_bytes_for_flat_row_storage() {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("id", ColumnType::Uuid),
        ])),
    );
    let schema_hash = crate::query_manager::types::SchemaHash::compute(&schema);

    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(schema.clone(), "dev", "main");

    let mut storage = MemoryStorage::new();
    qm.ensure_known_schemas_catalogued(&mut storage)
        .expect("schema bootstrap should succeed");

    let schema_entry = storage
        .load_catalogue_entry(schema_hash.to_object_id())
        .expect("catalogue lookup should succeed")
        .expect("schema should be catalogued");
    assert_eq!(
        schema_entry.content,
        encode_schema(&schema),
        "direct QueryManager bootstrapping should persist canonical schema bytes"
    );

    let descriptor = qm.schema_context().current_schema[&TableName::new("users")]
        .columns
        .clone();
    let values = descriptor
        .columns
        .iter()
        .map(|column| match column.name.as_str() {
            "id" => Value::Uuid(ObjectId::new()),
            "name" => Value::Text("Alice".into()),
            other => panic!("unexpected column {other}"),
        })
        .collect::<Vec<_>>();
    let inserted = qm
        .insert(&mut storage, "users", &values)
        .expect("insert should succeed");

    let history_bytes = storage
        .scan_history_region_bytes("users", HistoryScan::Branch)
        .expect("history bytes should be readable");
    assert_eq!(history_bytes.len(), 1);
    assert_eq!(
        storage
            .scan_history_row_batches("users", inserted.row_id)
            .expect("flat history rows should decode with keyed storage context")
            .len(),
        history_bytes.len(),
        "direct QueryManager writes should persist keyed-decodable flat history rows after schema bootstrap"
    );
}

#[test]
fn direct_query_manager_catalogues_known_schemas_only_once_per_storage() {
    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(test_schema(), "dev", "main");
    let mut storage = CountingCatalogueUpsertsStorage::new();

    let first = vec![Value::Text("Alice".into()), Value::Integer(1)];
    qm.insert(&mut storage, "users", &first)
        .expect("first insert should succeed");
    let first_upserts = storage.catalogue_upserts();
    assert!(
        first_upserts >= 1,
        "first insert should catalogue the current schema"
    );

    let second = vec![Value::Text("Bob".into()), Value::Integer(2)];
    qm.insert(&mut storage, "users", &second)
        .expect("second insert should succeed");

    assert_eq!(
        storage.catalogue_upserts(),
        first_upserts,
        "ordinary writes should not recatalogue unchanged schemas on the same storage",
    );
}

#[test]
fn direct_query_manager_insert_uses_prepared_write_context_without_catalogue_loads() {
    let mut qm = QueryManager::new(SyncManager::new());
    qm.set_current_schema(test_schema(), "dev", "main");
    let mut storage = CountingCatalogueUpsertsStorage::new();
    qm.ensure_known_schemas_catalogued(&mut storage)
        .expect("schema bootstrap should succeed");
    storage.reset_catalogue_loads();

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(1)],
    )
    .expect("insert should succeed");

    assert_eq!(
        storage.catalogue_loads(),
        0,
        "prepared local inserts already have the table descriptor and should not reload catalogue descriptors during history apply",
    );
}
