use super::*;

#[test]
fn context_validation() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();
    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema(v1).unwrap();

    // Live context should include both current and previous schema hashes.
    let live_hashes = manager.all_live_hashes();
    assert_eq!(live_hashes.len(), 2);
    assert!(live_hashes.contains(&v1_hash));
    assert!(live_hashes.contains(&v2_hash));

    // v1 should have a single-step lens path to current v2.
    let path_from_v1 = manager
        .lens_path(&v1_hash)
        .expect("v1 should have a reachable lens path to current schema");
    assert_eq!(path_from_v1.len(), 1);

    // Validation should pass - no draft lenses.
    manager
        .validate()
        .expect("Schema context should validate with fully connected live schemas");
}
