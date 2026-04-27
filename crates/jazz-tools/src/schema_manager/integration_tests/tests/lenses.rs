use super::*;

#[test]
fn draft_lens_rejected() {
    let v1 = SchemaBuilder::new()
        .table(TableSchema::builder("users").column("id", ColumnType::Uuid))
        .build();

    // Add non-nullable UUID column - can't have sensible default
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("org_id", ColumnType::Uuid), // non-nullable, no default
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
    let result = manager.add_live_schema(v1);

    // Should fail - auto-generated lens is draft
    assert!(result.is_err());
}
