use super::*;

#[test]
fn rebac_recursive_inherits_cycle_does_not_overgrant() {
    use crate::query_manager::query::QueryBuilder;

    let schema = recursive_folders_schema(Some(10));
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let a = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("A".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let b = qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("carol".into()),
                Value::Text("B".into()),
                Value::Uuid(a),
            ],
        )
        .unwrap()
        .row_id;

    // Close the cycle: A.parent_id = B
    let _ = qm
        .update(
            &mut storage,
            a,
            &[
                Value::Text("bob".into()),
                Value::Text("A".into()),
                Value::Uuid(b),
            ],
        )
        .unwrap();

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("folders").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let result_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();

    assert!(
        result_ids.is_empty(),
        "Cycle should not grant access when no ancestor is owned by session user"
    );
}

#[test]
fn rebac_inherits_cycle_detection() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Table A references B via INHERITS
    let a_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("b_id", ColumnType::Uuid)
            .nullable()
            .references("table_b"),
    ]);
    let a_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "b_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("table_a"),
        TableSchema::with_policies(a_desc, a_policy),
    );

    // Table B references A via INHERITS (creates cycle!)
    let b_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("a_id", ColumnType::Uuid)
            .nullable()
            .references("table_a"),
    ]);
    let b_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "a_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("table_b"),
        TableSchema::with_policies(b_desc, b_policy),
    );

    // Should fail validation with cycle detected
    let result = validate_no_inherits_cycles(&schema);
    assert!(result.is_err(), "Should detect INHERITS cycle: A → B → A");
    let err = result.unwrap_err();
    assert!(
        err.contains("cycle"),
        "Error message should mention cycle: {}",
        err
    );
}

#[test]
fn rebac_inherits_self_reference_detection() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    // Folder table with parent_id referencing itself
    let folder_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folder_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "parent_id".into(),
        max_depth: None,
    });
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folder_desc, folder_policy),
    );

    // Should fail validation - self-reference is a cycle of length 1
    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_err(),
        "Should detect INHERITS self-reference cycle: folders → folders"
    );
    let err = result.unwrap_err();
    assert!(
        err.contains("cycle"),
        "Error message should mention cycle: {}",
        err
    );
}

#[test]
fn rebac_inherits_bounded_self_reference_passes_validation() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let mut schema = Schema::new();

    let folder_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("parent_id", ColumnType::Uuid)
            .nullable()
            .references("folders"),
    ]);
    let folder_policy = TablePolicies::new().with_select(PolicyExpr::Inherits {
        operation: Operation::Select,
        via_column: "parent_id".into(),
        max_depth: Some(10),
    });
    schema.insert(
        TableName::new("folders"),
        TableSchema::with_policies(folder_desc, folder_policy),
    );

    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_ok(),
        "Bounded self-referential INHERITS should pass cycle validation: {:?}",
        result
    );
}
