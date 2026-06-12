use super::*;

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_recursive_inherits_cycle_does_not_overgrant() {
    let schema = recursive_folders_schema(Some(10));
    let client = crate::JazzClient::test_client(schema).await;

    let (a, _, _) = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "bob", "name" => "A", "parent_id" => Value::Null),
            None,
        )
        .expect("insert folder A");
    let (b, _, _) = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "carol", "name" => "B", "parent_id" => a),
            None,
        )
        .expect("insert folder B");

    // Close the cycle: A.parent_id = B
    client
        .for_session(Session::new("bob"))
        .update(a, vec![("parent_id".to_string(), Value::Uuid(b))])
        .expect("close folder cycle");

    let result_ids: HashSet<_> = client
        .for_session(Session::new("alice"))
        .query(QueryBuilder::new("folders").build(), None)
        .await
        .expect("query folders as alice")
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

    let a_policy = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("b_id"));
    });

    let b_policy = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("a_id"));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("table_a")
                .nullable_fk_column("b_id", "table_b")
                .policies(a_policy),
        )
        .table(
            TableSchema::builder("table_b")
                .nullable_fk_column("a_id", "table_a")
                .policies(b_policy),
        )
        .build();

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

    let folder_policy = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("parent_id"));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folder_policy),
        )
        .build();

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

    let folder_policy = permissions(|p| {
        p.allow_read()
            .where_(pe::allowed_to_read_with_depth("parent_id", 10));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folder_policy),
        )
        .build();

    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_ok(),
        "Bounded self-referential INHERITS should pass cycle validation: {:?}",
        result
    );
}
