use super::*;

#[test]
fn rebac_select_policy_with_null_literal_filters_query_results() {
    use crate::query_manager::query::QueryBuilder;

    let mut schema = Schema::new();
    let documents_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("deleted_at", ColumnType::Text).nullable(),
    ]);
    let documents_policies =
        TablePolicies::new().with_select(PolicyExpr::eq_literal("deleted_at", Value::Null));
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(documents_descriptor, documents_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let visible_id = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("draft".into()), Value::Null],
        )
        .unwrap()
        .row_id;
    let hidden_id = qm
        .insert(
            &mut storage,
            "documents",
            &[
                Value::Text("soft-deleted".into()),
                Value::Text("2026-03-30T12:00:00Z".into()),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("documents").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.contains(&visible_id),
        "rows with deleted_at = NULL should remain visible"
    );
    assert!(
        !visible_ids.contains(&hidden_id),
        "rows with non-null deleted_at should be filtered out"
    );
}

#[test]
fn rebac_select_policy_with_is_null_filters_query_results() {
    use crate::query_manager::query::QueryBuilder;

    let mut schema = Schema::new();
    let documents_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("deleted_at", ColumnType::Text).nullable(),
    ]);
    let documents_policies = TablePolicies::new().with_select(PolicyExpr::IsNull {
        column: "deleted_at".into(),
    });
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(documents_descriptor, documents_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let visible_id = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("draft".into()), Value::Null],
        )
        .unwrap()
        .row_id;
    let hidden_id = qm
        .insert(
            &mut storage,
            "documents",
            &[
                Value::Text("soft-deleted".into()),
                Value::Text("2026-03-30T12:00:00Z".into()),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("documents").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.contains(&visible_id),
        "rows with deleted_at IS NULL should remain visible"
    );
    assert!(
        !visible_ids.contains(&hidden_id),
        "rows with non-null deleted_at should be filtered out by IS NULL"
    );
}
