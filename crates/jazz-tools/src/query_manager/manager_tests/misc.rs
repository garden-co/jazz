use super::*;

#[test]
fn table_not_found_error() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let result = qm.insert(&mut storage, "nonexistent", &[Value::Text("test".into())]);
    match result {
        Err(QueryError::TableNotFound(table)) => {
            assert_eq!(table, TableName::new("nonexistent"));
        }
        other => panic!("Expected TableNotFound(nonexistent), got {other:?}"),
    }
}

#[test]
fn column_count_mismatch_error() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let result = qm.insert(&mut storage, "users", &[Value::Text("Alice".into())]);
    match result {
        Err(QueryError::ColumnCountMismatch { expected, actual }) => {
            assert_eq!(expected, 2, "users table has two columns in test_schema()");
            assert_eq!(actual, 1, "insert call provided one value");
        }
        other => panic!("Expected ColumnCountMismatch, got {other:?}"),
    }
}

#[test]
#[should_panic(expected = "invalid composite index schema")]
fn set_current_schema_rejects_invalid_composite_index_namespace() {
    use crate::query_manager::types::{ColumnName, CompositeIndex, CompositeIndexColumn};

    let mut schema = Schema::new();
    let mut documents = TableSchema::builder("documents")
        .column("owner_id", ColumnType::Text)
        .column("updated_at", ColumnType::Timestamp)
        .build();
    documents.composite_indexes = vec![CompositeIndex {
        name: ColumnName::new("owner_id"),
        columns: vec![CompositeIndexColumn::asc("updated_at")],
    }];
    schema.insert(TableName::new("documents"), documents);

    QueryManager::new(SyncManager::new()).set_current_schema(schema, "dev", "main");
}

#[test]
fn insert_index_mutations_include_system_timestamp_indexes() {
    let descriptor = RowDescriptor::new(vec![ColumnDescriptor::new("title", ColumnType::Text)]);
    let data = encode_row(&descriptor, &[Value::Text("Launch notes".into())]).unwrap();
    let row_id = ObjectId::new();
    let provenance = RowProvenance::for_insert("alice", 123);

    let mutations = QueryManager::index_mutations_for_insert_on_branch(
        "documents",
        "main",
        row_id,
        &data,
        &provenance,
        &descriptor,
        None,
        &[],
    );

    assert!(mutations.iter().any(|mutation| {
        matches!(
            mutation,
            IndexMutation::Insert {
                column: "$createdAt",
                value: Value::Timestamp(123),
                row_id: indexed_row_id,
                ..
            } if *indexed_row_id == row_id
        )
    }));
    assert!(mutations.iter().any(|mutation| {
        matches!(
            mutation,
            IndexMutation::Insert {
                column: "$updatedAt",
                value: Value::Timestamp(123),
                row_id: indexed_row_id,
                ..
            } if *indexed_row_id == row_id
        )
    }));
}

#[test]
fn query_can_order_by_system_updated_at_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    for (name, score) in [("old", 1), ("new", 2), ("middle", 3)] {
        qm.insert(
            &mut storage,
            "users",
            &[Value::Text(name.into()), Value::Integer(score)],
        )
        .unwrap();
    }

    let query = qm
        .query("users")
        .select(&["name", "$updatedAt"])
        .order_by_desc("$updatedAt")
        .limit(2)
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    let rows = results
        .into_iter()
        .map(|(_, values)| (values[0].clone(), values[1].clone()))
        .collect::<Vec<_>>();
    assert_eq!(
        rows.iter()
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>(),
        vec![Value::Text("middle".into()), Value::Text("new".into())]
    );
    assert!(matches!(
        (&rows[0].1, &rows[1].1),
        (Value::Timestamp(left), Value::Timestamp(right)) if left >= right
    ));
}
