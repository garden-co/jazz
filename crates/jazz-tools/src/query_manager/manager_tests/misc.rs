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
