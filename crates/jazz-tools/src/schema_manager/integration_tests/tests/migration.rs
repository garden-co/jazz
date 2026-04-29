use super::*;

#[test]
fn full_migration_add_column() {
    // Define schema versions
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

    // Create manager with v2 as current
    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();

    // Add v1 as live schema
    let lens = manager.add_live_schema(v1.clone()).unwrap();
    assert!(!lens.is_draft());

    // Verify branches
    let branches = manager.all_branch_strings();
    assert_eq!(branches.len(), 2);

    // Create a row in v1 format
    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let id = ObjectId::new();
    let v1_values = vec![Value::Uuid(id), Value::Text("Alice".to_string())];
    let v1_data = encode_row(&v1_table.columns, &v1_values).unwrap();

    // Transform to v2 using LensTransformer
    let v1_hash = SchemaHash::compute(&v1);
    let transformer = manager.transformer("users");
    let result = transformer
        .transform(&v1_data, make_commit_id(1), v1_hash)
        .unwrap();

    assert!(result.was_transformed);

    // Verify result can be decoded as v2
    let v2_descriptor = &manager
        .current_schema()
        .get(&TableName::new("users"))
        .unwrap()
        .columns;
    let v2_values = decode_row(v2_descriptor, &result.data).unwrap();

    assert_eq!(v2_values.len(), 3);
    assert_eq!(
        v2_values[v2_descriptor.column_index("id").unwrap()],
        Value::Uuid(id)
    );
    assert_eq!(
        v2_values[v2_descriptor.column_index("name").unwrap()],
        Value::Text("Alice".to_string())
    );
    assert_eq!(
        v2_values[v2_descriptor.column_index("email").unwrap()],
        Value::Null
    ); // Added column with default
}

#[test]
fn multi_table_migration() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Uuid)
                .fk_column("author_id", "users")
                .column("title", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Uuid)
                .fk_column("author_id", "users")
                .column("title", ColumnType::Text)
                .nullable_column("body", ColumnType::Text),
        )
        .build();

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema(v1.clone()).unwrap();

    // Transform user row
    let v1_hash = SchemaHash::compute(&v1);
    let v1_users = v1.get(&TableName::new("users")).unwrap();
    let user_id = ObjectId::new();
    let v1_user = vec![Value::Uuid(user_id), Value::Text("Alice".to_string())];
    let v1_user_data = encode_row(&v1_users.columns, &v1_user).unwrap();

    let user_transformer = manager.transformer("users");
    let user_result = user_transformer
        .transform(&v1_user_data, make_commit_id(1), v1_hash)
        .unwrap();
    assert!(user_result.was_transformed);

    // Transform post row
    let v1_posts = v1.get(&TableName::new("posts")).unwrap();
    let post_id = ObjectId::new();
    let v1_post = vec![
        Value::Uuid(post_id),
        Value::Uuid(user_id),
        Value::Text("Hello World".to_string()),
    ];
    let v1_post_data = encode_row(&v1_posts.columns, &v1_post).unwrap();

    let post_transformer = manager.transformer("posts");
    let post_result = post_transformer
        .transform(&v1_post_data, make_commit_id(2), v1_hash)
        .unwrap();
    assert!(post_result.was_transformed);

    // Verify post has new body column
    let v2_descriptor = &manager
        .current_schema()
        .get(&TableName::new("posts"))
        .unwrap()
        .columns;
    let v2_post_values = decode_row(v2_descriptor, &post_result.data).unwrap();
    assert_eq!(v2_post_values.len(), 4);
    assert_eq!(
        v2_post_values[v2_descriptor.column_index("body").unwrap()],
        Value::Null
    ); // body column
}

#[test]
fn end_to_end_lens_transform_on_query() {
    // Schema v1: users(id, name)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    // Schema v2: users(id, name, email) - added nullable email
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
    let lens = generate_lens(&v1, &v2);

    // Create QueryManager with new API
    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);
    let mut storage = MemoryStorage::new();

    // Get branch names
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    // --- Ingest a synced row on the OLD schema branch (v1 format: id, name only) ---
    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let old_row_id = ObjectId::new();
    let old_row_values = vec![Value::Uuid(old_row_id), Value::Text("Alice".to_string())];
    let old_row_data = encode_row(&v1_table.columns, &old_row_values).unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        old_row_id,
        &v1_branch,
        old_row_data,
        1_000,
    );

    // --- Insert a row on the NEW schema branch (v2 format: id, name, email) ---
    let new_user_id = ObjectId::new();
    qm.insert(
        &mut storage,
        "users",
        &[
            Value::Uuid(new_user_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ],
    )
    .unwrap();

    // --- Query across both branches ---
    let query = QueryBuilder::new("users")
        .branches(&[&v1_branch, &v2_branch])
        .build();

    // Subscribe and process to settle the graph.
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Get results and clean up subscription.
    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);

    // Should have 2 rows
    assert_eq!(
        results.len(),
        2,
        "Expected 2 rows from both schema branches"
    );

    // Find Alice's row (from v1 branch) - should have 3 columns after transform
    let alice_row = results
        .iter()
        .find(|(_, row)| {
            row.iter()
                .any(|v| matches!(v, Value::Text(s) if s == "Alice"))
        })
        .expect("Alice's row should be present");

    // Alice's row should have been transformed to v2 format (3 columns)
    assert_eq!(
        alice_row.1.len(),
        3,
        "Alice's row should have 3 columns after lens transform"
    );
    assert_eq!(alice_row.1[0], Value::Uuid(old_row_id));
    assert_eq!(alice_row.1[1], Value::Text("Alice".to_string()));
    assert_eq!(
        alice_row.1[2],
        Value::Null,
        "Added email column should be Null"
    );

    // Find Bob's row (from v2 branch) - already in v2 format
    let bob_row = results
        .iter()
        .find(|(_, row)| {
            row.iter()
                .any(|v| matches!(v, Value::Text(s) if s == "Bob"))
        })
        .expect("Bob's row should be present");

    assert_eq!(bob_row.1.len(), 3);
    assert_eq!(bob_row.1[0], Value::Uuid(new_user_id));
    assert_eq!(bob_row.1[1], Value::Text("Bob".to_string()));
    assert_eq!(bob_row.1[2], Value::Text("bob@example.com".to_string()));
}

#[test]
fn end_to_end_multi_hop_transform() {
    // Schema v1: users(id, name)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build();

    // Schema v2: users(id, name, email) - added email
    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build();

    // Schema v3: users(id, name, email, role) - added role
    let v3 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text)
                .nullable_column("role", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);
    let v3_hash = SchemaHash::compute(&v3);

    let lens_v1_v2 = generate_lens(&v1, &v2);
    let lens_v2_v3 = generate_lens(&v2, &v3);

    // Create QueryManager with new API
    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v3.clone(), "dev", "main");
    qm.add_live_schema(v2.clone());
    qm.register_lens(lens_v2_v3);
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens_v1_v2);

    // Get branch names
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());
    let v3_branch = format!("dev-{}-main", v3_hash.short());
    let mut storage = MemoryStorage::new();

    // --- Ingest row on v1 branch (oldest schema) ---
    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let row1_id = ObjectId::new();
    let row1_values = vec![Value::Uuid(row1_id), Value::Text("Alice".to_string())];
    let row1_data = encode_row(&v1_table.columns, &row1_values).unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        row1_id,
        &v1_branch,
        row1_data,
        1_000,
    );

    // --- Ingest row on v2 branch (middle schema) ---
    let v2_table = v2.get(&TableName::new("users")).unwrap();
    let row2_id = ObjectId::new();
    let row2_values = vec![
        Value::Uuid(row2_id),
        Value::Text("Bob".to_string()),
        Value::Text("bob@example.com".to_string()),
    ];
    let row2_data = encode_row(&v2_table.columns, &row2_values).unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v2_hash,
        row2_id,
        &v2_branch,
        row2_data,
        1_100,
    );

    // --- Insert row on v3 branch (current schema) ---
    let row3_id = ObjectId::new();
    qm.insert(
        &mut storage,
        "users",
        &[
            Value::Uuid(row3_id),
            Value::Text("Charlie".to_string()),
            Value::Text("charlie@example.com".to_string()),
            Value::Text("admin".to_string()),
        ],
    )
    .unwrap();

    // --- Query across all three branches ---
    let query = QueryBuilder::new("users")
        .branches(&[&v1_branch, &v2_branch, &v3_branch])
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);

    // Should have 3 rows
    assert_eq!(results.len(), 3, "Expected 3 rows from all schema branches");

    // All rows should have 4 columns (v3 format)
    for (_, row) in &results {
        assert_eq!(
            row.len(),
            4,
            "All rows should be transformed to v3 format (4 columns)"
        );
    }

    // Find Alice's row (from v1 branch - 2 hop transform)
    let alice_row = results
        .iter()
        .find(|(_, row)| {
            row.iter()
                .any(|v| matches!(v, Value::Text(s) if s == "Alice"))
        })
        .expect("Alice's row should be present");
    assert_eq!(alice_row.1[0], Value::Uuid(row1_id));
    assert_eq!(alice_row.1[1], Value::Text("Alice".to_string()));
    assert_eq!(alice_row.1[2], Value::Null); // email added in v1->v2
    assert_eq!(alice_row.1[3], Value::Null); // role added in v2->v3

    // Find Bob's row (from v2 branch - 1 hop transform)
    let bob_row = results
        .iter()
        .find(|(_, row)| {
            row.iter()
                .any(|v| matches!(v, Value::Text(s) if s == "Bob"))
        })
        .expect("Bob's row should be present");
    assert_eq!(bob_row.1[0], Value::Uuid(row2_id));
    assert_eq!(bob_row.1[1], Value::Text("Bob".to_string()));
    assert_eq!(bob_row.1[2], Value::Text("bob@example.com".to_string())); // preserved
    assert_eq!(bob_row.1[3], Value::Null); // role added in v2->v3

    // Find Charlie's row (from v3 branch - no transform)
    let charlie_row = results
        .iter()
        .find(|(_, row)| {
            row.iter()
                .any(|v| matches!(v, Value::Text(s) if s == "Charlie"))
        })
        .expect("Charlie's row should be present");
    assert_eq!(charlie_row.1[0], Value::Uuid(row3_id));
    assert_eq!(charlie_row.1[1], Value::Text("Charlie".to_string()));
    assert_eq!(
        charlie_row.1[2],
        Value::Text("charlie@example.com".to_string())
    );
    assert_eq!(charlie_row.1[3], Value::Text("admin".to_string()));
}

/// Regression: every MaterializeNode the schema-aware compiler emits must carry
/// the actual table name in each output-descriptor element. The materialize
/// hint propagates to the row loader and ultimately to `LensTransformer::new`;
/// an empty hint causes `translate_table_name_to_schema(ctx, "", &source_hash)`
/// to fail with `TableNotFound("")`, silently dropping every old-branch row
/// after a schema migration on storage backends that resolve rows by locator
/// rather than by table key.
#[test]
fn materialize_nodes_carry_concrete_table_name() {
    use crate::query_manager::graph::GraphNode;

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

    let lens = generate_lens(&v1, &v2);
    let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
    ctx.add_live_schema(v1.clone(), lens);

    let query = QueryBuilder::new("users").build();
    let graph = QueryGraph::compile_with_schema_context(&query, &v2, None, &ctx)
        .expect("query graph compile should succeed");

    let mut materialize_count = 0;
    for compact in &graph.nodes {
        let GraphNode::Materialize(node) = &compact.node else {
            continue;
        };
        materialize_count += 1;
        let desc = node.output_tuple_descriptor();
        for index in 0..desc.element_count() {
            let element = desc.element(index).expect("element index in range");
            assert!(
                !element.table.as_str().is_empty(),
                "MaterializeNode element {index} has empty table; \
                 row_loader will pass an empty table_hint to LensTransformer \
                 and TableNotFound(\"\") will silently drop old-branch rows",
            );
            assert_eq!(
                element.table.as_str(),
                "users",
                "MaterializeNode element {index} should carry the queried table name",
            );
        }
    }
    assert!(
        materialize_count > 0,
        "expected at least one MaterializeNode in the compiled graph"
    );
}

/// End-to-end regression: with a storage backend that resolves rows by
/// locator (matching `RocksDBStorage`'s production behaviour, but mocked
/// here via `LocatorOnlyStorage` so the test stays in-process and doesn't
/// pull in native deps), an old-schema row must still project through the
/// lens into the current schema and appear in subscription results. Before
/// the fix, the empty `element.table` on the materialize node propagated
/// to `LensTransformer::new(ctx, "")`, the transform failed with
/// `TableNotFound("")`, and the row was silently dropped — exactly the
/// user-visible "data disappears after a schema migration" symptom.
#[test]
fn locator_only_storage_returns_old_branch_rows_through_lens() {
    use super::locator_only_storage::LocatorOnlyStorage;
    use crate::metadata::MetadataKey;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};
    use crate::test_row_history::put_test_row_metadata;

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
    let lens = generate_lens(&v1, &v2);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);

    let mut storage = LocatorOnlyStorage::new();

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    // Ingest Alice as a v1 row through the sync inbox. With
    // LocatorOnlyStorage, `load_visible_query_row("", branch, row_id)` will
    // find this row by locator — so the empty table hint propagates all the
    // way to LensTransformer (this is the path that's broken in production
    // and that MemoryStorage masks).
    let v1_users = v1.get(&TableName::new("users")).unwrap();
    let alice_id = ObjectId::new();
    let alice_v1_data = encode_row(
        &v1_users.columns,
        &[Value::Uuid(alice_id), Value::Text("Alice".to_string())],
    )
    .unwrap();
    let mut alice_metadata = HashMap::new();
    alice_metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    alice_metadata.insert(
        MetadataKey::OriginSchemaHash.to_string(),
        v1_hash.to_string(),
    );
    put_test_row_metadata(&mut storage, alice_id, alice_metadata);
    let alice_commit = stored_row_commit(alice_v1_data, 1_000, alice_id.to_string());
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: alice_commit.to_row(alice_id, &v1_branch),
        },
    });

    let query = QueryBuilder::new("users")
        .branches(&[&v1_branch, &v2_branch])
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);

    assert_eq!(
        results.len(),
        1,
        "Alice's old-schema row should project through the lens, \
         not be silently dropped by TableNotFound(\"\")"
    );
    let alice = &results[0];
    assert_eq!(alice.0, alice_id);
    assert_eq!(
        alice.1.len(),
        3,
        "row should be transformed to v2 (3 columns)"
    );
    assert_eq!(alice.1[0], Value::Uuid(alice_id));
    assert_eq!(alice.1[1], Value::Text("Alice".to_string()));
    assert_eq!(
        alice.1[2],
        Value::Null,
        "added email column should be Null after lens transform"
    );
}
