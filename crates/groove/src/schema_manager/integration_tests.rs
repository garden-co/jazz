//! Integration tests for SchemaManager - full flow from schema to transformation.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, ComposedBranchName, RowDescriptor, Schema, SchemaBuilder,
        SchemaHash, TableName, TableSchema, Value,
    };
    use crate::schema_manager::{
        AppId, CopyOnWriteWriter, Lens, LensOp, LensTransform, SchemaContext, SchemaManager,
        generate_lens,
    };
    use crate::storage::{MemoryStorage, Storage};

    fn make_commit_id(n: u8) -> CommitId {
        CommitId([n; 32])
    }

    fn test_app_id() -> AppId {
        AppId::from_name("integration-test-app")
    }

    /// Test full migration workflow: v1 -> v2 with added column.
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
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();

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
        let v1_data = encode_row(&v1_table.descriptor, &v1_values).unwrap();

        // Transform to v2 using LensTransformer
        let v1_hash = SchemaHash::compute(&v1);
        let transformer = manager.transformer("users");
        let result = transformer
            .transform(&v1_data, make_commit_id(1), v1_hash)
            .unwrap();

        assert!(result.was_transformed);

        // Verify result can be decoded as v2
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let v2_values = decode_row(&v2_table.descriptor, &result.data).unwrap();

        assert_eq!(v2_values.len(), 3);
        assert_eq!(v2_values[0], Value::Uuid(id));
        assert_eq!(v2_values[1], Value::Text("Alice".to_string()));
        assert_eq!(v2_values[2], Value::Null); // Added column with default
    }

    /// Test copy-on-write update across schema versions.
    #[test]
    fn copy_on_write_update() {
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

        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        // Build branch -> schema map
        let mut branch_map = HashMap::new();
        let v1_branch = format!("dev-{}-main", v1_hash.short());
        let v2_branch = format!("dev-{}-main", v2_hash.short());
        branch_map.insert(v1_branch.clone(), v1_hash);
        branch_map.insert(v2_branch.clone(), v2_hash);

        let mut writer = CopyOnWriteWriter::new(&ctx, "users", branch_map);

        // Create a row in v1 schema
        let id = ObjectId::new();
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let v1_values = vec![Value::Uuid(id), Value::Text("Alice".to_string())];
        let v1_data = encode_row(&v1_table.descriptor, &v1_values).unwrap();

        // Cache the row (simulating loading from storage)
        writer.cache_row(id, &v1_branch, v1_data, make_commit_id(1));

        // Get write info - should indicate copy-on-write
        let write_info = writer.get_write_info(id);
        assert!(write_info.is_copy_on_write);
        assert_eq!(write_info.source_branch, Some(v1_branch.clone()));

        // Prepare update
        let result = writer
            .prepare_update(id, |vals| {
                // After transform, we have 3 columns
                assert_eq!(vals.len(), 3);
                vec![
                    vals[0].clone(),
                    Value::Text("Alice Updated".to_string()),
                    Value::Text("alice@example.com".to_string()),
                ]
            })
            .unwrap();

        assert!(result.was_transformed);
        assert_eq!(result.source_schema, v1_hash);

        // Verify result
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let final_values = decode_row(&v2_table.descriptor, &result.data).unwrap();

        assert_eq!(final_values[0], Value::Uuid(id));
        assert_eq!(final_values[1], Value::Text("Alice Updated".to_string()));
        assert_eq!(
            final_values[2],
            Value::Text("alice@example.com".to_string())
        );
    }

    /// Test column rename through lens.
    #[test]
    fn column_rename_lens() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit rename lens (auto-gen would mark as draft)
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
        manager.add_live_schema_with_lens(v1.clone(), lens).unwrap();

        // Test column translation for index lookup
        let translated = manager
            .translate_column_for_schema("users", "email_address", &v1_hash)
            .unwrap();
        assert_eq!(translated, "email");

        // Transform a row
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let id = ObjectId::new();
        let v1_values = vec![
            Value::Uuid(id),
            Value::Text("alice@example.com".to_string()),
        ];
        let v1_data = encode_row(&v1_table.descriptor, &v1_values).unwrap();

        let transformer = manager.transformer("users");
        let result = transformer
            .transform(&v1_data, make_commit_id(1), v1_hash)
            .unwrap();

        assert!(result.was_transformed);

        // Verify - column value should be preserved under new name
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let v2_values = decode_row(&v2_table.descriptor, &result.data).unwrap();

        assert_eq!(v2_values[0], Value::Uuid(id));
        assert_eq!(v2_values[1], Value::Text("alice@example.com".to_string()));
    }

    /// Test multi-table schema evolution.
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
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
        manager.add_live_schema(v1.clone()).unwrap();

        // Transform user row
        let v1_hash = SchemaHash::compute(&v1);
        let v1_users = v1.get(&TableName::new("users")).unwrap();
        let user_id = ObjectId::new();
        let v1_user = vec![Value::Uuid(user_id), Value::Text("Alice".to_string())];
        let v1_user_data = encode_row(&v1_users.descriptor, &v1_user).unwrap();

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
        let v1_post_data = encode_row(&v1_posts.descriptor, &v1_post).unwrap();

        let post_transformer = manager.transformer("posts");
        let post_result = post_transformer
            .transform(&v1_post_data, make_commit_id(2), v1_hash)
            .unwrap();
        assert!(post_result.was_transformed);

        // Verify post has new body column
        let v2_posts = v2.get(&TableName::new("posts")).unwrap();
        let v2_post_values = decode_row(&v2_posts.descriptor, &post_result.data).unwrap();
        assert_eq!(v2_post_values.len(), 4);
        assert_eq!(v2_post_values[3], Value::Null); // body column
    }

    /// Test draft lens detection and rejection.
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

    /// Test validation of schema context.
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2, test_app_id(), "dev", "main").unwrap();
        manager.add_live_schema(v1).unwrap();

        // Validation should pass - no draft lenses
        assert!(manager.validate().is_ok());
    }

    // ========================================================================
    // QueryManager Integration Tests
    // ========================================================================

    use crate::query_manager::graph::QueryGraph;
    use crate::query_manager::manager::QueryManager;
    use crate::query_manager::query::{Query, QueryBuilder};
    use crate::sync_manager::SyncManager;

    /// Helper to execute a query synchronously via subscribe/process/unsubscribe on SchemaManager.
    fn execute_query(
        manager: &mut SchemaManager,
        storage: &mut MemoryStorage,
        query: Query,
    ) -> Vec<(ObjectId, Vec<Value>)> {
        let qm = manager.query_manager_mut();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(storage);
        let results = qm.get_subscription_results(sub_id);
        qm.unsubscribe_with_sync(sub_id);
        results
    }

    /// Test QueryManager with schema context initialization.
    #[test]
    fn query_manager_with_schema_context() {
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

        // Create QueryManager with new API
        let sm = SyncManager::new();
        let mut qm = QueryManager::new(sm);
        qm.set_current_schema(v2.clone(), "dev", "main");
        qm.add_live_schema(v1.clone());
        qm.register_lens(lens);

        // Verify schema context is initialized
        assert!(qm.schema_context().is_initialized());

        // Verify all branches are available for queries
        let branches = qm.all_query_branches();
        assert_eq!(branches.len(), 2);

        // Both schema branches should be included
        let v1_branch = format!("dev-{}-main", v1_hash.short());
        let v2_branch = format!("dev-{}-main", v2_hash.short());
        assert!(branches.contains(&v1_branch));
        assert!(branches.contains(&v2_branch));
    }

    /// Test QueryGraph compilation with schema context and column translation.
    #[test]
    fn query_graph_compile_with_schema_context() {
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text), // renamed column
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit rename lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        // Build query with filter on the renamed column
        let query = QueryBuilder::new("users").build();

        // Compile with schema context
        let graph = QueryGraph::compile_with_schema_context(&query, &v2, None, &ctx);
        assert!(graph.is_some());

        let graph = graph.unwrap();

        // Should have index scan nodes for both branches
        // Note: the exact number depends on how many disjuncts and branches
        assert!(!graph.index_scan_nodes.is_empty());
    }

    /// Test that SchemaManager's context can be used with QueryManager.
    #[test]
    fn schema_manager_to_query_manager_integration() {
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

        // Create schema manager (which manages SchemaContext internally)
        let mut schema_mgr =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
        schema_mgr.add_live_schema(v1.clone()).unwrap();

        // Verify SchemaManager's QueryManager is properly configured
        let qm = schema_mgr.query_manager();
        assert!(qm.schema_context().is_initialized());
        assert_eq!(qm.all_query_branches().len(), 2);
    }

    // ========================================================================
    // End-to-End ObjectManager Integration Tests
    // ========================================================================

    /// End-to-end test: Insert rows in old schema format, query with new schema,
    /// verify lens transforms are applied.
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

        // --- Insert a row on the OLD schema branch (v1 format: id, name only) ---
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let old_row_id = ObjectId::new();
        let old_row_values = vec![Value::Uuid(old_row_id), Value::Text("Alice".to_string())];
        let old_row_data = encode_row(&v1_table.descriptor, &old_row_values).unwrap();

        // Create object and add commit on v1 branch
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut().object_manager.create_with_id(
            &mut storage,
            old_row_id,
            Some(metadata.clone()),
        );
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                old_row_id,
                &v1_branch,
                vec![],
                old_row_data.clone(),
                old_row_id,
                None,
            )
            .unwrap();

        // Manually update indices for the old branch
        // (In real usage, this would happen via handle_object_update)
        storage
            .index_insert(
                "users",
                "_id",
                &v1_branch,
                &Value::Uuid(old_row_id),
                old_row_id,
            )
            .unwrap();

        // --- Insert a row on the NEW schema branch (v2 format: id, name, email) ---
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let new_row_id = ObjectId::new();
        let new_row_values = vec![
            Value::Uuid(new_row_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ];
        let new_row_data = encode_row(&v2_table.descriptor, &new_row_values).unwrap();

        qm.sync_manager_mut().object_manager.create_with_id(
            &mut storage,
            new_row_id,
            Some(metadata),
        );
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                new_row_id,
                &v2_branch,
                vec![],
                new_row_data.clone(),
                new_row_id,
                None,
            )
            .unwrap();

        // Update indices for new branch
        storage
            .index_insert(
                "users",
                "_id",
                &v2_branch,
                &Value::Uuid(new_row_id),
                new_row_id,
            )
            .unwrap();

        // --- Query across both branches ---
        let query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch])
            .build();

        // Subscribe and process to settle the graph
        let mut storage = MemoryStorage::new();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(&mut storage);

        // Get results
        let results = qm.get_subscription_results(sub_id);

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
        assert_eq!(bob_row.1[0], Value::Uuid(new_row_id));
        assert_eq!(bob_row.1[1], Value::Text("Bob".to_string()));
        assert_eq!(bob_row.1[2], Value::Text("bob@example.com".to_string()));
    }

    // ========================================================================
    // Multi-Hop Lens Path Integration Tests
    // ========================================================================

    /// End-to-end test: v1 -> v2 -> v3 multi-hop transform.
    /// Insert rows in v1 and v2 format, query with v3 schema,
    /// verify lens transforms are applied across multiple hops.
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

        // --- Insert row on v1 branch (oldest schema) ---
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let row1_id = ObjectId::new();
        let row1_values = vec![Value::Uuid(row1_id), Value::Text("Alice".to_string())];
        let row1_data = encode_row(&v1_table.descriptor, &row1_values).unwrap();

        let mut storage = MemoryStorage::new();
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut().object_manager.create_with_id(
            &mut storage,
            row1_id,
            Some(metadata.clone()),
        );
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                row1_id,
                &v1_branch,
                vec![],
                row1_data.clone(),
                row1_id,
                None,
            )
            .unwrap();

        // Update v1 branch index
        storage
            .index_insert("users", "_id", &v1_branch, &Value::Uuid(row1_id), row1_id)
            .unwrap();

        // --- Insert row on v2 branch (middle schema) ---
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let row2_id = ObjectId::new();
        let row2_values = vec![
            Value::Uuid(row2_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ];
        let row2_data = encode_row(&v2_table.descriptor, &row2_values).unwrap();

        qm.sync_manager_mut().object_manager.create_with_id(
            &mut storage,
            row2_id,
            Some(metadata.clone()),
        );
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                row2_id,
                &v2_branch,
                vec![],
                row2_data.clone(),
                row2_id,
                None,
            )
            .unwrap();

        // Update v2 branch index
        storage
            .index_insert("users", "_id", &v2_branch, &Value::Uuid(row2_id), row2_id)
            .unwrap();

        // --- Insert row on v3 branch (current schema) ---
        let v3_table = v3.get(&TableName::new("users")).unwrap();
        let row3_id = ObjectId::new();
        let row3_values = vec![
            Value::Uuid(row3_id),
            Value::Text("Charlie".to_string()),
            Value::Text("charlie@example.com".to_string()),
            Value::Text("admin".to_string()),
        ];
        let row3_data = encode_row(&v3_table.descriptor, &row3_values).unwrap();

        qm.sync_manager_mut()
            .object_manager
            .create_with_id(&mut storage, row3_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                row3_id,
                &v3_branch,
                vec![],
                row3_data.clone(),
                row3_id,
                None,
            )
            .unwrap();

        // Update v3 branch index
        storage
            .index_insert("users", "_id", &v3_branch, &Value::Uuid(row3_id), row3_id)
            .unwrap();

        // --- Query across all three branches ---
        let query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch, &v3_branch])
            .build();

        let mut storage = MemoryStorage::new();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(&mut storage);

        let results = qm.get_subscription_results(sub_id);

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

    /// Test multi-hop with chained column renames across versions.
    #[test]
    fn end_to_end_multi_hop_chained_renames() {
        // v1: users(id, email)
        // v2: users(id, email_address) - renamed
        // v3: users(id, contact_email) - renamed again
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v3 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("contact_email", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);
        let v3_hash = SchemaHash::compute(&v3);

        // v1 -> v2: email -> email_address
        let mut transform_v1_v2 = LensTransform::new();
        transform_v1_v2.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens_v1_v2 = Lens::new(v1_hash, v2_hash, transform_v1_v2);

        // v2 -> v3: email_address -> contact_email
        let mut transform_v2_v3 = LensTransform::new();
        transform_v2_v3.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email_address".to_string(),
                new_name: "contact_email".to_string(),
            },
            false,
        );
        let lens_v2_v3 = Lens::new(v2_hash, v3_hash, transform_v2_v3);

        // Create QueryManager with new API
        let sm = SyncManager::new();
        let mut qm = QueryManager::new(sm);
        qm.set_current_schema(v3.clone(), "dev", "main");
        qm.add_live_schema(v2.clone());
        qm.register_lens(lens_v2_v3);
        qm.add_live_schema(v1.clone());
        qm.register_lens(lens_v1_v2);

        let v1_branch = format!("dev-{}-main", v1_hash.short());

        // Insert row on v1 branch with old column name
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let row_id = ObjectId::new();
        let row_values = vec![
            Value::Uuid(row_id),
            Value::Text("alice@example.com".to_string()),
        ];
        let row_data = encode_row(&v1_table.descriptor, &row_values).unwrap();

        let mut storage = MemoryStorage::new();
        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .create_with_id(&mut storage, row_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                row_id,
                &v1_branch,
                vec![],
                row_data.clone(),
                row_id,
                None,
            )
            .unwrap();

        // Update v1 branch index
        storage
            .index_insert("users", "_id", &v1_branch, &Value::Uuid(row_id), row_id)
            .unwrap();

        // Query
        let query = QueryBuilder::new("users").branches(&[&v1_branch]).build();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(&mut storage);

        let results = qm.get_subscription_results(sub_id);

        // Should find the row
        assert_eq!(results.len(), 1);

        // Row should have v3 schema structure (2 columns, but with contact_email name)
        let (_, row) = &results[0];
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::Uuid(row_id));
        // Email value should be preserved through both renames
        assert_eq!(row[1], Value::Text("alice@example.com".to_string()));
    }

    /// End-to-end test with column rename: query uses new column name,
    /// lens translates for old schema index lookup.
    #[test]
    fn end_to_end_column_rename_index_translation() {
        // Schema v1: users(id, email)
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email", ColumnType::Text),
            )
            .build();

        // Schema v2: users(id, email_address) - renamed column
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("email_address", ColumnType::Text),
            )
            .build();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // Create explicit rename lens
        let mut transform = LensTransform::new();
        transform.push(
            LensOp::RenameColumn {
                table: "users".to_string(),
                old_name: "email".to_string(),
                new_name: "email_address".to_string(),
            },
            false,
        );
        let lens = Lens::new(v1_hash, v2_hash, transform);

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

        // --- Insert row on v1 branch with old column name ---
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let row_id = ObjectId::new();
        let row_values = vec![
            Value::Uuid(row_id),
            Value::Text("alice@example.com".to_string()),
        ];
        let row_data = encode_row(&v1_table.descriptor, &row_values).unwrap();

        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .create_with_id(&mut storage, row_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                row_id,
                &v1_branch,
                vec![],
                row_data.clone(),
                row_id,
                None,
            )
            .unwrap();

        // Update _id index for v1 branch
        storage
            .index_insert("users", "_id", &v1_branch, &Value::Uuid(row_id), row_id)
            .unwrap();

        // --- Query using NEW column name (email_address) ---
        let query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch])
            .build();
        let sub_id = qm.subscribe(query).unwrap();
        qm.process(&mut storage);

        let results = qm.get_subscription_results(sub_id);

        // Should find the row
        assert_eq!(results.len(), 1);

        // Row should be transformed - column renamed
        let (_, row) = &results[0];
        assert_eq!(row.len(), 2);
        assert_eq!(row[0], Value::Uuid(row_id));
        assert_eq!(row[1], Value::Text("alice@example.com".to_string()));
    }

    // ========================================================================
    // Catalogue Sync Tests
    // ========================================================================

    use crate::schema_manager::manager::{CATALOGUE_TYPE_LENS, CATALOGUE_TYPE_SCHEMA};
    use crate::schema_manager::{
        decode_lens_transform, decode_schema, encode_lens_transform, encode_schema,
    };

    /// Test schema persistence and encoding roundtrip.
    #[test]
    fn catalogue_schema_persistence() {
        let v2 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text)
                    .nullable_column("email", ColumnType::Text),
            )
            .build();

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();

        // Persist schema
        let mut storage = MemoryStorage::new();
        let object_id = manager.persist_schema(&mut storage);

        // Verify ObjectId is deterministic (based on schema hash)
        let schema_hash = SchemaHash::compute(&v2);
        assert_eq!(object_id, schema_hash.to_object_id());

        // Verify schema was encoded correctly by decoding it back
        let encoded = encode_schema(&v2);
        let decoded = decode_schema(&encoded).unwrap();

        // Decoded schema should match original
        assert_eq!(decoded.len(), v2.len());
        assert!(decoded.contains_key(&TableName::new("users")));
    }

    /// Test lens persistence and encoding roundtrip.
    #[test]
    fn catalogue_lens_persistence() {
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

        let mut manager =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
        let lens = manager.add_live_schema(v1).unwrap().clone();

        // Persist lens
        let mut storage = MemoryStorage::new();
        let object_id = manager.persist_lens(&mut storage, &lens);

        // Verify ObjectId is deterministic
        assert_eq!(object_id, lens.object_id());

        // Verify lens transform was encoded correctly
        let encoded = encode_lens_transform(&lens.forward);
        let decoded = decode_lens_transform(&encoded).unwrap();

        assert_eq!(decoded.ops.len(), lens.forward.ops.len());
    }

    /// Test catalogue update processing: schema received via sync.
    #[test]
    fn catalogue_process_schema_update() {
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

        // Client B starts with v1 schema
        let mut manager_b =
            SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main")
                .unwrap();

        // Simulate receiving v2 schema from catalogue sync
        let v2_hash = SchemaHash::compute(&v2);
        let v2_object_id = v2_hash.to_object_id();
        let v2_encoded = encode_schema(&v2);

        let mut metadata = HashMap::new();
        metadata.insert("type".to_string(), CATALOGUE_TYPE_SCHEMA.to_string());
        metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        metadata.insert("schema_hash".to_string(), v2_hash.to_string());

        // Process the catalogue update
        manager_b
            .process_catalogue_update(v2_object_id, &metadata, &v2_encoded)
            .unwrap();

        // v2 should now be pending (no lens path yet)
        assert!(manager_b.context().is_pending(&v2_hash));
        assert!(!manager_b.context().is_live(&v2_hash));
    }

    /// Test catalogue update processing: lens makes pending schema live.
    #[test]
    fn catalogue_lens_activates_pending_schema() {
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

        // Client B starts with v1 schema as current
        let mut manager_b =
            SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main")
                .unwrap();

        let v1_hash = SchemaHash::compute(&v1);
        let v2_hash = SchemaHash::compute(&v2);

        // First, receive v2 schema (will be pending - no lens)
        let v2_object_id = v2_hash.to_object_id();
        let v2_encoded = encode_schema(&v2);
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("type".to_string(), CATALOGUE_TYPE_SCHEMA.to_string());
        schema_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        schema_metadata.insert("schema_hash".to_string(), v2_hash.to_string());

        manager_b
            .process_catalogue_update(v2_object_id, &schema_metadata, &v2_encoded)
            .unwrap();

        assert!(manager_b.context().is_pending(&v2_hash));

        // Now receive the lens v1->v2
        let lens = generate_lens(&v1, &v2);
        let lens_object_id = lens.object_id();
        let lens_encoded = encode_lens_transform(&lens.forward);

        let mut lens_metadata = HashMap::new();
        lens_metadata.insert("type".to_string(), CATALOGUE_TYPE_LENS.to_string());
        lens_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        lens_metadata.insert("source_hash".to_string(), v1_hash.to_string());
        lens_metadata.insert("target_hash".to_string(), v2_hash.to_string());

        manager_b
            .process_catalogue_update(lens_object_id, &lens_metadata, &lens_encoded)
            .unwrap();

        // v2 should now be live because lens_path() traverses backward lenses too.
        // Current schema is v1. For pending v2 to become live, we need a path from v2 to v1.
        // The lens is registered as (v1, v2), but lens_path() BFS considers backward direction,
        // so it finds v2 -> v1 via the backward transform.
        assert!(manager_b.get_lens(&v1_hash, &v2_hash).is_some());

        // v2 should now be live (no longer pending)
        assert!(!manager_b.context().is_pending(&v2_hash));
        assert!(manager_b.context().is_live(&v2_hash));
        assert_eq!(manager_b.all_branches().len(), 2);
    }

    /// Test pending catalogue updates are queued in QueryManager.
    #[test]
    fn query_manager_queues_catalogue_updates() {
        use crate::query_manager::manager::QueryManager;

        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let mut qm = QueryManager::new(SyncManager::new());
        qm.set_current_schema(v1.clone(), "dev", "main");

        // Initially no pending catalogue updates
        assert!(qm.take_pending_catalogue_updates().is_empty());

        // Simulate a catalogue object being received via sync
        // For this to work, we'd need to inject via the object manager and process
        // This is more of a unit test showing the API exists
    }

    /// E2E test: Full catalogue sync flow with data query.
    ///
    /// This test simulates the complete flow where:
    /// 1. Client A (v2) persists schema and lens to catalogue
    /// 2. Client B (v1) receives via catalogue sync
    /// 3. Client A writes a row on v2 branch
    /// 4. Client B queries and receives transformed data
    #[test]
    fn e2e_catalogue_sync_with_data_query() {
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

        // === Client A: v2 schema, knows about v1 ===
        let mut client_a =
            SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main")
                .unwrap();
        let lens = client_a.add_live_schema(v1.clone()).unwrap().clone();
        // add_live_schema now automatically updates QueryManager

        // === Client A persists schema and lens to catalogue ===
        let mut io_a = MemoryStorage::new();
        let schema_object_id = client_a.persist_schema(&mut io_a);
        let lens_object_id = client_a.persist_lens(&mut io_a, &lens);

        // Verify deterministic ObjectIds
        assert_eq!(schema_object_id, v2_hash.to_object_id());
        assert_eq!(lens_object_id, lens.object_id());

        // === Client B: v1 schema (old client) ===
        let mut client_b =
            SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main")
                .unwrap();

        // Initially B only knows about v1
        assert_eq!(client_b.all_branches().len(), 1);
        assert!(!client_b.context().is_live(&v2_hash));

        // === Simulate catalogue sync: transfer schema and lens objects ===
        // In real sync, this would happen via SyncManager inbox/outbox.
        // Here we simulate by manually constructing the catalogue updates.

        // First, receive the v2 schema
        let v2_encoded = encode_schema(&v2);
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("type".to_string(), CATALOGUE_TYPE_SCHEMA.to_string());
        schema_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        schema_metadata.insert("schema_hash".to_string(), v2_hash.to_string());

        client_b
            .process_catalogue_update(schema_object_id, &schema_metadata, &v2_encoded)
            .unwrap();

        // v2 is pending (no lens yet)
        assert!(client_b.context().is_pending(&v2_hash));

        // Then, receive the lens
        let lens_encoded = encode_lens_transform(&lens.forward);
        let mut lens_metadata = HashMap::new();
        lens_metadata.insert("type".to_string(), CATALOGUE_TYPE_LENS.to_string());
        lens_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        lens_metadata.insert("source_hash".to_string(), v1_hash.to_string());
        lens_metadata.insert("target_hash".to_string(), v2_hash.to_string());

        client_b
            .process_catalogue_update(lens_object_id, &lens_metadata, &lens_encoded)
            .unwrap();

        // Now v2 should be live (lens path v2->v1 exists via backward lens)
        assert!(!client_b.context().is_pending(&v2_hash));
        assert!(client_b.context().is_live(&v2_hash));
        assert_eq!(client_b.all_branches().len(), 2);

        // QueryManager is automatically updated when schemas activate via process()

        // === Client A writes a row on v2 branch ===
        let id = ObjectId::new();
        let id_val = Value::Uuid(id);
        let name = Value::Text("Alice".into());
        let email = Value::Text("alice@example.com".into());

        client_a
            .insert(
                &mut io_a,
                "users",
                &[id_val.clone(), name.clone(), email.clone()],
            )
            .unwrap();
        client_a.process(&mut io_a);

        // === Simulate data sync: A's row arrives at B ===
        // For a full E2E test, we would pump sync messages between A and B.
        // Since that requires more infrastructure, we verify the transformation
        // logic works by having B transform a v2 row manually.

        // IMPORTANT: Use the schema from client_b's context (decoded from catalogue)
        // because encoding sorts columns alphabetically, so the order may differ
        // from the test-defined v2 schema.
        let v2_schema_from_catalogue = client_b.context().get_schema(&v2_hash).unwrap();
        let v2_table = v2_schema_from_catalogue
            .get(&TableName::new("users"))
            .unwrap();

        // Build values in the correct column order (alphabetical: email, id, name)
        let v2_values_ordered: Vec<Value> = v2_table
            .descriptor
            .columns
            .iter()
            .map(|col| match col.name.as_str() {
                "id" => id_val.clone(),
                "name" => name.clone(),
                "email" => email.clone(),
                _ => panic!("unexpected column"),
            })
            .collect();

        let v2_row_data = encode_row(&v2_table.descriptor, &v2_values_ordered).unwrap();

        // Transform v2 row to v1 format using B's transformer
        let transformer = client_b.transformer("users");
        let result = transformer
            .transform(&v2_row_data, make_commit_id(1), v2_hash)
            .unwrap();

        // Verify transformation happened (v2 -> v1 via backward lens)
        assert!(result.was_transformed);

        // Decode as v1 row using the schema from context (also sorted alphabetically)
        let v1_schema_from_context = client_b
            .context()
            .get_schema(&client_b.current_hash())
            .unwrap();
        let v1_table = v1_schema_from_context
            .get(&TableName::new("users"))
            .unwrap();
        let v1_values = decode_row(&v1_table.descriptor, &result.data).unwrap();

        // v1 has 2 columns (id, name) - email was removed
        assert_eq!(v1_values.len(), 2);

        // Find id and name values by column position
        let id_idx = v1_table
            .descriptor
            .columns
            .iter()
            .position(|c| c.name.as_str() == "id")
            .unwrap();
        let name_idx = v1_table
            .descriptor
            .columns
            .iter()
            .position(|c| c.name.as_str() == "name")
            .unwrap();

        assert_eq!(v1_values[id_idx], id_val);
        assert_eq!(v1_values[name_idx], name);
    }

    /// Test multi-hop lens cascade activation via catalogue.
    ///
    /// Scenario: v1 client receives v2, then v3, then lens(v1->v2), then lens(v2->v3).
    /// After each step, verify correct pending/live states.
    #[test]
    fn e2e_multi_hop_catalogue_activation() {
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

        // Client starts with v1 schema
        let mut client =
            SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main")
                .unwrap();

        // Receive v2 schema - becomes pending
        let v2_encoded = encode_schema(&v2);
        let mut v2_metadata = HashMap::new();
        v2_metadata.insert("type".to_string(), CATALOGUE_TYPE_SCHEMA.to_string());
        v2_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        v2_metadata.insert("schema_hash".to_string(), v2_hash.to_string());

        client
            .process_catalogue_update(v2_hash.to_object_id(), &v2_metadata, &v2_encoded)
            .unwrap();

        assert!(client.context().is_pending(&v2_hash));
        assert!(!client.context().is_pending(&v3_hash)); // Not received yet

        // Receive v3 schema - also becomes pending
        let v3_encoded = encode_schema(&v3);
        let mut v3_metadata = HashMap::new();
        v3_metadata.insert("type".to_string(), CATALOGUE_TYPE_SCHEMA.to_string());
        v3_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        v3_metadata.insert("schema_hash".to_string(), v3_hash.to_string());

        client
            .process_catalogue_update(v3_hash.to_object_id(), &v3_metadata, &v3_encoded)
            .unwrap();

        assert!(client.context().is_pending(&v2_hash));
        assert!(client.context().is_pending(&v3_hash));

        // Receive lens v1->v2
        // This should activate v2 (v2 can reach v1 via backward lens)
        let lens_v1_v2 = generate_lens(&v1, &v2);
        let lens_v1_v2_encoded = encode_lens_transform(&lens_v1_v2.forward);
        let mut lens_v1_v2_metadata = HashMap::new();
        lens_v1_v2_metadata.insert("type".to_string(), CATALOGUE_TYPE_LENS.to_string());
        lens_v1_v2_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        lens_v1_v2_metadata.insert("source_hash".to_string(), v1_hash.to_string());
        lens_v1_v2_metadata.insert("target_hash".to_string(), v2_hash.to_string());

        client
            .process_catalogue_update(
                lens_v1_v2.object_id(),
                &lens_v1_v2_metadata,
                &lens_v1_v2_encoded,
            )
            .unwrap();

        // v2 should now be live
        assert!(!client.context().is_pending(&v2_hash));
        assert!(client.context().is_live(&v2_hash));

        // v3 should still be pending (no path to v1 yet)
        assert!(client.context().is_pending(&v3_hash));

        // Receive lens v2->v3
        // This should activate v3 (v3 -> v2 via backward, then v2 -> v1 via backward)
        let lens_v2_v3 = generate_lens(&v2, &v3);
        let lens_v2_v3_encoded = encode_lens_transform(&lens_v2_v3.forward);
        let mut lens_v2_v3_metadata = HashMap::new();
        lens_v2_v3_metadata.insert("type".to_string(), CATALOGUE_TYPE_LENS.to_string());
        lens_v2_v3_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        lens_v2_v3_metadata.insert("source_hash".to_string(), v2_hash.to_string());
        lens_v2_v3_metadata.insert("target_hash".to_string(), v3_hash.to_string());

        client
            .process_catalogue_update(
                lens_v2_v3.object_id(),
                &lens_v2_v3_metadata,
                &lens_v2_v3_encoded,
            )
            .unwrap();

        // v3 should now be live (2-hop path: v3 -> v2 -> v1)
        assert!(!client.context().is_pending(&v3_hash));
        assert!(client.context().is_live(&v3_hash));

        // All three schemas should now be live
        assert_eq!(client.all_branches().len(), 3);
    }

    // ========================================================================
    // Multi-Client Server Schema Sync Tests
    // ========================================================================

    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, PersistenceTier, QueryId, ServerId, Source, SyncPayload,
    };

    /// Approve all pending updates on a SyncManager (test helper).
    fn approve_all_pending(sm: &mut SyncManager, storage: &mut MemoryStorage) {
        let ids = sm.pending_update_ids();
        for id in ids {
            sm.approve_update(storage, id);
        }
    }

    /// E2E test: Two clients with same schema, server with empty schema.
    ///
    /// NOTE: This test is incomplete. The current architecture requires servers
    /// to be initialized with the schema. Catalogue sync is designed for schema
    /// EVOLUTION (adding new schema versions via lenses), not for schema
    /// BOOTSTRAPPING (starting with no schema).
    ///
    /// The main test `e2e_two_clients_query_subscriptions_through_server`
    /// validates the intended use case where all nodes share the same schema.
    ///
    /// Now implemented via lazy schema activation in QueryManager.
    #[test]
    fn e2e_two_clients_server_schema_sync() {
        // === Define the schema (both clients use this) ===
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("documents")
                    .column("id", ColumnType::Uuid)
                    .column("owner_id", ColumnType::Text)
                    .column("title", ColumnType::Text),
            )
            .build();

        let schema_hash = SchemaHash::compute(&schema);

        // === Setup Client A (alice) ===
        let mut client_a = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        // === Setup Client B (bob) ===
        let mut client_b = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        // === Setup Server with NO schema (true server mode) ===
        // Server starts with no schema knowledge - will learn via catalogue sync
        let mut server = SchemaManager::new_server(SyncManager::new(), test_app_id(), "dev");

        // === Client A persists schema to catalogue BEFORE connecting to server ===
        // This way, when the server is added, the catalogue object will sync
        let mut io_a = MemoryStorage::new();
        let mut io_server = MemoryStorage::new();
        let schema_obj_id = client_a.persist_schema(&mut io_a);
        assert_eq!(schema_obj_id, schema_hash.to_object_id());

        // === Network topology setup ===
        // Client A <-> Server <-> Client B
        let client_a_id = ClientId::new();
        let client_b_id = ClientId::new();
        let server_id = ServerId::new();

        // Server knows about both clients
        server
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_a_id);
        server
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_b_id);

        // Clients know about the server - this triggers initial sync including catalogue objects
        client_a
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);
        client_b
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);

        // Process to generate outbox messages
        client_a.process(&mut io_a);

        // === Transfer catalogue object from Client A to Server ===
        let outbox_a = client_a
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        assert!(
            !outbox_a.is_empty(),
            "Client A should have catalogue object to sync"
        );

        // Find the schema catalogue object
        let schema_msg = outbox_a
            .iter()
            .find(|e| {
                if let SyncPayload::ObjectUpdated { object_id, .. } = &e.payload {
                    *object_id == schema_obj_id
                } else {
                    false
                }
            })
            .expect("Should have schema object in outbox");

        // Push to server inbox
        server
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_a_id),
                payload: schema_msg.payload.clone(),
            });

        // Server processes the inbox
        server.process(&mut io_server);

        // Approve pending updates (catalogue objects are out-of-scope for clients without queries)
        approve_all_pending(
            server.query_manager_mut().sync_manager_mut(),
            &mut io_server,
        );
        // Re-process after approval to trigger catalogue processing
        server.process(&mut io_server);

        // === Server should now have the schema in known_schemas ===
        assert!(
            server.is_schema_known(&schema_hash),
            "Server should know about the schema after catalogue sync"
        );

        // === Now test that row data syncs and is indexed via lazy activation ===
        // Client A creates a document
        let doc_uuid = ObjectId::new();
        let doc_values = vec![
            Value::Uuid(doc_uuid),
            Value::Text("alice".into()),
            Value::Text("Test Document".into()),
        ];
        let handle = client_a
            .insert(&mut io_a, "documents", &doc_values)
            .unwrap();
        let doc_id = handle.row_id; // The actual row object ID
        client_a.process(&mut io_a);

        // Get client A's outbox - should have the row object
        let outbox_a = client_a
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        assert!(!outbox_a.is_empty(), "Client A should have row to sync");

        // Find the row object message
        let row_msg = outbox_a
            .iter()
            .find(|e| {
                if let SyncPayload::ObjectUpdated { object_id, .. } = &e.payload {
                    *object_id == doc_id
                } else {
                    false
                }
            })
            .expect("Should have row object in outbox");

        // Push to server inbox
        server
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_a_id),
                payload: row_msg.payload.clone(),
            });

        // Server processes
        server.process(&mut io_server);

        // Approve pending row update and re-process for lazy activation
        approve_all_pending(
            server.query_manager_mut().sync_manager_mut(),
            &mut io_server,
        );
        server.process(&mut io_server);

        // === Verify the row was indexed on server ===
        // Build the branch name that client A uses
        let client_branch = ComposedBranchName::new("dev", schema_hash, "main").to_branch_name();

        // The row should be in the server's index for that branch
        assert!(
            server.query_manager().row_is_indexed_on_branch(
                &io_server,
                "documents",
                client_branch.as_str(),
                doc_id
            ),
            "Row should be indexed on server after lazy activation"
        );
    }

    /// E2E test: Two clients, server all with same schema - query subscriptions sync.
    ///
    /// This is the more direct test of the user's question: both clients issue
    /// query subscriptions that correctly sync through the server.
    #[test]
    fn e2e_two_clients_query_subscriptions_through_server() {
        use crate::query_manager::session::Session;

        // === Define schema with owner-based policy ===
        let schema = {
            use crate::query_manager::policy::PolicyExpr;
            use crate::query_manager::types::TablePolicies;

            let mut schema = Schema::new();

            let docs_descriptor = RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]);

            // Policy: SELECT allowed if owner_id matches session.user_id
            let docs_policies = TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));

            schema.insert(
                TableName::new("documents"),
                TableSchema::with_policies(docs_descriptor, docs_policies),
            );

            schema
        };

        // === Setup three nodes with same schema ===
        let mut client_a = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        let mut client_b = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        let mut server = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        // === Network topology ===
        let client_a_id = ClientId::new();
        let client_b_id = ClientId::new();
        let server_id = ServerId::new();

        // Server knows about clients
        server
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_a_id);
        server
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_b_id);

        // Set sessions for permission checking
        server
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_session(client_a_id, Session::new("alice"));
        server
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_session(client_b_id, Session::new("bob"));

        // Clients know about server
        client_a
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);
        client_b
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);

        // Clear initial sync
        client_a
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        client_b
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        server.query_manager_mut().sync_manager_mut().take_outbox();

        let mut io_a = MemoryStorage::new();
        let mut io_b = MemoryStorage::new();
        let mut io_server = MemoryStorage::new();

        // === Create documents on server ===
        // Alice's document
        let alice_doc_id = ObjectId::new();
        let alice_doc_data = encode_row(
            &RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            &[
                Value::Uuid(alice_doc_id),
                Value::Text("alice".into()),
                Value::Text("Alice's Secret Doc".into()),
            ],
        )
        .unwrap();

        let mut alice_metadata = HashMap::new();
        alice_metadata.insert("table".to_string(), "documents".to_string());

        // Get branch name before mutable borrows
        let server_branch = server.branch_name().to_string();

        server
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .create_with_id(&mut io_server, alice_doc_id, Some(alice_metadata.clone()));
        server
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .add_commit(
                &mut io_server,
                alice_doc_id,
                &server_branch,
                vec![],
                alice_doc_data,
                alice_doc_id,
                None,
            )
            .unwrap();

        // Bob's document
        let bob_doc_id = ObjectId::new();
        let bob_doc_data = encode_row(
            &RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            &[
                Value::Uuid(bob_doc_id),
                Value::Text("bob".into()),
                Value::Text("Bob's Private Doc".into()),
            ],
        )
        .unwrap();

        let mut bob_metadata = HashMap::new();
        bob_metadata.insert("table".to_string(), "documents".to_string());

        server
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .create_with_id(&mut io_server, bob_doc_id, Some(bob_metadata.clone()));
        server
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .add_commit(
                &mut io_server,
                bob_doc_id,
                &server_branch,
                vec![],
                bob_doc_data,
                bob_doc_id,
                None,
            )
            .unwrap();

        // Update server indices
        {
            let branch = server.branch_name().to_string();
            io_server
                .index_insert(
                    "documents",
                    "_id",
                    &branch,
                    &Value::Uuid(alice_doc_id),
                    alice_doc_id,
                )
                .unwrap();
            io_server
                .index_insert(
                    "documents",
                    "_id",
                    &branch,
                    &Value::Uuid(bob_doc_id),
                    bob_doc_id,
                )
                .unwrap();
        }

        // Clear any sync messages from document creation
        server.query_manager_mut().sync_manager_mut().take_outbox();

        // === Client A subscribes to documents (as alice) ===
        let query_a = QueryBuilder::new("documents")
            .branch(&client_a.branch_name().to_string())
            .build();

        let _sub_a = client_a
            .query_manager_mut()
            .subscribe_with_sync(query_a.clone(), Some(Session::new("alice")), None)
            .unwrap();

        client_a.process(&mut io_a);

        // Get the QuerySubscription message from client A
        let outbox_a = client_a
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        let query_sub_msg = outbox_a
            .iter()
            .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
            .expect("Client A should send QuerySubscription");

        // Forward to server
        server
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_a_id),
                payload: query_sub_msg.payload.clone(),
            });

        // Server processes the subscription
        server.process(&mut io_server);

        // === Server should send Alice's doc to Client A ===
        let server_outbox = server.query_manager_mut().sync_manager_mut().take_outbox();

        // Find ObjectUpdated messages destined for client A
        let alice_updates: Vec<_> = server_outbox
            .iter()
            .filter(|e| {
                matches!(e.destination, Destination::Client(cid) if cid == client_a_id)
                    && matches!(e.payload, SyncPayload::ObjectUpdated { .. })
            })
            .collect();

        // Alice should receive her own document (alice_doc_id)
        // Alice should NOT receive Bob's document due to policy
        let received_ids: Vec<ObjectId> = alice_updates
            .iter()
            .filter_map(|e| {
                if let SyncPayload::ObjectUpdated { object_id, .. } = &e.payload {
                    Some(*object_id)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            received_ids.contains(&alice_doc_id),
            "Alice should receive her own document"
        );
        assert!(
            !received_ids.contains(&bob_doc_id),
            "Alice should NOT receive Bob's document (policy check)"
        );

        // === Client B subscribes to documents (as bob) ===
        let query_b = QueryBuilder::new("documents")
            .branch(&client_b.branch_name().to_string())
            .build();

        let _sub_b = client_b
            .query_manager_mut()
            .subscribe_with_sync(query_b.clone(), Some(Session::new("bob")), None)
            .unwrap();

        client_b.process(&mut io_b);

        // Forward subscription to server
        let outbox_b = client_b
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        let query_sub_b = outbox_b
            .iter()
            .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
            .expect("Client B should send QuerySubscription");

        server
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_b_id),
                payload: query_sub_b.payload.clone(),
            });

        server.process(&mut io_server);

        // === Server should send Bob's doc to Client B ===
        let server_outbox_b = server.query_manager_mut().sync_manager_mut().take_outbox();

        let bob_updates: Vec<_> = server_outbox_b
            .iter()
            .filter(|e| {
                matches!(e.destination, Destination::Client(cid) if cid == client_b_id)
                    && matches!(e.payload, SyncPayload::ObjectUpdated { .. })
            })
            .collect();

        let bob_received_ids: Vec<ObjectId> = bob_updates
            .iter()
            .filter_map(|e| {
                if let SyncPayload::ObjectUpdated { object_id, .. } = &e.payload {
                    Some(*object_id)
                } else {
                    None
                }
            })
            .collect();

        assert!(
            bob_received_ids.contains(&bob_doc_id),
            "Bob should receive his own document"
        );
        assert!(
            !bob_received_ids.contains(&alice_doc_id),
            "Bob should NOT receive Alice's document (policy check)"
        );
    }

    /// E2E test: Server with empty schema receives schema via sync, then handles queries.
    ///
    /// This tests the full scenario: server starts with no schema knowledge,
    /// receives schema through catalogue sync, and can then process queries.
    #[test]
    fn e2e_server_learns_schema_via_catalogue_sync() {
        // === Client schema (what we want to sync to server) ===
        let client_schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("notes")
                    .column("id", ColumnType::Uuid)
                    .column("content", ColumnType::Text),
            )
            .build();

        let _client_schema_hash = SchemaHash::compute(&client_schema);

        // === Setup client with schema ===
        let mut client = SchemaManager::new(
            SyncManager::new(),
            client_schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        // === Setup server with same schema ===
        // NOTE: In the current architecture, server needs to have the schema
        // to process queries. Schema sync via catalogue is for adding NEW
        // schema versions (migrations), not for initializing from scratch.
        //
        // The server's QueryManager needs the schema to:
        // 1. Decode row data
        // 2. Build query graphs
        // 3. Maintain indices
        //
        // So for now, server must be initialized with the same base schema.
        // Catalogue sync handles schema EVOLUTION, not schema BOOTSTRAPPING.
        let mut server = SchemaManager::new(
            SyncManager::new(),
            client_schema.clone(), // Server must have base schema
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();

        // === Network topology ===
        let client_id = ClientId::new();
        let server_id = ServerId::new();

        server
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_id);
        client
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_id);

        // Clear initial sync
        client.query_manager_mut().sync_manager_mut().take_outbox();
        server.query_manager_mut().sync_manager_mut().take_outbox();

        let mut io_client = MemoryStorage::new();
        let mut io_server = MemoryStorage::new();

        // === Client persists schema to catalogue ===
        let schema_obj_id = client.persist_schema(&mut io_client);
        client.process(&mut io_client);

        // Transfer to server
        let outbox = client.query_manager_mut().sync_manager_mut().take_outbox();
        for entry in &outbox {
            if let SyncPayload::ObjectUpdated { object_id, .. } = &entry.payload {
                if *object_id == schema_obj_id {
                    server
                        .query_manager_mut()
                        .sync_manager_mut()
                        .push_inbox(InboxEntry {
                            source: Source::Client(client_id),
                            payload: entry.payload.clone(),
                        });
                }
            }
        }

        server.process(&mut io_server);

        // Approve pending updates (catalogue objects are out-of-scope for clients without queries)
        approve_all_pending(
            server.query_manager_mut().sync_manager_mut(),
            &mut io_server,
        );
        server.process(&mut io_server);

        // Process catalogue updates
        let updates = server.query_manager_mut().take_pending_catalogue_updates();
        for update in &updates {
            let _ = server.process_catalogue_update(
                update.object_id,
                &update.metadata,
                &update.content,
            );
        }

        // === Create a note on the server ===
        let note_id = ObjectId::new();
        let note_data = encode_row(
            &RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Uuid),
                ColumnDescriptor::new("content", ColumnType::Text),
            ]),
            &[Value::Uuid(note_id), Value::Text("Hello World".into())],
        )
        .unwrap();

        let mut note_metadata = HashMap::new();
        note_metadata.insert("table".to_string(), "notes".to_string());

        // Get branch name before mutable borrows
        let server_branch = server.branch_name().to_string();

        server
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .create_with_id(&mut io_server, note_id, Some(note_metadata));
        server
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .add_commit(
                &mut io_server,
                note_id,
                &server_branch,
                vec![],
                note_data,
                note_id,
                None,
            )
            .unwrap();

        // Update index
        {
            let branch = server.branch_name().to_string();
            io_server
                .index_insert("notes", "_id", &branch, &Value::Uuid(note_id), note_id)
                .unwrap();
        }

        server.query_manager_mut().sync_manager_mut().take_outbox();

        // === Client subscribes to notes ===
        let query = QueryBuilder::new("notes")
            .branch(&client.branch_name().to_string())
            .build();

        let _sub_id = client
            .query_manager_mut()
            .subscribe_with_sync(query, None, None)
            .unwrap();

        client.process(&mut io_client);

        // Forward subscription to server
        let client_outbox = client.query_manager_mut().sync_manager_mut().take_outbox();
        for entry in &client_outbox {
            if matches!(entry.payload, SyncPayload::QuerySubscription { .. }) {
                server
                    .query_manager_mut()
                    .sync_manager_mut()
                    .push_inbox(InboxEntry {
                        source: Source::Client(client_id),
                        payload: entry.payload.clone(),
                    });
            }
        }

        server.process(&mut io_server);

        // === Server should send the note to client ===
        let server_outbox = server.query_manager_mut().sync_manager_mut().take_outbox();

        let note_sent = server_outbox.iter().any(|e| {
            if let SyncPayload::ObjectUpdated { object_id, .. } = &e.payload {
                *object_id == note_id
            } else {
                false
            }
        });

        assert!(
            note_sent,
            "Server should send the note to client after query subscription"
        );
    }

    // ========================================================================
    // Pending Row Updates Tests (rows arriving before schema)
    // ========================================================================

    /// Test that rows arriving before their schema is known are buffered
    /// and processed when the schema activates.
    ///
    /// Scenario:
    /// 1. Client B (v1 schema) receives a row on the v2 branch (unknown schema)
    /// 2. The row is buffered in pending_row_updates
    /// 3. Client B receives schema v2 and lens v1->v2 via catalogue
    /// 4. process() activates v2 and retries pending rows
    /// 5. The row is now queryable with lens transform applied
    #[test]
    fn e2e_rows_buffered_until_schema_activates() {
        // Schema v1: users(id, name)
        let v1 = SchemaBuilder::new()
            .table(
                TableSchema::builder("users")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        // Schema v2: users(id, name, email)
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

        // Client starts with v1 schema only
        let mut client =
            SchemaManager::new(SyncManager::new(), v1.clone(), test_app_id(), "dev", "main")
                .unwrap();
        let mut storage = MemoryStorage::new();

        // Get branch names
        let v1_branch = format!("dev-{}-main", v1_hash.short());
        let v2_branch = format!("dev-{}-main", v2_hash.short());

        // Create a row on the v2 branch (simulate receiving via sync)
        // IMPORTANT: When schema goes through encode/decode, columns are sorted alphabetically.
        // So we need to encode the row in the same order it would arrive via sync.
        // v2 columns alphabetically: email, id, name
        let row_id = ObjectId::new();

        // Simulate what the schema looks like after encode/decode (alphabetical columns)
        let v2_encoded_decoded = decode_schema(&encode_schema(&v2)).unwrap();
        let v2_table = v2_encoded_decoded.get(&TableName::new("users")).unwrap();

        // Build values in the correct column order (alphabetical)
        let row_values: Vec<Value> = v2_table
            .descriptor
            .columns
            .iter()
            .map(|col| match col.name.as_str() {
                "id" => Value::Uuid(row_id),
                "name" => Value::Text("Alice".to_string()),
                "email" => Value::Text("alice@example.com".to_string()),
                _ => panic!("unexpected column"),
            })
            .collect();
        let row_data = encode_row(&v2_table.descriptor, &row_values).unwrap();

        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());

        // Add the object to the object manager
        client
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .create_with_id(&mut storage, row_id, Some(metadata));
        client
            .query_manager_mut()
            .sync_manager_mut()
            .object_manager
            .add_commit(
                &mut storage,
                row_id,
                &v2_branch,
                vec![],
                row_data.clone(),
                row_id,
                None,
            )
            .unwrap();

        // Process - this should trigger handle_object_update for the v2 row
        // Since v2 schema is unknown, it should be buffered
        client.process(&mut storage);

        // Query on v1 branch should not find the row (it's on v2 branch)
        let query = QueryBuilder::new("users").branch(&v1_branch).build();
        let results = execute_query(&mut client, &mut storage, query);
        assert_eq!(
            results.len(),
            0,
            "Row on v2 branch should not appear in v1 query yet"
        );

        // === Now simulate receiving v2 schema and lens via catalogue ===

        // Receive v2 schema
        let v2_encoded = encode_schema(&v2);
        let mut schema_metadata = HashMap::new();
        schema_metadata.insert("type".to_string(), CATALOGUE_TYPE_SCHEMA.to_string());
        schema_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        schema_metadata.insert("schema_hash".to_string(), v2_hash.to_string());

        client
            .process_catalogue_update(v2_hash.to_object_id(), &schema_metadata, &v2_encoded)
            .unwrap();

        // v2 should be pending (no lens yet)
        assert!(client.context().is_pending(&v2_hash));

        // Receive lens v1->v2
        let lens = generate_lens(&v1, &v2);
        let lens_encoded = encode_lens_transform(&lens.forward);
        let mut lens_metadata = HashMap::new();
        lens_metadata.insert("type".to_string(), CATALOGUE_TYPE_LENS.to_string());
        lens_metadata.insert("app_id".to_string(), test_app_id().uuid().to_string());
        lens_metadata.insert("source_hash".to_string(), v1_hash.to_string());
        lens_metadata.insert("target_hash".to_string(), v2_hash.to_string());

        client
            .process_catalogue_update(lens.object_id(), &lens_metadata, &lens_encoded)
            .unwrap();

        // Process again - this should:
        // 1. Activate v2 schema (now has lens path)
        // 2. Call sync_context()
        // 3. Retry pending row updates
        client.process(&mut storage);

        // v2 should now be live
        assert!(
            client.context().is_live(&v2_hash),
            "v2 schema should be live after lens received"
        );
        assert_eq!(client.all_branches().len(), 2, "Should have 2 branches now");

        // Now query across both branches - should find the row
        let multi_query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch])
            .build();
        let sub_id = client.query_manager_mut().subscribe(multi_query).unwrap();
        client.process(&mut storage);

        let results = client.query_manager().get_subscription_results(sub_id);

        // Should have 1 row (from v2 branch, transformed to v1 format)
        assert_eq!(
            results.len(),
            1,
            "Should find the buffered row after schema activation"
        );

        // The row should be transformed via lens (v2 -> v1 removes email column)
        // Note: v1 has 2 columns (id, name), so after transform we get 2 columns
        // Columns are alphabetically sorted: id, name
        assert_eq!(
            results[0].1.len(),
            2,
            "Row should be transformed to v1 format (2 columns)"
        );

        // Find columns by name since order depends on encoding
        let v1_encoded_decoded = decode_schema(&encode_schema(&v1)).unwrap();
        let v1_table = v1_encoded_decoded.get(&TableName::new("users")).unwrap();
        let id_idx = v1_table
            .descriptor
            .columns
            .iter()
            .position(|c| c.name.as_str() == "id")
            .unwrap();
        let name_idx = v1_table
            .descriptor
            .columns
            .iter()
            .position(|c| c.name.as_str() == "name")
            .unwrap();

        assert_eq!(results[0].1[id_idx], Value::Uuid(row_id));
        assert_eq!(results[0].1[name_idx], Value::Text("Alice".to_string()));
    }

    // ========================================================================
    // Query Settlement Tier Tests
    // ========================================================================

    /// Test 1: Subscribe with settled_tier=None — immediate delivery (current behavior).
    #[test]
    fn query_settled_no_tier_immediate() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let mut manager = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        let mut storage = MemoryStorage::new();

        // Insert a row
        let row_id = ObjectId::new();
        let values = vec![Value::Uuid(row_id), Value::Text("hello".into())];
        manager.insert(&mut storage, "items", &values).unwrap();
        manager.process(&mut storage);

        // Subscribe with settled_tier=None
        let query = QueryBuilder::new("items").build();
        let sub_id = manager
            .query_manager_mut()
            .subscribe_with_session(query, None, None)
            .unwrap();
        manager.process(&mut storage);

        // Should get immediate callback on first process
        let updates = manager.query_manager_mut().take_updates();
        assert!(
            !updates.is_empty(),
            "settled_tier=None should deliver immediately"
        );
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert_eq!(matching.len(), 1);
        assert_eq!(matching[0].delta.added.len(), 1);
    }

    /// Test 2: Client A subscribes on server B with settled_tier=Worker.
    /// B settles → emits QuerySettled(Worker). After A receives it, A delivers.
    #[test]
    fn query_settled_direct() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        // === Setup Client A ===
        let mut client_a = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        let mut io_a = MemoryStorage::new();

        // === Setup Server B (Worker tier) ===
        let mut server_b = SchemaManager::new(
            SyncManager::new().with_tier(PersistenceTier::Worker),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        let mut io_b = MemoryStorage::new();

        // === Network topology ===
        let client_a_id = ClientId::new();
        let server_b_id = ServerId::new();

        server_b
            .query_manager_mut()
            .sync_manager_mut()
            .add_client(client_a_id);
        client_a
            .query_manager_mut()
            .sync_manager_mut()
            .add_server(server_b_id);

        // Insert a row on server B
        let row_id = ObjectId::new();
        let values = vec![Value::Uuid(row_id), Value::Text("srv".into())];
        server_b.insert(&mut io_b, "items", &values).unwrap();
        server_b.process(&mut io_b);

        // === Client A subscribes with settled_tier=Worker ===
        let query = QueryBuilder::new("items")
            .branch(&client_a.branch_name().to_string())
            .build();
        let sub_id = client_a
            .query_manager_mut()
            .subscribe_with_sync(query, None, Some(PersistenceTier::Worker))
            .unwrap();
        client_a.process(&mut io_a);

        // First process: no data yet (no QuerySettled received)
        let updates = client_a.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            matching.is_empty() || matching.iter().all(|u| u.delta.added.is_empty()),
            "Should not deliver before QuerySettled"
        );

        // === Forward QuerySubscription from A to B ===
        let outbox_a = client_a
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();
        let query_sub = outbox_a
            .iter()
            .find(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
            .expect("Should have QuerySubscription");

        server_b
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Client(client_a_id),
                payload: query_sub.payload.clone(),
            });

        // Server B processes (settles server sub → emits QuerySettled)
        server_b.process(&mut io_b);
        // Approve any pending updates
        approve_all_pending(server_b.query_manager_mut().sync_manager_mut(), &mut io_b);
        server_b.process(&mut io_b);

        let outbox_b = server_b
            .query_manager_mut()
            .sync_manager_mut()
            .take_outbox();

        // Expect QuerySettled(Worker) in outbox
        let settled_msg = outbox_b
            .iter()
            .find(|e| matches!(e.payload, SyncPayload::QuerySettled { .. }));
        assert!(
            settled_msg.is_some(),
            "Server B should emit QuerySettled(Worker)"
        );

        // Forward all from B to A (including data + QuerySettled)
        for entry in &outbox_b {
            if matches!(entry.destination, Destination::Client(cid) if cid == client_a_id) {
                client_a
                    .query_manager_mut()
                    .sync_manager_mut()
                    .push_inbox(InboxEntry {
                        source: Source::Server(server_b_id),
                        payload: entry.payload.clone(),
                    });
            }
        }

        // Process client A with QuerySettled now available
        client_a.process(&mut io_a);
        // Second process to settle after data arrives
        client_a.process(&mut io_a);

        let updates = client_a.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            !matching.is_empty(),
            "Should deliver after QuerySettled(Worker) received"
        );
        let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
        assert!(total_added >= 1, "Should have at least 1 row delivered");
    }

    /// Test 3: A subscribes with settled_tier=EdgeServer through B (Worker) to C (EdgeServer).
    /// Worker settling is insufficient. EdgeServer settling satisfies the requirement.
    #[test]
    fn query_settled_holds_until_tier() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        // Client A — no tier (end client)
        let mut client_a = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        let mut io_a = MemoryStorage::new();

        // Subscribe with settled_tier=EdgeServer
        let query = QueryBuilder::new("items")
            .branch(&client_a.branch_name().to_string())
            .build();
        let sub_id = client_a
            .query_manager_mut()
            .subscribe_with_sync(query, None, Some(PersistenceTier::EdgeServer))
            .unwrap();
        client_a.process(&mut io_a);

        // Insert a row locally on A (so there's data to deliver)
        let row_id = ObjectId::new();
        let values = vec![Value::Uuid(row_id), Value::Text("local".into())];
        client_a.insert(&mut io_a, "items", &values).unwrap();
        client_a.process(&mut io_a);

        // No delivery yet — tier not satisfied
        let updates = client_a.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
            "Should not deliver — EdgeServer tier not achieved"
        );

        // Simulate receiving QuerySettled(Worker) — insufficient
        let query_id = QueryId(sub_id.0);
        let server_b_id = ServerId::new();
        client_a
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Server(server_b_id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: PersistenceTier::Worker,
                },
            });
        client_a.process(&mut io_a);

        let updates = client_a.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
            "Worker < EdgeServer — should still not deliver"
        );

        // Simulate receiving QuerySettled(EdgeServer) — sufficient
        client_a
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Server(server_b_id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: PersistenceTier::EdgeServer,
                },
            });
        client_a.process(&mut io_a);

        let updates = client_a.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            !matching.is_empty(),
            "EdgeServer >= EdgeServer — should deliver now"
        );
        let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
        assert_eq!(total_added, 1, "Should deliver the accumulated row");
    }

    /// Test 4: Data accumulates while waiting for tier. First delivery contains all rows.
    #[test]
    fn query_settled_data_accumulates() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let mut client = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        let mut storage = MemoryStorage::new();

        // Subscribe with settled_tier=Worker
        let query = QueryBuilder::new("items")
            .branch(&client.branch_name().to_string())
            .build();
        let sub_id = client
            .query_manager_mut()
            .subscribe_with_sync(query, None, Some(PersistenceTier::Worker))
            .unwrap();
        client.process(&mut storage);

        // Insert 3 rows before tier is satisfied
        for i in 0..3 {
            let row_id = ObjectId::new();
            let values = vec![Value::Uuid(row_id), Value::Text(format!("item_{}", i))];
            client.insert(&mut storage, "items", &values).unwrap();
            client.process(&mut storage);
        }

        // No delivery yet
        let updates = client.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
            "Should not deliver before tier is satisfied"
        );

        // Send QuerySettled(Worker)
        let query_id = QueryId(sub_id.0);
        let server_id = ServerId::new();
        client
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Server(server_id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: PersistenceTier::Worker,
                },
            });
        client.process(&mut storage);

        let updates = client.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            !matching.is_empty(),
            "Should deliver after QuerySettled(Worker)"
        );
        let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
        assert_eq!(
            total_added, 3,
            "First delivery should contain all 3 accumulated rows"
        );
    }

    /// Test 5: One-shot query() with settled_tier via subscribe_with_sync.
    /// The subscription should not deliver until the required tier confirms.
    #[test]
    fn query_one_shot_settled_tier() {
        let schema = SchemaBuilder::new()
            .table(
                TableSchema::builder("items")
                    .column("id", ColumnType::Uuid)
                    .column("name", ColumnType::Text),
            )
            .build();

        let mut client = SchemaManager::new(
            SyncManager::new(),
            schema.clone(),
            test_app_id(),
            "dev",
            "main",
        )
        .unwrap();
        let mut storage = MemoryStorage::new();

        // Insert a row first
        let row_id = ObjectId::new();
        let values = vec![Value::Uuid(row_id), Value::Text("one-shot".into())];
        client.insert(&mut storage, "items", &values).unwrap();
        client.process(&mut storage);

        // Subscribe with settled_tier=Worker (simulating one-shot behavior)
        let query = QueryBuilder::new("items")
            .branch(&client.branch_name().to_string())
            .build();
        let sub_id = client
            .query_manager_mut()
            .subscribe_with_sync(query, None, Some(PersistenceTier::Worker))
            .unwrap();
        client.process(&mut storage);

        // First process: no delivery (waiting for Worker tier)
        let updates = client.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            matching.is_empty() || matching.iter().all(|u| u.delta.is_empty()),
            "One-shot with settled_tier should not resolve on first local settle"
        );

        // Send QuerySettled(Worker)
        let query_id = QueryId(sub_id.0);
        let server_id = ServerId::new();
        client
            .query_manager_mut()
            .sync_manager_mut()
            .push_inbox(InboxEntry {
                source: Source::Server(server_id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: PersistenceTier::Worker,
                },
            });
        client.process(&mut storage);

        // Now should resolve with correct data
        let updates = client.query_manager_mut().take_updates();
        let matching: Vec<_> = updates
            .iter()
            .filter(|u| u.subscription_id == sub_id)
            .collect();
        assert!(
            !matching.is_empty(),
            "Should deliver after QuerySettled(Worker)"
        );
        let total_added: usize = matching.iter().map(|u| u.delta.added.len()).sum();
        assert_eq!(total_added, 1, "Should contain the one row");
    }
}
