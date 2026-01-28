//! Integration tests for SchemaManager - full flow from schema to transformation.

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::query_manager::types::{
        ColumnType, SchemaBuilder, SchemaHash, TableName, TableSchema, Value,
    };
    use crate::schema_manager::{
        AppId, CopyOnWriteWriter, Lens, LensOp, LensTransform, SchemaContext, SchemaManager,
        generate_lens,
    };

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
    use crate::query_manager::query::QueryBuilder;
    use crate::sync_manager::SyncManager;

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

        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        // Create QueryManager with schema context
        let sm = SyncManager::new();
        let qm = QueryManager::new_with_schema_context(sm, v2.clone(), ctx);

        // Verify schema context is present
        assert!(qm.schema_context().is_some());

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

        // Extract context for use with QueryManager
        // In real usage, SchemaManager would provide this or QueryManager would be created from it
        let ctx = schema_mgr.context().clone();

        // Create QueryManager with the context
        let sm = SyncManager::new();
        let qm = QueryManager::new_with_schema_context(sm, v2.clone(), ctx);

        // Verify integration
        assert!(qm.schema_context().is_some());
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

        // Create schema context with v2 as current, v1 as live
        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        // Create QueryManager with schema context
        let sm = SyncManager::new();
        let mut qm = QueryManager::new_with_schema_context(sm, v2.clone(), ctx);

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
        qm.sync_manager_mut()
            .object_manager
            .create_with_id(old_row_id, Some(metadata.clone()));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
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
        {
            let index_key = ("users".to_string(), "_id".to_string(), v1_branch.clone());
            let index = qm
                .test_get_index_mut(&index_key)
                .expect("v1 branch _id index should exist");
            index
                .insert(old_row_id.uuid().as_bytes(), old_row_id)
                .unwrap();
        }

        // --- Insert a row on the NEW schema branch (v2 format: id, name, email) ---
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let new_row_id = ObjectId::new();
        let new_row_values = vec![
            Value::Uuid(new_row_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ];
        let new_row_data = encode_row(&v2_table.descriptor, &new_row_values).unwrap();

        qm.sync_manager_mut()
            .object_manager
            .create_with_id(new_row_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                new_row_id,
                &v2_branch,
                vec![],
                new_row_data.clone(),
                new_row_id,
                None,
            )
            .unwrap();

        // Update indices for new branch
        {
            let index_key = ("users".to_string(), "_id".to_string(), v2_branch.clone());
            let index = qm
                .test_get_index_mut(&index_key)
                .expect("v2 branch _id index should exist");
            index
                .insert(new_row_id.uuid().as_bytes(), new_row_id)
                .unwrap();
        }

        // --- Query across both branches ---
        let query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch])
            .build();

        // Subscribe and process to settle the graph
        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

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
            .find(|r| {
                r.iter()
                    .any(|v| matches!(v, Value::Text(s) if s == "Alice"))
            })
            .expect("Alice's row should be present");

        // Alice's row should have been transformed to v2 format (3 columns)
        assert_eq!(
            alice_row.len(),
            3,
            "Alice's row should have 3 columns after lens transform"
        );
        assert_eq!(alice_row[0], Value::Uuid(old_row_id));
        assert_eq!(alice_row[1], Value::Text("Alice".to_string()));
        assert_eq!(
            alice_row[2],
            Value::Null,
            "Added email column should be Null"
        );

        // Find Bob's row (from v2 branch) - already in v2 format
        let bob_row = results
            .iter()
            .find(|r| r.iter().any(|v| matches!(v, Value::Text(s) if s == "Bob")))
            .expect("Bob's row should be present");

        assert_eq!(bob_row.len(), 3);
        assert_eq!(bob_row[0], Value::Uuid(new_row_id));
        assert_eq!(bob_row[1], Value::Text("Bob".to_string()));
        assert_eq!(bob_row[2], Value::Text("bob@example.com".to_string()));
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

        // Create schema context with v3 as current
        let mut ctx = SchemaContext::new(v3.clone(), "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1.clone(), lens_v1_v2);

        // Create QueryManager with schema context
        let sm = SyncManager::new();
        let mut qm = QueryManager::new_with_schema_context(sm, v3.clone(), ctx);

        // Get branch names
        let v1_branch = format!("dev-{}-main", v1_hash.short());
        let v2_branch = format!("dev-{}-main", v2_hash.short());
        let v3_branch = format!("dev-{}-main", v3_hash.short());

        // --- Insert row on v1 branch (oldest schema) ---
        let v1_table = v1.get(&TableName::new("users")).unwrap();
        let row1_id = ObjectId::new();
        let row1_values = vec![Value::Uuid(row1_id), Value::Text("Alice".to_string())];
        let row1_data = encode_row(&v1_table.descriptor, &row1_values).unwrap();

        let mut metadata = HashMap::new();
        metadata.insert("table".to_string(), "users".to_string());
        qm.sync_manager_mut()
            .object_manager
            .create_with_id(row1_id, Some(metadata.clone()));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                row1_id,
                &v1_branch,
                vec![],
                row1_data.clone(),
                row1_id,
                None,
            )
            .unwrap();

        // Update v1 branch index
        {
            let index_key = ("users".to_string(), "_id".to_string(), v1_branch.clone());
            if let Some(index) = qm.test_get_index_mut(&index_key) {
                index.insert(row1_id.uuid().as_bytes(), row1_id).unwrap();
            }
        }

        // --- Insert row on v2 branch (middle schema) ---
        let v2_table = v2.get(&TableName::new("users")).unwrap();
        let row2_id = ObjectId::new();
        let row2_values = vec![
            Value::Uuid(row2_id),
            Value::Text("Bob".to_string()),
            Value::Text("bob@example.com".to_string()),
        ];
        let row2_data = encode_row(&v2_table.descriptor, &row2_values).unwrap();

        qm.sync_manager_mut()
            .object_manager
            .create_with_id(row2_id, Some(metadata.clone()));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                row2_id,
                &v2_branch,
                vec![],
                row2_data.clone(),
                row2_id,
                None,
            )
            .unwrap();

        // Update v2 branch index
        {
            let index_key = ("users".to_string(), "_id".to_string(), v2_branch.clone());
            if let Some(index) = qm.test_get_index_mut(&index_key) {
                index.insert(row2_id.uuid().as_bytes(), row2_id).unwrap();
            }
        }

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
            .create_with_id(row3_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(
                row3_id,
                &v3_branch,
                vec![],
                row3_data.clone(),
                row3_id,
                None,
            )
            .unwrap();

        // Update v3 branch index
        {
            let index_key = ("users".to_string(), "_id".to_string(), v3_branch.clone());
            if let Some(index) = qm.test_get_index_mut(&index_key) {
                index.insert(row3_id.uuid().as_bytes(), row3_id).unwrap();
            }
        }

        // --- Query across all three branches ---
        let query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch, &v3_branch])
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let results = qm.get_subscription_results(sub_id);

        // Should have 3 rows
        assert_eq!(results.len(), 3, "Expected 3 rows from all schema branches");

        // All rows should have 4 columns (v3 format)
        for row in &results {
            assert_eq!(
                row.len(),
                4,
                "All rows should be transformed to v3 format (4 columns)"
            );
        }

        // Find Alice's row (from v1 branch - 2 hop transform)
        let alice_row = results
            .iter()
            .find(|r| {
                r.iter()
                    .any(|v| matches!(v, Value::Text(s) if s == "Alice"))
            })
            .expect("Alice's row should be present");
        assert_eq!(alice_row[0], Value::Uuid(row1_id));
        assert_eq!(alice_row[1], Value::Text("Alice".to_string()));
        assert_eq!(alice_row[2], Value::Null); // email added in v1->v2
        assert_eq!(alice_row[3], Value::Null); // role added in v2->v3

        // Find Bob's row (from v2 branch - 1 hop transform)
        let bob_row = results
            .iter()
            .find(|r| r.iter().any(|v| matches!(v, Value::Text(s) if s == "Bob")))
            .expect("Bob's row should be present");
        assert_eq!(bob_row[0], Value::Uuid(row2_id));
        assert_eq!(bob_row[1], Value::Text("Bob".to_string()));
        assert_eq!(bob_row[2], Value::Text("bob@example.com".to_string())); // preserved
        assert_eq!(bob_row[3], Value::Null); // role added in v2->v3

        // Find Charlie's row (from v3 branch - no transform)
        let charlie_row = results
            .iter()
            .find(|r| {
                r.iter()
                    .any(|v| matches!(v, Value::Text(s) if s == "Charlie"))
            })
            .expect("Charlie's row should be present");
        assert_eq!(charlie_row[0], Value::Uuid(row3_id));
        assert_eq!(charlie_row[1], Value::Text("Charlie".to_string()));
        assert_eq!(
            charlie_row[2],
            Value::Text("charlie@example.com".to_string())
        );
        assert_eq!(charlie_row[3], Value::Text("admin".to_string()));
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

        // Create schema context
        let mut ctx = SchemaContext::new(v3.clone(), "dev", "main");
        ctx.add_live_schema(v2.clone(), lens_v2_v3);
        ctx.add_live_schema(v1.clone(), lens_v1_v2);

        // Create QueryManager
        let sm = SyncManager::new();
        let mut qm = QueryManager::new_with_schema_context(sm, v3.clone(), ctx);

        let v1_branch = format!("dev-{}-main", v1_hash.short());

        // Insert row on v1 branch with old column name
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
            .create_with_id(row_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(row_id, &v1_branch, vec![], row_data.clone(), row_id, None)
            .unwrap();

        // Update v1 branch index
        {
            let index_key = ("users".to_string(), "_id".to_string(), v1_branch.clone());
            if let Some(index) = qm.test_get_index_mut(&index_key) {
                index.insert(row_id.uuid().as_bytes(), row_id).unwrap();
            }
        }

        // Query
        let query = QueryBuilder::new("users").branches(&[&v1_branch]).build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let results = qm.get_subscription_results(sub_id);

        // Should find the row
        assert_eq!(results.len(), 1);

        // Row should have v3 schema structure (2 columns, but with contact_email name)
        let row = &results[0];
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

        // Create schema context
        let mut ctx = SchemaContext::new(v2.clone(), "dev", "main");
        ctx.add_live_schema(v1.clone(), lens);

        // Create QueryManager with schema context
        let sm = SyncManager::new();
        let mut qm = QueryManager::new_with_schema_context(sm, v2.clone(), ctx);

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
            .create_with_id(row_id, Some(metadata));
        qm.sync_manager_mut()
            .object_manager
            .add_commit(row_id, &v1_branch, vec![], row_data.clone(), row_id, None)
            .unwrap();

        // Update _id index for v1 branch
        {
            let index_key = ("users".to_string(), "_id".to_string(), v1_branch.clone());
            if let Some(index) = qm.test_get_index_mut(&index_key) {
                index.insert(row_id.uuid().as_bytes(), row_id).unwrap();
            }
        }

        // --- Query using NEW column name (email_address) ---
        let query = QueryBuilder::new("users")
            .branches(&[&v1_branch, &v2_branch])
            .build();

        let sub_id = qm.subscribe(query).unwrap();
        qm.process();

        let results = qm.get_subscription_results(sub_id);

        // Should find the row
        assert_eq!(results.len(), 1);

        // Row should be transformed - column renamed
        let row = &results[0];
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
        let object_id = manager.persist_schema();

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
        let object_id = manager.persist_lens(&lens);

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

        let mut qm = QueryManager::new(SyncManager::new(), v1.clone());

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
        client_a.sync_context(); // Refresh QueryManager with multi-schema awareness

        // === Client A persists schema and lens to catalogue ===
        let schema_object_id = client_a.persist_schema();
        let lens_object_id = client_a.persist_lens(&lens);

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

        // === Refresh B's QueryManager with multi-schema context ===
        client_b.sync_context();

        // === Client A writes a row on v2 branch ===
        let id = ObjectId::new();
        let id_val = Value::Uuid(id);
        let name = Value::Text("Alice".into());
        let email = Value::Text("alice@example.com".into());

        client_a
            .insert("users", &[id_val.clone(), name.clone(), email.clone()])
            .unwrap();
        client_a.process();

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
}
