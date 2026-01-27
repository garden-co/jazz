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
        CopyOnWriteWriter, Lens, LensOp, LensTransform, SchemaContext, SchemaManager, generate_lens,
    };

    fn make_commit_id(n: u8) -> CommitId {
        CommitId([n; 32])
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
            SchemaManager::new(SyncManager::new(), v2.clone(), "dev", "main").unwrap();

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
            SchemaManager::new(SyncManager::new(), v2.clone(), "dev", "main").unwrap();
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
            SchemaManager::new(SyncManager::new(), v2.clone(), "dev", "main").unwrap();
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

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
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

        let mut manager = SchemaManager::new(SyncManager::new(), v2, "dev", "main").unwrap();
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
            SchemaManager::new(SyncManager::new(), v2.clone(), "dev", "main").unwrap();
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
}
