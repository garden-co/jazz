use super::*;

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
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
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
    let v1_data = encode_row(&v1_table.columns, &v1_values).unwrap();

    let transformer = manager.transformer("users");
    let result = transformer
        .transform(&v1_data, make_commit_id(1), v1_hash)
        .unwrap();

    assert!(result.was_transformed);

    // Verify - column value should be preserved under new name
    let v2_table = v2.get(&TableName::new("users")).unwrap();
    let v2_values = decode_row(&v2_table.columns, &result.data).unwrap();

    assert_eq!(v2_values[0], Value::Uuid(id));
    assert_eq!(v2_values[1], Value::Text("alice@example.com".to_string()));
}

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
    let row_data = encode_row(&v1_table.columns, &row_values).unwrap();

    let mut storage = MemoryStorage::new();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        row_id,
        &v1_branch,
        row_data,
        1_000,
    );

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
    let row_data = encode_row(&v1_table.columns, &row_values).unwrap();

    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        row_id,
        &v1_branch,
        row_data,
        1_000,
    );

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

#[test]
fn end_to_end_table_rename_translation() {
    // v1 branch: users(id, email)
    //            |
    //            | RenameTable users -> people
    //            v
    // v2 branch: people(id, email)
    //
    // Querying `people` across both branches should still find the v1 row.
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);
    let mut storage = MemoryStorage::new();

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let row_id = ObjectId::new();
    let row_values = vec![
        Value::Uuid(row_id),
        Value::Text("alice@example.com".to_string()),
    ];
    let row_data = encode_row(&v1_table.columns, &row_values).unwrap();

    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        row_id,
        &v1_branch,
        row_data,
        1_000,
    );

    let query = QueryBuilder::new("people")
        .branches(&[&v1_branch, &v2_branch])
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);

    assert_eq!(results.len(), 1);

    let (_, row) = &results[0];
    assert_eq!(row.len(), 2);
    assert_eq!(row[0], Value::Uuid(row_id));
    assert_eq!(row[1], Value::Text("alice@example.com".to_string()));
}

#[test]
fn table_rename_subscription_reacts_to_old_branch_updates() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);
    let mut storage = MemoryStorage::new();

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    let query = QueryBuilder::new("people")
        .branches(&[&v1_branch, &v2_branch])
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    assert!(
        qm.get_subscription_results(sub_id).is_empty(),
        "query should start empty before any rows are synced"
    );

    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let row_id = ObjectId::new();
    let row_values = vec![
        Value::Uuid(row_id),
        Value::Text("alice@example.com".to_string()),
    ];
    let row_data = encode_row(&v1_table.columns, &row_values).unwrap();

    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        row_id,
        &v1_branch,
        row_data,
        1_000,
    );

    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, row_id);
    assert_eq!(
        results[0].1[1],
        Value::Text("alice@example.com".to_string())
    );
}

#[test]
fn table_rename_subscription_reacts_to_new_branch_updates_after_schema_evolution() {
    // v1 current: users(id, email) -- subscribe(users on v1 branch)
    //                                |
    //                                | add live schema v2 + RenameTable users -> people
    //                                v
    // v2 live:    people(id, email) -- ingest people row on v2 branch
    //
    // The original `users` subscription should be recompiled to read both branches
    // and surface the v2 row through the rename lens.
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v1.clone(), "dev", "main");
    let mut storage = MemoryStorage::new();

    let v2_branch = format!("dev-{}-main", v2_hash.short());

    let query = QueryBuilder::new("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    assert!(
        qm.get_subscription_results(sub_id).is_empty(),
        "query should start empty before any rows are synced"
    );

    qm.add_live_schema(v2.clone());
    qm.register_lens(lens);

    let v2_table = v2.get(&TableName::new("people")).unwrap();
    let row_id = ObjectId::new();
    let row_values = vec![
        Value::Uuid(row_id),
        Value::Text("alice@example.com".to_string()),
    ];
    let row_data = encode_row(&v2_table.columns, &row_values).unwrap();

    ingest_remote_row(
        &mut qm,
        &mut storage,
        "people",
        v2_hash,
        row_id,
        &v2_branch,
        row_data,
        1_000,
    );

    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, row_id);
    assert_eq!(
        results[0].1[1],
        Value::Text("alice@example.com".to_string())
    );
}

#[test]
fn table_rename_update_and_delete_copy_on_write() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema_with_lens(v1.clone(), lens).unwrap();

    let mut storage = MemoryStorage::new();
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let current_branch = manager.branch_name().as_str().to_string();

    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let row_id = ObjectId::new();
    let row_data = encode_row(
        &v1_table.columns,
        &[
            Value::Uuid(row_id),
            Value::Text("alice@example.com".to_string()),
        ],
    )
    .unwrap();

    ingest_remote_row(
        manager.query_manager_mut(),
        &mut storage,
        "users",
        v1_hash,
        row_id,
        &v1_branch,
        row_data,
        1_000,
    );
    manager.process(&mut storage);

    manager
        .update_with_session(
            &mut storage,
            row_id,
            &[(
                "email".to_string(),
                Value::Text("alice+updated@example.com".to_string()),
            )],
            None,
        )
        .expect("rename-aware copy-on-write update should succeed");

    assert!(
        manager.query_manager().row_is_indexed_on_branch(
            &storage,
            "people",
            &current_branch,
            row_id
        ),
        "updated row should be indexed on the renamed table in the current branch"
    );
    assert!(
        !manager.query_manager().row_is_indexed_on_branch(
            &storage,
            "users",
            &current_branch,
            row_id
        ),
        "current-branch indices should use the renamed table name"
    );

    let current_results = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("people")
            .branch(current_branch.clone())
            .build(),
    );
    assert_eq!(current_results.len(), 1);
    assert_eq!(current_results[0].0, row_id);
    assert!(
        current_results[0]
            .1
            .iter()
            .any(|value| value == &Value::Text("alice+updated@example.com".to_string())),
        "updated row should carry the new email in its current-branch projection"
    );

    manager
        .delete(&mut storage, row_id, None)
        .expect("rename-aware copy-on-write delete should succeed");

    assert!(
        manager.query_manager().row_is_deleted_on_branch(
            &storage,
            "people",
            &current_branch,
            row_id
        ),
        "soft delete should be indexed under the renamed current-branch table"
    );

    let visible_after_delete = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("people")
            .branch(current_branch.clone())
            .build(),
    );
    assert!(
        visible_after_delete.is_empty(),
        "soft-deleted row should no longer appear in current-branch people queries"
    );
}

#[test]
fn transactional_insert_uses_frozen_target_branch_renamed_table_schema() {
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let mut manager =
        SchemaManager::new(SyncManager::new(), v2.clone(), test_app_id(), "dev", "main").unwrap();
    manager.add_live_schema_with_lens(v1.clone(), lens).unwrap();

    let mut storage = MemoryStorage::new();
    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let current_branch = manager.branch_name().as_str().to_string();

    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_target_branch_name(v1_branch.clone());

    let inserted = manager
        .insert_with_write_context(
            &mut storage,
            "users",
            HashMap::from([
                ("id".to_string(), Value::Uuid(ObjectId::new())),
                (
                    "email".to_string(),
                    Value::Text("alice@example.com".to_string()),
                ),
            ]),
            Some(&write_context),
        )
        .expect("frozen-target insert should use the renamed target table schema");

    let target_rows = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("people")
            .branch(v1_branch.clone())
            .build(),
    );
    assert!(
        target_rows.is_empty(),
        "uncommitted transactional insert should not be visible to ordinary reads"
    );

    let staged_row = load_single_staged_history_row(&storage, "users", &v1_branch, inserted.row_id);
    assert_eq!(staged_row.row_id, inserted.row_id);
    let v1_table = v1.get(&TableName::new("users")).unwrap();
    let staged_values = decode_row(&v1_table.columns, &staged_row.data).unwrap();
    assert!(
        staged_values
            .iter()
            .any(|value| value == &Value::Text("alice@example.com".to_string()))
    );

    let current_rows = execute_query(
        &mut manager,
        &mut storage,
        QueryBuilder::new("people").branch(current_branch).build(),
    );
    assert!(
        current_rows.is_empty(),
        "insert should stay on the frozen renamed-table target branch"
    );
}

#[test]
fn table_rename_join_query_translates_join_target_on_old_branch() {
    // v1 branch: posts.author_id -> users.id
    //                            |
    //                            | RenameTable users -> people
    //                            v
    // v2 query: posts JOIN people ON posts.author_id = people.id
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
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Uuid)
                .fk_column("author_id", "people")
                .column("title", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);
    let mut storage = MemoryStorage::new();

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    let author_id = ObjectId::new();
    let v1_users = v1.get(&TableName::new("users")).unwrap();
    let user_data = encode_row(
        &v1_users.columns,
        &[Value::Uuid(author_id), Value::Text("Alice".to_string())],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        author_id,
        &v1_branch,
        user_data,
        1_000,
    );

    let post_id = ObjectId::new();
    let v1_posts = v1.get(&TableName::new("posts")).unwrap();
    let post_data = encode_row(
        &v1_posts.columns,
        &[
            Value::Uuid(post_id),
            Value::Uuid(author_id),
            Value::Text("Hello from v1".to_string()),
        ],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "posts",
        v1_hash,
        post_id,
        &v1_branch,
        post_data,
        1_100,
    );

    let query = QueryBuilder::new("posts")
        .branches(&[&v1_branch, &v2_branch])
        .join("people")
        .on("posts.author_id", "people.id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1);

    let (_, row) = &results[0];
    assert_eq!(row.len(), 5);
    assert_eq!(row[0], Value::Uuid(post_id));
    assert_eq!(row[1], Value::Uuid(author_id));
    assert_eq!(row[2], Value::Text("Hello from v1".to_string()));
    assert_eq!(row[3], Value::Uuid(author_id));
    assert_eq!(row[4], Value::Text("Alice".to_string()));
}

#[test]
fn table_rename_fk_array_lookup_finds_related_rows_on_old_branch() {
    // v1 branch: users <- posts.author_id
    //            |
    //            | RenameTable users -> people
    //            v
    // v2 query: people.with_array(posts where posts.author_id = people.id)
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
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("posts")
                .column("id", ColumnType::Uuid)
                .fk_column("author_id", "people")
                .column("title", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);

    let mut transform = LensTransform::new();
    transform.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens = Lens::new(v1_hash, v2_hash, transform);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v2.clone(), "dev", "main");
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens);
    let mut storage = MemoryStorage::new();

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());

    let author_id = ObjectId::new();
    let v1_users = v1.get(&TableName::new("users")).unwrap();
    let user_data = encode_row(
        &v1_users.columns,
        &[Value::Uuid(author_id), Value::Text("Alice".to_string())],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        author_id,
        &v1_branch,
        user_data,
        1_000,
    );

    let post_id = ObjectId::new();
    let v1_posts = v1.get(&TableName::new("posts")).unwrap();
    let post_data = encode_row(
        &v1_posts.columns,
        &[
            Value::Uuid(post_id),
            Value::Uuid(author_id),
            Value::Text("Alice post".to_string()),
        ],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "posts",
        v1_hash,
        post_id,
        &v1_branch,
        post_data,
        1_100,
    );

    let query = QueryBuilder::new("people")
        .branches(&[&v1_branch, &v2_branch])
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "people.id")
        })
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1);

    let (_, row) = &results[0];
    assert_eq!(row[0], Value::Uuid(author_id));
    assert_eq!(row[1], Value::Text("Alice".to_string()));

    let posts = row[2]
        .as_array()
        .expect("third column should be posts array");
    assert_eq!(posts.len(), 1);
    let first_post = posts[0]
        .as_row()
        .expect("post array element should be a row");
    assert_eq!(first_post[0], Value::Uuid(post_id));
    assert_eq!(first_post[1], Value::Uuid(author_id));
    assert_eq!(first_post[2], Value::Text("Alice post".to_string()));
}

#[test]
fn multi_hop_table_renames_and_column_rename() {
    // v1: users(id, email)
    //      |
    //      | RenameTable users -> people
    //      v
    // v2: people(id, email)
    //      |
    //      | RenameTable people -> members
    //      | RenameColumn email -> email_address
    //      v
    // v3: members(id, email_address)
    let v1 = SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v2 = SchemaBuilder::new()
        .table(
            TableSchema::builder("people")
                .column("id", ColumnType::Uuid)
                .column("email", ColumnType::Text),
        )
        .build();

    let v3 = SchemaBuilder::new()
        .table(
            TableSchema::builder("members")
                .column("id", ColumnType::Uuid)
                .column("email_address", ColumnType::Text),
        )
        .build();

    let v1_hash = SchemaHash::compute(&v1);
    let v2_hash = SchemaHash::compute(&v2);
    let v3_hash = SchemaHash::compute(&v3);

    let mut transform_v1_v2 = LensTransform::new();
    transform_v1_v2.push(
        LensOp::RenameTable {
            old_name: "users".to_string(),
            new_name: "people".to_string(),
        },
        false,
    );
    let lens_v1_v2 = Lens::new(v1_hash, v2_hash, transform_v1_v2);

    let mut transform_v2_v3 = LensTransform::new();
    transform_v2_v3.push(
        LensOp::RenameTable {
            old_name: "people".to_string(),
            new_name: "members".to_string(),
        },
        false,
    );
    transform_v2_v3.push(
        LensOp::RenameColumn {
            table: "members".to_string(),
            old_name: "email".to_string(),
            new_name: "email_address".to_string(),
        },
        false,
    );
    let lens_v2_v3 = Lens::new(v2_hash, v3_hash, transform_v2_v3);

    let sm = SyncManager::new();
    let mut qm = QueryManager::new(sm);
    qm.set_current_schema(v3.clone(), "dev", "main");
    qm.add_live_schema(v2.clone());
    qm.register_lens(lens_v2_v3);
    qm.add_live_schema(v1.clone());
    qm.register_lens(lens_v1_v2);

    let v1_branch = format!("dev-{}-main", v1_hash.short());
    let v2_branch = format!("dev-{}-main", v2_hash.short());
    let v3_branch = format!("dev-{}-main", v3_hash.short());
    let mut storage = MemoryStorage::new();

    let alice_id = ObjectId::new();
    let v1_users = v1.get(&TableName::new("users")).unwrap();
    let alice_data = encode_row(
        &v1_users.columns,
        &[
            Value::Uuid(alice_id),
            Value::Text("alice@example.com".to_string()),
        ],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "users",
        v1_hash,
        alice_id,
        &v1_branch,
        alice_data,
        1_000,
    );

    let bob_id = ObjectId::new();
    let v2_people = v2.get(&TableName::new("people")).unwrap();
    let bob_data = encode_row(
        &v2_people.columns,
        &[
            Value::Uuid(bob_id),
            Value::Text("bob@example.com".to_string()),
        ],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "people",
        v2_hash,
        bob_id,
        &v2_branch,
        bob_data,
        1_100,
    );

    let carol_id = ObjectId::new();
    let v3_members = v3.get(&TableName::new("members")).unwrap();
    let carol_data = encode_row(
        &v3_members.columns,
        &[
            Value::Uuid(carol_id),
            Value::Text("carol@example.com".to_string()),
        ],
    )
    .unwrap();
    ingest_remote_row(
        &mut qm,
        &mut storage,
        "members",
        v3_hash,
        carol_id,
        &v3_branch,
        carol_data,
        1_200,
    );

    let query = QueryBuilder::new("members")
        .branches(&[&v1_branch, &v2_branch, &v3_branch])
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 3);

    for (_, row) in &results {
        assert_eq!(row.len(), 2);
    }

    assert!(results.iter().any(|(_, row)| {
        row[0] == Value::Uuid(alice_id) && row[1] == Value::Text("alice@example.com".to_string())
    }));
    assert!(results.iter().any(|(_, row)| {
        row[0] == Value::Uuid(bob_id) && row[1] == Value::Text("bob@example.com".to_string())
    }));
    assert!(results.iter().any(|(_, row)| {
        row[0] == Value::Uuid(carol_id) && row[1] == Value::Text("carol@example.com".to_string())
    }));
}
