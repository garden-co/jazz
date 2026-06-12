#[cfg(feature = "test-utils")]
use crate::JazzClient;

use super::*;

#[test]
fn rebac_inherited_insert_uses_payload_branch_for_parent_lookup() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);

    // Server mode keeps current_branch() at "main", while the write arrives on
    // a composed client branch. The inherited parent lookup must use payload
    // branch context, not current_branch().
    let mut storage = seeded_memory_storage(&schema);
    let mut qm = create_server_mode_query_manager(schema, schema_hash);

    assert_ne!(qm.current_branch(), branch);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));

    let folder_id = seed_folder_on_branch(
        &mut qm,
        &mut storage,
        &branch,
        "alice",
        "Shared Folder",
        &folders_descriptor,
    );
    let doc_id = create_test_row(&mut storage, Some(document_metadata()));

    let mut scope = HashSet::new();
    scope.insert((doc_id, branch.clone().into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Allowed via folder ownership",
    );

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should use the payload branch to find the parent folder"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should be applied when the parent folder is visible on the payload branch"
    );
}

#[test]
fn rebac_inherited_insert_uses_payload_branch_after_cold_start() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);
    let mut storage = seeded_memory_storage(&schema);

    let mut seed_qm = create_server_mode_query_manager(schema.clone(), schema_hash);
    let folder_id = seed_folder_on_branch(
        &mut seed_qm,
        &mut storage,
        &branch,
        "alice",
        "Cold Folder",
        &folders_descriptor,
    );

    let mut qm = create_server_mode_query_manager(schema, schema_hash);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    let doc_id = ObjectId::new();
    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Cold-start branch lookup",
    );

    // Cold start:
    //   storage: folder exists only on the composed payload branch
    //   cache:   parent folder is not loaded at all
    //   write:   document insert must still authorize on that payload branch
    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should authorize on the payload branch even after a cold start"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should be applied after settlement reads the parent from the payload branch"
    );
}

#[test]
fn rebac_inherited_insert_uses_visible_row_region_after_legacy_branch_history_is_removed() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);
    let mut storage = seeded_memory_storage(&schema);

    let mut seed_qm = create_server_mode_query_manager(schema.clone(), schema_hash);
    let folder_id = seed_folder_on_branch(
        &mut seed_qm,
        &mut storage,
        &branch,
        "alice",
        "Cold Folder",
        &folders_descriptor,
    );

    let _folder_commit_id = *test_row_tip_ids(&storage, folder_id, &branch)
        .unwrap()
        .iter()
        .next()
        .expect("seed folder should have one tip");
    let mut qm = create_server_mode_query_manager(schema, schema_hash);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    let doc_id = ObjectId::new();
    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Cold-start branch lookup",
    );

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should authorize from the visible row region without legacy branch commits"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should still be applied after permission settlement"
    );
}

#[test]
fn rebac_inherited_insert_uses_requested_branch_instead_of_reusing_cached_branch() {
    let (schema, folders_descriptor, schema_hash) = inherited_insert_schema();
    let branch = inherited_insert_branch(schema_hash);
    let mut storage = seeded_memory_storage(&schema);

    let mut seed_qm = create_server_mode_query_manager(schema.clone(), schema_hash);
    let folder_id = seed_folder_on_branch(
        &mut seed_qm,
        &mut storage,
        "main",
        "bob",
        "Main Folder",
        &folders_descriptor,
    );
    add_row_commit(
        &mut storage,
        folder_id,
        &branch,
        vec![],
        encode_folder("alice", "Dev Folder"),
        1000,
        ObjectId::new().to_string(),
    );
    QueryManager::update_indices_for_insert_on_branch(
        &mut storage,
        "folders",
        &branch,
        folder_id,
        &encode_folder("alice", "Dev Folder"),
        &folders_descriptor,
        None,
    )
    .unwrap();
    seed_qm.persist_row_region_tip(&mut storage, "folders", folder_id, &branch);

    let mut qm = create_server_mode_query_manager(schema, schema_hash);
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    assert!(
        storage
            .load_visible_region_row("folders", "main", folder_id)
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .load_visible_region_row("folders", &branch, folder_id)
            .unwrap()
            .is_some(),
        "requested-branch visible state should already live in storage"
    );

    let doc_id = ObjectId::new();
    let commit = enqueue_inherited_insert(
        &mut qm,
        client_id,
        doc_id,
        &branch,
        folder_id,
        "Hydrate branch instead of reusing main",
    );

    // Wrong-branch reuse:
    //   storage: folder[main] = bob, folder[dev] = alice
    //   write:   document[dev] must consult folder[dev], not reuse folder[main]
    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        client_id,
        row_batch_id_for_commit(doc_id, &branch, &commit),
    );
    assert!(
        !denied,
        "Inherited insert should use the requested payload branch instead of reusing cached main"
    );

    let tips = test_row_tip_ids(&storage, doc_id, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(doc_id, &branch, &commit)),
        "Document insert should apply once the requested parent branch is consulted"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_inherits_filters_select_query_results() {
    // Schema with INHERITS policy
    let folders_table = TableSchema::builder("folders")
        .column("owner_id", ColumnType::Text)
        .column("name", ColumnType::Text);
    let folders_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("owner_id", pe::session("user_id")));
    });

    let docs_table = TableSchema::builder("documents")
        .column("owner_id", ColumnType::Text)
        .column("title", ColumnType::Text)
        .nullable_fk_column("folder_id", "folders");

    // SELECT policy: owner_id = @user_id OR INHERITS SELECT VIA folder_id
    let docs_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read("folder_id"),
        ]));
    });
    let schema = SchemaBuilder::new()
        .table(folders_table.policies(folders_policies))
        .table(docs_table.policies(docs_policies))
        .build();
    let client = JazzClient::test_client(schema).await;

    let folder_id = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "alice", "name" => "Alice's Folder"),
            None,
        )
        .expect("seed folder")
        .0;
    client
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "bob",
                "title" => "Bob's Doc in Alice's Folder",
                "folder_id" => folder_id,
            ),
            None,
        )
        .expect("seed document");

    let charlie_rows = client
        .for_session(Session::new("charlie"))
        .query(QueryBuilder::new("documents").build(), None)
        .await
        .expect("query documents as charlie");

    // Charlie should NOT see Bob's document (doesn't own it, doesn't own folder)
    assert!(
        charlie_rows.is_empty(),
        "Charlie should not see Bob's document - he owns neither the doc nor the folder. \
         INHERITS should have denied access, but currently it always returns true."
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn inherits_select_denies_when_parent_operation_policy_is_missing() {
    let documents_policies = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("folder_id"));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text),
        )
        .table(
            TableSchema::builder("documents")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(documents_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    let folder = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "alice", "name" => "Shared"),
            None,
        )
        .expect("folder insert should succeed");
    client
        .insert(
            "documents",
            crate::row_input!("owner_id" => "bob", "title" => "Inherited doc", "folder_id" => folder.0),
            None,
        )
        .expect("document insert should succeed");

    let rows = client
        .for_session(Session::new("alice"))
        .query(
            QueryBuilder::new("documents").select(&["title"]).build(),
            None,
        )
        .await
        .expect("query documents as alice");

    assert!(
        rows.is_empty(),
        "child rows should be denied when INHERITS reaches a parent table with no explicit SELECT policy"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn local_insert_with_inherits_policy_allows_missing_parent_policy_in_permissive_local() {
    let documents_policies = permissions(|p| {
        p.allow_insert().where_(pe::allowed_to_insert("folder_id"));
    });
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("folders").column("title", ColumnType::Text))
        .table(
            TableSchema::builder("documents")
                .column("title", ColumnType::Text)
                .nullable_fk_column("folder_id", "folders")
                .policies(documents_policies),
        )
        .build();

    let client = JazzClient::permissive_test_client(schema).await;

    let folder = client
        .insert(
            "folders",
            crate::row_input!("title" => "alice folder"),
            None,
        )
        .expect("seed folder row");

    client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!("title" => "draft doc", "folder_id" => folder.0),
        )
    .expect(
        "permissive local runtimes should treat missing parent INSERT policy as allow for INHERITS",
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn local_update_with_inherits_referencing_allows_missing_source_policy_in_permissive_local() {
    let files_policies = permissions(|p| {
        p.allow_update()
            .where_old(pe::allowed_to_update_referencing("todos", "file_id"))
            .where_new(pe::always());
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("files")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .policies(files_policies),
        )
        .table(
            TableSchema::builder("todos")
                .column("owner_id", ColumnType::Text)
                .column("title", ColumnType::Text)
                .nullable_fk_column("file_id", "files"),
        )
        .build();

    let client = JazzClient::permissive_test_client(schema).await;

    let file = client
        .insert(
            "files",
            crate::row_input!("owner_id" => "bob", "name" => "shared-file"),
            None,
        )
        .expect("seed file row");
    client
        .insert(
            "todos",
            crate::row_input!(
                "owner_id" => "alice",
                "title" => "todo referencing file",
                "file_id" => file.0,
            ),
            None,
        )
        .expect("seed referencing todo row");

    client
        .for_session(Session::new("alice"))
        .update(
            file.0,
            vec![
                ("owner_id".into(), Value::Text("bob".into())),
                ("name".into(), Value::Text("updated by alice".into())),
            ],
        )
    .expect(
        "permissive local runtimes should treat missing source UPDATE policy as allow for INHERITS_REFERENCING",
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn local_update_with_check_inherits_denies_when_parent_is_not_updateable() {
    let folders_policies = permissions(|p| {
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session("user_id")))
            .where_new(pe::allowed_to_update_with_depth("parent_id", 10));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folders_policies),
        )
        .build();
    let client = JazzClient::test_client(schema).await;

    let root = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "alice", "name" => "Root", "parent_id" => Value::Null),
            None,
        )
        .expect("create root");
    let child = client
        .insert(
            "folders",
            crate::row_input!("owner_id" => "bob", "name" => "Child", "parent_id" => root.0),
            None,
        )
        .expect("create child");

    let update_err = client
        .for_session(Session::new("bob"))
        .update(
            child.0,
            vec![
                ("owner_id".into(), Value::Text("bob".into())),
                ("name".into(), Value::Text("Child renamed".into())),
                ("parent_id".into(), Value::Uuid(root.0)),
            ],
        )
        .expect_err("update should fail inherited WITH CHECK");
    assert_client_policy_denied(update_err, "folders", Operation::Update);
}

#[test]
fn local_update_with_check_inherits_uses_visible_row_region_after_legacy_branch_history_is_removed()
{
    let folders_policies = permissions(|p| {
        p.allow_update()
            .where_old(pe::eq("owner_id", pe::session("user_id")))
            .where_new(pe::allowed_to_update_with_depth("parent_id", 10));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("folders")
                .column("owner_id", ColumnType::Text)
                .column("name", ColumnType::Text)
                .nullable_fk_column("parent_id", "folders")
                .policies(folders_policies),
        )
        .build();

    let mut writer_qm = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);
    let mut storage = seeded_memory_storage(&writer_qm.schema_context().current_schema);

    let root = writer_qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .expect("create root");
    let child = writer_qm
        .insert(
            &mut storage,
            "folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root.row_id),
            ],
        )
        .expect("create child");

    let mut qm = create_query_manager(SyncManager::new(), schema);
    let update_err = qm
        .update_with_session(
            &mut storage,
            child.row_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child renamed".into()),
                Value::Uuid(root.row_id),
            ],
            Some(&Session::new("bob")),
        )
        .expect_err("update should still evaluate inherited WITH CHECK from visible rows");
    assert!(matches!(
        update_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("folders")
    ));
}

#[test]
fn rebac_inherits_no_cycle_passes() {
    use crate::query_manager::types::validate_no_inherits_cycles;

    let org_policy = permissions(|p| {
        p.allow_read().where_(pe::eq("name", pe::session("org")));
    });

    let team_policy = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("org_id"));
    });

    let project_policy = permissions(|p| {
        p.allow_read().where_(pe::allowed_to_read("team_id"));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("orgs")
                .column("name", ColumnType::Text)
                .policies(org_policy),
        )
        .table(
            TableSchema::builder("teams")
                .column("name", ColumnType::Text)
                .nullable_fk_column("org_id", "orgs")
                .policies(team_policy),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .nullable_fk_column("team_id", "teams")
                .policies(project_policy),
        )
        .build();

    // Should pass - this is a valid chain: projects → teams → orgs
    let result = validate_no_inherits_cycles(&schema);
    assert!(
        result.is_ok(),
        "Valid INHERITS chain should pass validation: {:?}",
        result
    );
}
