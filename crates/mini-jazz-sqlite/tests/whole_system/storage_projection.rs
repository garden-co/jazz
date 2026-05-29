use super::*;

#[test]
fn memory_runtime_writes_through_sqlite_current_projection() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    assert_eq!(alice.storage_format_version().unwrap(), 12);

    alice.create_project("project-1", "Spec work").unwrap();
    let tx = alice
        .create_todo("todo-1", "Write Attempt 3 tests", false, "project-1")
        .unwrap();

    let todos = alice.open_todos().unwrap();
    assert_eq!(todos.len(), 1);
    assert_eq!(todos[0].id, "todo-1");
    assert_eq!(todos[0].title, "Write Attempt 3 tests");
    assert_eq!(todos[0].project_title.as_deref(), Some("Spec work"));
    assert_eq!(todos[0].created_by, "alice");

    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.sealed_history_rows, 0);
    assert_eq!(stats.history_blocks, 0);
    assert_eq!(stats.current_rows, 2);
    assert!(stats.page_count > 0);
    assert!(stats.page_size > 0);
    assert_eq!(stats.database_bytes, stats.page_count * stats.page_size);
    assert!(stats.physical_tx_num_for(&tx).is_some());
}

#[test]
fn durable_storage_is_tagged_with_format_version() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("versioned.sqlite");

    Runtime::open(Storage::File(path.clone()), "worker", "alice").unwrap();

    let conn = rusqlite::Connection::open(path).unwrap();
    let version: i64 = conn
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(version, 12);
}

#[test]
fn future_storage_format_versions_fail_before_opening_runtime() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("future.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 13).unwrap();
    drop(conn);

    let err = match Runtime::open(Storage::File(path), "worker", "alice") {
        Ok(_) => panic!("future storage format opened successfully"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("unsupported storage format version 13"));
}

#[test]
fn system_user_metadata_is_interned_but_app_user_fields_stay_text() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("interned-users.sqlite");
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("owner_id");
        table.text("title");
    });

    {
        let mut alice =
            Runtime::open_with_schema(Storage::File(path.clone()), "alice-node", "alice", schema)
                .unwrap();
        alice
            .insert_row(
                "docs",
                "doc-1",
                BTreeMap::from([
                    ("owner_id".to_owned(), json!("user-visible-owner")),
                    ("title".to_owned(), json!("Intern system users")),
                ]),
            )
            .unwrap();
        let row = alice.read_rows("docs").unwrap().remove(0);
        assert_eq!(row.created_by, "alice");
        assert_eq!(row.values["owner_id"], json!("user-visible-owner"));
    }

    let conn = rusqlite::Connection::open(path).unwrap();
    let created_by_type: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('docs__schema_v1_history') WHERE name = 'j_created_by'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let owner_type: String = conn
        .query_row(
            "SELECT type FROM pragma_table_info('docs__schema_v1_history') WHERE name = 'owner_id'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored_created_by: i64 = conn
        .query_row(
            "SELECT j_created_by FROM docs__schema_v1_history WHERE row_num = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored_owner_id: String = conn
        .query_row(
            "SELECT owner_id FROM docs__schema_v1_history WHERE row_num = 1",
            [],
            |row| row.get(0),
        )
        .unwrap();
    let stored_user_id: String = conn
        .query_row(
            "SELECT user_id FROM jazz_user WHERE user_num = ?",
            [stored_created_by],
            |row| row.get(0),
        )
        .unwrap();

    assert_eq!(created_by_type, "INTEGER");
    assert_eq!(owner_type, "TEXT");
    assert_eq!(stored_user_id, "alice");
    assert_eq!(stored_owner_id, "user-visible-owner");
}

#[test]
fn previous_storage_format_versions_fail_fast() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("old.sqlite");
    let conn = rusqlite::Connection::open(&path).unwrap();
    conn.pragma_update(None, "user_version", 1).unwrap();
    drop(conn);

    let err = match Runtime::open(Storage::File(path), "worker", "alice") {
        Ok(_) => panic!("old storage format opened successfully"),
        Err(err) => err,
    };

    assert!(err
        .to_string()
        .contains("unsupported storage format version 1"));
}

#[test]
fn durable_nodes_survive_reopen_but_memory_nodes_start_empty() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("durable.sqlite");

    {
        let mut durable = Runtime::open(Storage::File(path.clone()), "worker", "alice").unwrap();
        durable.create_project("project-1", "Durable").unwrap();
        durable
            .create_todo("todo-1", "Survives reopen", false, "project-1")
            .unwrap();
    }

    let reopened = Runtime::open(Storage::File(path), "worker", "alice").unwrap();
    assert_eq!(reopened.open_todos().unwrap().len(), 1);

    let mut memory = Runtime::open(Storage::Memory, "tab", "alice").unwrap();
    memory.create_project("project-1", "Memory").unwrap();
    memory
        .create_todo("todo-1", "Lost on restart", false, "project-1")
        .unwrap();
    assert_eq!(memory.open_todos().unwrap().len(), 1);

    let fresh_memory = Runtime::open(Storage::Memory, "tab", "alice").unwrap();
    assert!(fresh_memory.open_todos().unwrap().is_empty());
}

#[test]
fn rebuild_current_projection_from_history_matches_current_reads() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Rebuild me", false, "project-1")
        .unwrap();
    let before = alice.open_todos().unwrap();

    alice.clear_current_projection_for_test().unwrap();
    assert!(alice.open_todos().unwrap().is_empty());

    alice.rebuild_current_projection().unwrap();
    assert_eq!(alice.open_todos().unwrap(), before);
}

#[test]
fn accepted_history_compaction_seals_old_versions_without_changing_exports() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for idx in 2..=6 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }

    let before_rows = alice.read_rows("notes").unwrap();
    let before_bundle = alice.export_table_history("notes").unwrap();
    assert_eq!(before_bundle.history.len(), 6);
    let archived_tx_info = alice.transaction_info("tx-alice-node-2").unwrap();
    let archived_tx_writes = alice.transaction_write_rows("tx-alice-node-2").unwrap();
    let archived_tx_previous_reads = alice
        .transaction_previous_read_rows("tx-alice-node-2")
        .unwrap();

    let compacted = alice
        .compact_accepted_history("notes", "note-1", 2)
        .unwrap();
    assert_eq!(compacted.sealed_history_rows, 4);
    assert_eq!(compacted.history_blocks, 1);
    assert!(compacted.sealed_transactions > 0);

    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 2);
    assert_eq!(stats.sealed_history_rows, 4);
    assert_eq!(stats.history_blocks, 1);
    assert_eq!(
        stats.history_block_uncompressed_bytes,
        compacted.uncompressed_bytes
    );
    assert_eq!(
        stats.history_block_compressed_bytes,
        compacted.compressed_bytes
    );
    let manifests = alice.history_block_manifests("notes").unwrap();
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].kind, "accepted");
    assert_eq!(manifests[0].row_id, "note-1");
    assert_eq!(manifests[0].row_count, 4);
    assert!(manifests[0].compressed_bytes > 0);
    assert_eq!(manifests[0].payload_sha256.len(), 64);
    assert_eq!(alice.read_rows("notes").unwrap(), before_rows);
    assert_eq!(
        alice.transaction_info("tx-alice-node-2").unwrap(),
        archived_tx_info
    );
    assert_eq!(
        alice.transaction_write_rows("tx-alice-node-2").unwrap(),
        archived_tx_writes
    );
    assert_eq!(
        alice
            .transaction_previous_read_rows("tx-alice-node-2")
            .unwrap(),
        archived_tx_previous_reads
    );

    let after_bundle = alice.export_table_history("notes").unwrap();
    assert_eq!(after_bundle.history, before_bundle.history);
    assert_eq!(after_bundle.txs, before_bundle.txs);
    assert_eq!(after_bundle.reads, before_bundle.reads);

    bob.apply_bundle(&after_bundle).unwrap();
    assert_eq!(bob.read_rows("notes").unwrap(), before_rows);
}

#[test]
fn history_blocks_can_sync_as_raw_blocks_without_reopening_rows() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for idx in 2..=5 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }
    let before_bundle = alice.export_table_history("notes").unwrap();
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let blocks = alice.export_history_blocks("notes").unwrap();
    assert_eq!(blocks.len(), 1);
    assert_eq!(blocks[0].manifest.payload_sha256.len(), 64);
    let alice_manifests = alice.all_history_block_manifests().unwrap();
    let missing = bob
        .missing_history_block_manifests(&alice_manifests)
        .unwrap();
    assert_eq!(missing, alice_manifests);
    assert_eq!(
        alice.export_history_blocks_matching(&missing).unwrap(),
        blocks
    );
    let mut duplicate_missing = missing.clone();
    duplicate_missing.extend(missing.clone());
    assert_eq!(
        alice
            .export_history_blocks_matching(&duplicate_missing)
            .unwrap(),
        blocks
    );
    let encoded_blocks = serde_json::to_vec(&blocks).unwrap();
    let encoded_json = String::from_utf8(encoded_blocks.clone()).unwrap();
    assert!(encoded_json.contains("\"payload\":\""));
    let decoded_blocks: Vec<HistoryBlockExport> = serde_json::from_slice(&encoded_blocks).unwrap();
    assert_eq!(decoded_blocks, blocks);
    assert_eq!(decoded_blocks[0].payload, blocks[0].payload);

    let mut tampered = blocks.clone();
    tampered[0].payload[0] ^= 1;
    let err = bob.import_history_blocks(&tampered).unwrap_err();
    assert!(err.to_string().contains("payload hash mismatch"));
    let mut wrong_row_count = blocks.clone();
    wrong_row_count[0].manifest.row_count += 1;
    let err = bob.import_history_blocks(&wrong_row_count).unwrap_err();
    assert!(err.to_string().contains("row count mismatch"));
    let mut wrong_uncompressed_count = blocks.clone();
    wrong_uncompressed_count[0].manifest.uncompressed_bytes += 1;
    let err = bob
        .import_history_blocks(&wrong_uncompressed_count)
        .unwrap_err();
    assert!(err.to_string().contains("uncompressed byte count mismatch"));
    let mut invalid_range = blocks.clone();
    invalid_range[0].tx_ranges[0].min_local_epoch =
        invalid_range[0].tx_ranges[0].max_local_epoch + 1;
    let err = bob.import_history_blocks(&invalid_range).unwrap_err();
    assert!(
        err.to_string().contains("invalid tx range"),
        "unexpected error: {err}"
    );
    let mut wrong_range = blocks.clone();
    wrong_range[0].tx_ranges[0].max_local_epoch += 1;
    let err = bob.import_history_blocks(&wrong_range).unwrap_err();
    assert!(err.to_string().contains("tx range mismatch"));
    let mut duplicate_range = blocks.clone();
    let duplicate_tx_range = duplicate_range[0].tx_ranges[0].clone();
    duplicate_range[0].tx_ranges.push(duplicate_tx_range);
    let err = bob.import_history_blocks(&duplicate_range).unwrap_err();
    assert!(err.to_string().contains("duplicate tx range"));
    assert_eq!(bob.import_history_blocks(&blocks).unwrap(), 1);
    assert_eq!(bob.import_history_blocks(&blocks).unwrap(), 0);
    assert!(bob
        .missing_history_block_manifests(&alice_manifests)
        .unwrap()
        .is_empty());

    assert_eq!(bob.storage_stats().unwrap().history_rows, 0);
    assert_eq!(bob.storage_stats().unwrap().sealed_history_rows, 4);
    assert!(bob
        .storage_stats()
        .unwrap()
        .physical_tx_num_for("tx-alice-node-2")
        .is_none());
    assert_eq!(
        bob.transaction_info("tx-alice-node-2").unwrap(),
        alice.transaction_info("tx-alice-node-2").unwrap()
    );
    assert_eq!(
        bob.transaction_write_rows("tx-alice-node-2").unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );

    let sealed_only = bob.export_table_history("notes").unwrap();
    assert_eq!(sealed_only.history.len(), 4);
    assert_eq!(sealed_only.history, before_bundle.history[..4].to_vec());

    bob.rebuild_current_projection().unwrap();
    let rebuilt = bob.read_rows("notes").unwrap();
    assert_eq!(rebuilt.len(), 1);
    assert_eq!(rebuilt[0].values["body"], json!("v4"));
}

#[test]
fn table_history_delta_syncs_open_rows_and_missing_blocks() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for idx in 2..=5 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let delta = alice.export_table_history_delta("notes", &[]).unwrap();
    assert_eq!(delta.bundle.history.len(), 1);
    assert_eq!(delta.blocks.len(), 1);

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();

    let rows = bob.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("v5"));
    assert_eq!(rows[0].values["pinned"], json!(false));
    assert_eq!(bob.storage_stats().unwrap().history_rows, 1);
    assert_eq!(bob.storage_stats().unwrap().sealed_history_rows, 4);

    let receiver_inventory = bob.all_history_block_manifests().unwrap();
    let already_have = alice
        .export_table_history_delta("notes", &receiver_inventory)
        .unwrap();
    assert!(already_have.blocks.is_empty());
}

#[test]
fn query_history_delta_syncs_open_rows_and_matching_blocks() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    for idx in 2..=5 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("other")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let delta = alice
        .export_query_where_eq_history_delta("notes", "pinned", json!(true), &[])
        .unwrap();
    assert_eq!(delta.bundle.history.len(), 1);
    assert_eq!(delta.blocks.len(), 1);
    assert_eq!(delta.blocks[0].manifest.row_id, "note-1");

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();

    let rows = bob
        .read_rows_where_eq("notes", "pinned", json!(true))
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-1");
    assert_eq!(rows[0].values["body"], json!("v5"));

    let already_have = alice
        .export_query_where_eq_history_delta(
            "notes",
            "pinned",
            json!(true),
            &bob.all_history_block_manifests().unwrap(),
        )
        .unwrap();
    assert!(already_have.blocks.is_empty());
}

#[test]
fn contains_query_history_delta_syncs_matching_blocks() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("alpha v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for idx in 2..=5 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("alpha v{idx}")))]),
            )
            .unwrap();
    }
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("beta")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let delta = alice
        .export_query_where_contains_history_delta("notes", "body", "alpha", &[])
        .unwrap();
    assert_eq!(delta.bundle.history.len(), 1);
    assert_eq!(delta.blocks.len(), 1);

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();
    let rows = bob
        .read_rows_where_contains("notes", "body", "alpha")
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-1");
    assert_eq!(rows[0].values["body"], json!("alpha v5"));
}

#[test]
fn top_created_query_history_delta_syncs_matching_blocks() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    for idx in 2..=5 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }
    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("newer")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let delta = alice
        .export_query_where_eq_top_created_at_desc_history_delta(
            "notes",
            "pinned",
            json!(true),
            2,
            &[],
        )
        .unwrap();
    assert_eq!(delta.blocks.len(), 1);

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();
    let rows = bob
        .read_rows_where_eq_top_created_at_desc("notes", "pinned", json!(true), 2)
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id.as_str()).collect::<Vec<_>>(),
        vec!["note-2", "note-1"]
    );
    assert_eq!(rows[1].values["body"], json!("v5"));

    let refresh = alice
        .export_query_where_eq_top_created_at_desc_history_delta_with_previous_observed(
            "notes",
            "pinned",
            json!(true),
            1,
            vec!["note-1".to_owned()],
            &bob.all_history_block_manifests().unwrap(),
        )
        .unwrap();
    assert!(refresh.blocks.is_empty());
    assert!(refresh
        .bundle
        .history
        .iter()
        .any(|record| record.row_id == "note-1"));
}

#[test]
fn top_field_query_history_delta_repairs_previous_observed_rows() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.text("rank");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("old top")),
                ("rank".to_owned(), json!("z")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("new top")),
                ("rank".to_owned(), json!("a")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let initial = alice
        .export_query_where_eq_top_field_desc_history_delta(
            "notes",
            "pinned",
            json!(true),
            "rank",
            1,
            &[],
        )
        .unwrap();
    bob.apply_history_delta(&initial.bundle, &initial.blocks)
        .unwrap();
    assert_eq!(
        bob.read_rows_where_eq_top_field_desc("notes", "pinned", json!(true), "rank", 1)
            .unwrap()[0]
            .id,
        "note-1"
    );

    alice
        .update_row(
            "notes",
            "note-2",
            BTreeMap::from([("rank".to_owned(), json!("zz"))]),
        )
        .unwrap();
    for idx in 0..4 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("old-v{idx}")))]),
            )
            .unwrap();
    }
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let refresh = alice
        .export_query_where_eq_top_field_desc_history_delta_with_options(
            "notes",
            "pinned",
            json!(true),
            TopFieldHistoryDeltaOptions::new("rank", 1)
                .with_previous_observed_ids(vec!["note-1".to_owned()])
                .with_remote_block_manifests(bob.all_history_block_manifests().unwrap()),
        )
        .unwrap();
    assert_eq!(refresh.blocks.len(), 1);
    assert!(refresh
        .bundle
        .history
        .iter()
        .any(|record| record.row_id == "note-1"));
}

#[test]
fn observed_query_refresh_history_delta_includes_sealed_blocks() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("old")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));
    alice
        .insert_row(
            "notes",
            "note-2",
            BTreeMap::from([
                ("body".to_owned(), json!("new")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();

    let initial = alice
        .export_query_where_eq_top_created_at_desc_history_delta(
            "notes",
            "pinned",
            json!(true),
            1,
            &[],
        )
        .unwrap();
    bob.apply_history_delta(&initial.bundle, &initial.blocks)
        .unwrap();
    assert_eq!(bob.observed_query_reads().unwrap().len(), 1);

    for idx in 0..4 {
        alice
            .update_row(
                "notes",
                "note-2",
                BTreeMap::from([("body".to_owned(), json!(format!("new-v{idx}")))]),
            )
            .unwrap();
    }
    alice
        .compact_accepted_history("notes", "note-2", 1)
        .unwrap();

    let deltas = alice
        .export_query_read_refresh_deltas(
            &bob.observed_query_reads().unwrap(),
            &bob.all_history_block_manifests().unwrap(),
        )
        .unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].blocks.len(), 1);

    for delta in deltas {
        bob.apply_history_delta(&delta.bundle, &delta.blocks)
            .unwrap();
    }

    let rows = bob
        .read_rows_where_eq_top_created_at_desc("notes", "pinned", json!(true), 1)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].id, "note-2");
    assert_eq!(rows[0].values["body"], json!("new-v3"));
    assert_eq!(bob.storage_stats().unwrap().history_blocks, 1);
}

#[test]
fn observed_query_refresh_history_deltas_dedupe_shared_blocks() {
    let schema = SchemaDef::new().table("notes", |table| {
        table.text("body");
        table.bool("pinned");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("shared block")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    bob.apply_bundle(
        &alice
            .export_query_where_eq("notes", "pinned", json!(true))
            .unwrap(),
    )
    .unwrap();
    bob.apply_bundle(
        &alice
            .export_query_where_contains("notes", "body", "shared")
            .unwrap(),
    )
    .unwrap();
    assert_eq!(bob.observed_query_reads().unwrap().len(), 2);

    for idx in 0..4 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("shared block {idx}")))]),
            )
            .unwrap();
    }
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();

    let deltas = alice
        .export_query_read_refresh_deltas(
            &bob.observed_query_reads().unwrap(),
            &bob.all_history_block_manifests().unwrap(),
        )
        .unwrap();
    assert_eq!(deltas.len(), 2);
    assert_eq!(
        deltas.iter().map(|delta| delta.blocks.len()).sum::<usize>(),
        1
    );
}

#[test]
fn recursive_observed_query_refresh_history_delta_includes_sealed_blocks() {
    let schema = support::folders_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "folders",
            "root",
            BTreeMap::from([
                ("name".to_owned(), json!("Root")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "folders",
            "child",
            BTreeMap::from([
                ("name".to_owned(), json!("Child")),
                ("parent".to_owned(), json!("root")),
            ]),
        )
        .unwrap();

    let initial = alice
        .export_recursive_refs_history_delta("folders", "root", "parent", &[])
        .unwrap();
    bob.apply_history_delta(&initial.bundle, &initial.blocks)
        .unwrap();
    assert_eq!(bob.observed_query_reads().unwrap()[0].op, "recursive_refs");

    for idx in 0..4 {
        alice
            .update_row(
                "folders",
                "child",
                BTreeMap::from([("name".to_owned(), json!(format!("Child {idx}")))]),
            )
            .unwrap();
    }
    alice
        .compact_accepted_history("folders", "child", 1)
        .unwrap();

    let deltas = alice
        .export_query_read_refresh_deltas(
            &bob.observed_query_reads().unwrap(),
            &bob.all_history_block_manifests().unwrap(),
        )
        .unwrap();
    assert_eq!(deltas.len(), 1);
    assert_eq!(deltas[0].blocks.len(), 1);
    bob.apply_history_delta(&deltas[0].bundle, &deltas[0].blocks)
        .unwrap();

    let rows = bob
        .read_recursive_refs("folders", "root", "parent")
        .unwrap();
    let child = rows.iter().find(|row| row.id == "child").unwrap();
    assert_eq!(child.values["name"], json!("Child 3"));
    assert_eq!(bob.storage_stats().unwrap().history_blocks, 1);
}

#[test]
fn all_history_delta_syncs_open_rows_and_missing_blocks_across_tables() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.bool("pinned");
        })
        .table("comments", |table| {
            table.text("body");
            table.bool("resolved");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([
                ("title".to_owned(), json!("doc v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice
        .insert_row(
            "comments",
            "comment-1",
            BTreeMap::from([
                ("body".to_owned(), json!("comment v1")),
                ("resolved".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for idx in 2..=4 {
        alice
            .update_row(
                "docs",
                "doc-1",
                BTreeMap::from([("title".to_owned(), json!(format!("doc v{idx}")))]),
            )
            .unwrap();
        alice
            .update_row(
                "comments",
                "comment-1",
                BTreeMap::from([("body".to_owned(), json!(format!("comment v{idx}")))]),
            )
            .unwrap();
    }
    alice.compact_all_history(1, 1).unwrap();

    let delta = alice.export_all_history_delta(&[]).unwrap();
    assert_eq!(delta.bundle.history.len(), 2);
    assert_eq!(delta.blocks.len(), 2);

    bob.apply_history_delta(&delta.bundle, &delta.blocks)
        .unwrap();

    let docs = bob.read_rows("docs").unwrap();
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0].values["title"], json!("doc v4"));
    assert_eq!(docs[0].values["pinned"], json!(false));
    let comments = bob.read_rows("comments").unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].values["body"], json!("comment v4"));
    assert_eq!(comments[0].values["resolved"], json!(false));

    let already_have = alice
        .export_all_history_delta(&bob.all_history_block_manifests().unwrap())
        .unwrap();
    assert!(already_have.blocks.is_empty());
}

#[test]
fn point_read_at_global_epoch_can_decode_sealed_history_block() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let tx1 = alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.accept_transaction_at_global(&tx1, 1).unwrap();
    for epoch in 2..=6 {
        let tx = alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{epoch}")))]),
            )
            .unwrap();
        alice.accept_transaction_at_global(&tx, epoch).unwrap();
    }

    assert_eq!(
        alice
            .read_row_at_global_epoch("notes", "note-1", 3)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v3")
    );

    alice
        .compact_accepted_history("notes", "note-1", 2)
        .unwrap();

    assert_eq!(
        alice
            .read_row_at_global_epoch("notes", "note-1", 3)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v3")
    );
    assert_eq!(
        alice
            .read_row_at_global_epoch("notes", "note-1", 6)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v6")
    );

    alice
        .compact_accepted_history("notes", "note-1", 0)
        .unwrap();
    assert_eq!(
        alice
            .read_row_at_global_epoch("notes", "note-1", 99)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v6")
    );
}

#[test]
fn point_read_at_node_epoch_can_decode_locally_sealed_history_block() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for epoch in 2..=6 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{epoch}")))]),
            )
            .unwrap();
    }

    assert_eq!(
        alice
            .read_row_at_node_epoch("notes", "note-1", "alice-node", 3)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v3")
    );

    alice
        .compact_accepted_history("notes", "note-1", 2)
        .unwrap();

    assert_eq!(
        alice
            .read_row_at_node_epoch("notes", "note-1", "alice-node", 3)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v3")
    );
    assert_eq!(
        alice
            .read_row_at_node_epoch("notes", "note-1", "alice-node", 6)
            .unwrap()
            .unwrap()
            .values["body"],
        json!("v6")
    );
}

#[test]
fn query_scope_export_includes_sealed_history_for_matching_rows() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(true)),
            ]),
        )
        .unwrap();
    for idx in 2..=5 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }

    let before = alice
        .export_query_where_eq("notes", "id", json!("note-1"))
        .unwrap();
    alice
        .compact_accepted_history("notes", "note-1", 1)
        .unwrap();
    let after = alice
        .export_query_where_eq("notes", "id", json!("note-1"))
        .unwrap();

    assert_eq!(after.history, before.history);
    assert_eq!(after.txs, before.txs);
    assert_eq!(after.reads, before.reads);
    bob.apply_bundle(&after).unwrap();
    assert_eq!(
        bob.read_rows("notes").unwrap()[0].values["body"],
        json!("v5")
    );
}

#[test]
fn compaction_keeps_visible_head_rebuildable_with_zero_hot_tail() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for epoch in 2..=4 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{epoch}")))]),
            )
            .unwrap();
    }

    alice
        .compact_accepted_history("notes", "note-1", 0)
        .unwrap();
    assert_eq!(alice.storage_stats().unwrap().history_rows, 0);

    alice.clear_current_projection_for_test().unwrap();
    assert!(alice.read_rows("notes").unwrap().is_empty());

    alice.rebuild_current_projection().unwrap();
    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("v4"));
}

#[test]
fn reclaim_storage_returns_compacted_pages_to_sqlite_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reclaim.sqlite");
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::File(path), "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    for idx in 2..=300 {
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }
    alice
        .compact_accepted_history("notes", "note-1", 0)
        .unwrap();
    let before = alice.storage_stats().unwrap();
    assert!(before.freelist_bytes > 0);

    alice.reclaim_storage().unwrap();
    let after = alice.storage_stats().unwrap();
    assert_eq!(after.history_rows, 0);
    assert_eq!(after.freelist_bytes, 0);
    assert!(after.total_file_bytes < before.total_file_bytes);
    assert_eq!(
        alice.read_rows("notes").unwrap()[0].values["body"],
        json!("v300")
    );
}

#[test]
fn table_compaction_seals_each_deep_row_independently() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    for note in ["note-1", "note-2"] {
        alice
            .insert_row(
                "notes",
                note,
                BTreeMap::from([
                    ("body".to_owned(), json!("v1")),
                    ("pinned".to_owned(), json!(false)),
                ]),
            )
            .unwrap();
        for version in 2..=4 {
            alice
                .update_row(
                    "notes",
                    note,
                    BTreeMap::from([("body".to_owned(), json!(format!("{note}-v{version}")))]),
                )
                .unwrap();
        }
    }

    let stats = alice.compact_table_accepted_history("notes", 1, 2).unwrap();

    assert_eq!(stats.history_blocks, 2);
    assert_eq!(stats.sealed_history_rows, 6);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 2);
    assert_eq!(alice.read_rows("notes").unwrap().len(), 2);
}

#[test]
fn rejected_history_compaction_keeps_diagnostics_but_not_accepted_exports() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Private doc"))]),
        )
        .unwrap();
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();
    let rejected_tx = bob
        .insert_row(
            "comments",
            "comment-denied",
            BTreeMap::from([
                ("body".to_owned(), json!("not allowed")),
                ("doc".to_owned(), json!("doc-1")),
            ]),
        )
        .unwrap();

    let stats = bob
        .compact_rejected_history("comments", "comment-denied", 0)
        .unwrap();

    assert_eq!(stats.history_blocks, 1);
    assert_eq!(stats.sealed_history_rows, 1);
    assert_eq!(bob.storage_stats().unwrap().history_rows, 1);
    assert_eq!(bob.storage_stats().unwrap().rejected_transactions, 1);
    assert_eq!(
        bob.transaction_info(&rejected_tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
    assert_eq!(
        bob.rejected_transactions().unwrap(),
        vec![RejectionInfo {
            tx_id: rejected_tx.clone(),
            code: "policy_denied".to_owned(),
            detail: None,
        }]
    );
    assert!(!bob
        .export_table_history("comments")
        .unwrap()
        .history
        .iter()
        .any(|record| record.tx_id == rejected_tx));
}

#[test]
fn table_rejected_history_compaction_seals_each_rejected_row() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Private doc"))]),
        )
        .unwrap();
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();
    for idx in 1..=2 {
        bob.insert_row(
            "comments",
            &format!("comment-denied-{idx}"),
            BTreeMap::from([
                ("body".to_owned(), json!(format!("not allowed {idx}"))),
                ("doc".to_owned(), json!("doc-1")),
            ]),
        )
        .unwrap();
    }

    let stats = bob
        .compact_table_rejected_history("comments", 0, 0)
        .unwrap();

    assert_eq!(stats.history_blocks, 2);
    assert_eq!(stats.sealed_history_rows, 2);
    assert_eq!(bob.storage_stats().unwrap().rejected_transactions, 2);
    assert_eq!(bob.rejected_transactions().unwrap().len(), 2);
    assert!(bob.read_rows("comments").unwrap().is_empty());
}

#[test]
fn compaction_policy_can_split_rejected_history_into_smaller_blocks() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("Private doc"))]),
        )
        .unwrap();
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();
    for idx in 1..=5 {
        bob.upsert_row(
            "comments",
            "comment-denied",
            BTreeMap::from([
                ("body".to_owned(), json!(format!("not allowed {idx}"))),
                ("doc".to_owned(), json!("doc-1")),
            ]),
        )
        .unwrap();
    }

    let mut policy = HistoryCompactionPolicy::all(0, 0).with_max_rows_per_block(2);
    policy.accepted = false;
    let stats = bob.compact_history_with_policy(policy).unwrap();
    assert_eq!(stats.sealed_history_rows, 5);
    assert_eq!(stats.history_blocks, 3);
    assert_eq!(bob.all_history_block_manifests().unwrap().len(), 3);
    assert_eq!(bob.rejected_transactions().unwrap().len(), 5);
}

#[test]
fn compact_all_history_runs_accepted_and_rejected_passes() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("v1"))]),
        )
        .unwrap();
    for idx in 2..=4 {
        alice
            .update_row(
                "docs",
                "doc-1",
                BTreeMap::from([("title".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();
    bob.insert_row(
        "comments",
        "comment-denied",
        BTreeMap::from([
            ("body".to_owned(), json!("not allowed")),
            ("doc".to_owned(), json!("doc-1")),
        ]),
    )
    .unwrap();

    let accepted = alice.compact_all_history(1, 1).unwrap();
    let rejected = bob.compact_all_history(0, 0).unwrap();

    assert_eq!(accepted.history_blocks, 1);
    assert_eq!(accepted.sealed_history_rows, 3);
    assert_eq!(rejected.history_blocks, 2);
    assert_eq!(rejected.sealed_history_rows, 5);
    assert_eq!(bob.all_history_block_manifests().unwrap().len(), 2);
    assert_eq!(bob.export_all_history_blocks().unwrap().len(), 2);
}

#[test]
fn compaction_policy_can_budget_maintenance_blocks() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
        })
        .table("notes", |table| {
            table.text("body");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("v1"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("v1"))]),
        )
        .unwrap();
    for idx in 2..=4 {
        alice
            .update_row(
                "docs",
                "doc-1",
                BTreeMap::from([("title".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }

    let first = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1).with_max_blocks(1),
        )
        .unwrap();
    assert_eq!(first.history_blocks, 1);
    assert_eq!(first.sealed_history_rows, 3);

    let second = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1).with_max_blocks(1),
        )
        .unwrap();
    assert_eq!(second.history_blocks, 1);
    assert_eq!(second.sealed_history_rows, 3);

    let empty = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1).with_max_blocks(1),
        )
        .unwrap();
    assert_eq!(empty.history_blocks, 0);
    assert_eq!(alice.all_history_block_manifests().unwrap().len(), 2);
}

#[test]
fn compaction_policy_can_skip_when_wall_clock_budget_is_spent() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("v1"))]),
        )
        .unwrap();
    for idx in 2..=4 {
        alice
            .update_row(
                "docs",
                "doc-1",
                BTreeMap::from([("title".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }

    let skipped = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1)
                .with_max_duration(std::time::Duration::from_millis(0)),
        )
        .unwrap();
    assert_eq!(skipped.history_blocks, 0);
    assert_eq!(alice.all_history_block_manifests().unwrap().len(), 0);
}

#[test]
fn compaction_policy_can_budget_compressed_block_bytes() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
        })
        .table("notes", |table| {
            table.text("body");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("v1"))]),
        )
        .unwrap();
    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("v1"))]),
        )
        .unwrap();
    for idx in 2..=4 {
        alice
            .update_row(
                "docs",
                "doc-1",
                BTreeMap::from([("title".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
        alice
            .update_row(
                "notes",
                "note-1",
                BTreeMap::from([("body".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }

    let first = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1).with_max_compressed_bytes(1),
        )
        .unwrap();
    assert_eq!(first.history_blocks, 1);
    assert!(first.compressed_bytes > 1);

    let second = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1).with_max_compressed_bytes(1),
        )
        .unwrap();
    assert_eq!(second.history_blocks, 1);
    assert_eq!(alice.all_history_block_manifests().unwrap().len(), 2);
}

#[test]
fn compaction_policy_can_split_deep_rows_into_smaller_blocks() {
    let schema = SchemaDef::new().table("docs", |table| {
        table.text("title");
    });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-1",
            BTreeMap::from([("title".to_owned(), json!("v1"))]),
        )
        .unwrap();
    for idx in 2..=8 {
        alice
            .update_row(
                "docs",
                "doc-1",
                BTreeMap::from([("title".to_owned(), json!(format!("v{idx}")))]),
            )
            .unwrap();
    }

    let stats = alice
        .compact_history_with_policy(
            HistoryCompactionPolicy::accepted_only(1, 1).with_max_rows_per_block(2),
        )
        .unwrap();
    assert_eq!(stats.sealed_history_rows, 7);
    assert_eq!(stats.history_blocks, 4);
    assert_eq!(alice.storage_stats().unwrap().history_rows, 1);
    assert_eq!(alice.all_history_block_manifests().unwrap().len(), 4);
    assert_eq!(
        alice
            .read_row_at_node_epoch("docs", "doc-1", "alice-node", 3)
            .unwrap()
            .unwrap()
            .values["title"],
        json!("v3")
    );
}

#[test]
fn rejected_multi_row_tx_metadata_stays_open_until_all_rows_are_compacted() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice doc"))]),
        )
        .unwrap();
    bob.insert_row(
        "docs",
        "doc-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob doc"))]),
    )
    .unwrap();
    alice
        .apply_bundle(&bob.export_table_history("docs").unwrap())
        .unwrap();

    let tx = alice
        .transaction()
        .insert_row(
            "comments",
            "comment-allowed",
            BTreeMap::from([
                ("body".to_owned(), json!("Would be allowed alone")),
                ("doc".to_owned(), json!("doc-alice")),
            ]),
        )
        .insert_row(
            "comments",
            "comment-denied",
            BTreeMap::from([
                ("body".to_owned(), json!("Rejects whole transaction")),
                ("doc".to_owned(), json!("doc-bob")),
            ]),
        )
        .commit()
        .unwrap();

    let first = alice
        .compact_rejected_history("comments", "comment-allowed", 0)
        .unwrap();
    assert_eq!(first.sealed_transactions, 0);
    assert!(alice.transaction_physical_num_for(&tx).is_ok());
    assert_eq!(alice.transaction_write_rows(&tx).unwrap().len(), 2);

    let second = alice
        .compact_rejected_history("comments", "comment-denied", 0)
        .unwrap();
    assert_eq!(second.sealed_transactions, 1);
    assert!(alice.transaction_physical_num_for(&tx).is_err());
    assert_eq!(alice.transaction_write_rows(&tx).unwrap().len(), 2);
    assert_eq!(
        alice.transaction_info(&tx).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn batched_updates_keep_distinct_jazz_transactions_but_commit_together() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx_ids = alice
        .update_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("v2"))]),
                ),
                (
                    "note-1".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("v3"))]),
                ),
            ],
        )
        .unwrap();

    assert_eq!(tx_ids, vec!["tx-alice-node-2", "tx-alice-node-3"]);
    assert_eq!(
        alice.read_rows("notes").unwrap()[0].values["body"],
        json!("v3")
    );
    assert_eq!(
        alice.export_table_history("notes").unwrap().history.len(),
        3
    );
    assert_eq!(
        alice.transaction_previous_read_rows(&tx_ids[1]).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(
        bob.read_rows("notes").unwrap()[0].values["body"],
        json!("v3")
    );
}

#[test]
fn batched_inserts_keep_distinct_jazz_transactions_but_commit_together() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    let tx_ids = alice
        .insert_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("first")),
                        ("pinned".to_owned(), json!(false)),
                    ]),
                ),
                (
                    "note-2".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("second")),
                        ("pinned".to_owned(), json!(true)),
                    ]),
                ),
            ],
        )
        .unwrap();

    assert_eq!(tx_ids, vec!["tx-alice-node-1", "tx-alice-node-2"]);
    assert_eq!(alice.read_rows("notes").unwrap().len(), 2);
    assert_eq!(
        alice.export_table_history("notes").unwrap().history.len(),
        2
    );

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(bob.read_rows("notes").unwrap().len(), 2);
}

#[test]
fn batched_writes_roll_back_atomically_on_validation_error() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    let err = alice
        .insert_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("first")),
                        ("pinned".to_owned(), json!(false)),
                    ]),
                ),
                (
                    "note-2".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("second")),
                        ("unknown".to_owned(), json!(true)),
                    ]),
                ),
            ],
        )
        .unwrap_err();

    assert!(err.to_string().contains("unknown field unknown"));
    assert!(alice.read_rows("notes").unwrap().is_empty());
    assert_eq!(alice.storage_stats().unwrap().history_rows, 0);
}

#[test]
fn batched_writes_keep_policy_denials_per_logical_transaction() {
    let schema = SchemaDef::new()
        .table("docs", |table| {
            table.text("title");
            table.read_if_created_by_user();
        })
        .table("comments", |table| {
            table.text("body");
            table.ref_("doc", "docs");
            table.write_if_ref_readable("doc");
        });
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "bob", schema).unwrap();

    alice
        .insert_row(
            "docs",
            "doc-alice",
            BTreeMap::from([("title".to_owned(), json!("Alice doc"))]),
        )
        .unwrap();
    bob.insert_row(
        "docs",
        "doc-bob",
        BTreeMap::from([("title".to_owned(), json!("Bob doc"))]),
    )
    .unwrap();
    bob.apply_bundle(&alice.export_table_history("docs").unwrap())
        .unwrap();

    let tx_ids = bob
        .insert_rows_batched(
            "comments",
            vec![
                (
                    "comment-ok".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("allowed")),
                        ("doc".to_owned(), json!("doc-bob")),
                    ]),
                ),
                (
                    "comment-denied".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("denied")),
                        ("doc".to_owned(), json!("doc-alice")),
                    ]),
                ),
            ],
        )
        .unwrap();

    let comments = bob.read_rows("comments").unwrap();
    assert_eq!(comments.len(), 1);
    assert_eq!(comments[0].id, "comment-ok");
    assert_eq!(
        bob.transaction_info(&tx_ids[0]).unwrap().rejection_code,
        None
    );
    assert_eq!(
        bob.transaction_info(&tx_ids[1]).unwrap().rejection_code,
        Some("policy_denied".to_owned())
    );
}

#[test]
fn batched_upserts_can_mix_creates_and_updates() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut bob = Runtime::open_with_schema(Storage::Memory, "bob-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx_ids = alice
        .upsert_rows_batched(
            "notes",
            vec![
                (
                    "note-1".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("v2"))]),
                ),
                (
                    "note-2".to_owned(),
                    BTreeMap::from([
                        ("body".to_owned(), json!("created in batch")),
                        ("pinned".to_owned(), json!(true)),
                    ]),
                ),
                (
                    "note-2".to_owned(),
                    BTreeMap::from([("body".to_owned(), json!("updated in same batch"))]),
                ),
            ],
        )
        .unwrap();

    assert_eq!(
        tx_ids,
        vec!["tx-alice-node-2", "tx-alice-node-3", "tx-alice-node-4"]
    );
    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows.iter().find(|row| row.id == "note-1").unwrap().values["body"],
        json!("v2")
    );
    assert_eq!(
        rows.iter().find(|row| row.id == "note-2").unwrap().values["body"],
        json!("updated in same batch")
    );
    assert_eq!(
        alice.export_table_history("notes").unwrap().history.len(),
        4
    );

    bob.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    assert_eq!(bob.read_rows("notes").unwrap().len(), 2);
}

#[test]
fn sealed_transaction_lookup_handles_hyphenated_node_ids() {
    let schema = support::notes_schema();
    let mut alice = Runtime::open_with_schema(
        Storage::Memory,
        "550e8400-e29b-41d4-a716-446655440000",
        "alice",
        schema,
    )
    .unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("v1")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    let tx = alice
        .update_row(
            "notes",
            "note-1",
            BTreeMap::from([("body".to_owned(), json!("v2"))]),
        )
        .unwrap();
    alice
        .compact_accepted_history("notes", "note-1", 0)
        .unwrap();

    assert_eq!(tx, "tx-550e8400-e29b-41d4-a716-446655440000-2".to_owned());
    assert_eq!(alice.transaction_info(&tx).unwrap().tx_id, tx);
    assert_eq!(
        alice.transaction_write_rows(&tx).unwrap(),
        vec![("notes".to_owned(), "note-1".to_owned())]
    );
}

#[test]
fn delete_is_history_not_removal() {
    let mut alice = Runtime::open(Storage::Memory, "alice-node", "alice").unwrap();

    alice.create_project("project-1", "Spec work").unwrap();
    alice
        .create_todo("todo-1", "Delete as history", false, "project-1")
        .unwrap();
    alice.delete_todo("todo-1").unwrap();

    assert!(alice.open_todos().unwrap().is_empty());
    let stats = alice.storage_stats().unwrap();
    assert_eq!(stats.history_rows, 3);
    assert_eq!(stats.current_rows, 1);
}

#[test]
fn deleted_generic_row_can_be_restored_as_new_history_version() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Before delete")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("notes", "note-1").unwrap();
    assert!(alice.read_rows("notes").unwrap().is_empty());

    alice.restore_deleted_row("notes", "note-1").unwrap();

    let rows = alice.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Before delete"));
    alice.clear_current_projection_for_test().unwrap();
    alice.rebuild_current_projection().unwrap();
    assert_eq!(alice.read_rows("notes").unwrap().len(), 1);
}

#[test]
fn restored_deleted_row_syncs_to_peer() {
    let schema = support::notes_schema();
    let mut alice =
        Runtime::open_with_schema(Storage::Memory, "alice-node", "alice", schema.clone()).unwrap();
    let mut peer =
        Runtime::open_with_schema(Storage::Memory, "peer-node", "alice", schema).unwrap();

    alice
        .insert_row(
            "notes",
            "note-1",
            BTreeMap::from([
                ("body".to_owned(), json!("Sync restore")),
                ("pinned".to_owned(), json!(false)),
            ]),
        )
        .unwrap();
    alice.delete_row("notes", "note-1").unwrap();
    alice.restore_deleted_row("notes", "note-1").unwrap();

    peer.apply_bundle(&alice.export_table_history("notes").unwrap())
        .unwrap();
    let rows = peer.read_rows("notes").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values["body"], json!("Sync restore"));
}
