use super::*;

#[test]
fn insert_json_preserves_original_text() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(None);
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let raw = "{\n  \"name\": \"Ada\",\n  \"active\": true\n}";
    qm.insert(&mut storage, "documents", &[Value::Text(raw.to_string())])
        .expect("insert valid json");

    let query = qm.query("documents").build();
    let rows = execute_query(&mut qm, &mut storage, query).expect("query inserted row");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1, vec![Value::Text(raw.to_string())]);
}

#[test]
fn insert_rejects_invalid_json_text() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(None);
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let err = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("{\"name\":true".to_string())],
        )
        .expect_err("invalid JSON must be rejected");

    assert!(
        matches!(&err, QueryError::EncodingError(msg) if msg.contains("invalid JSON for column `payload`")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn insert_rejects_json_schema_violation() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })));
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let err = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("{\"name\":123}".to_string())],
        )
        .expect_err("schema-invalid JSON must be rejected");

    assert!(
        matches!(&err, QueryError::EncodingError(msg) if msg.contains("JSON schema validation failed for column `payload`")),
        "unexpected error: {err:?}"
    );
}

#[test]
fn update_rejects_json_schema_violation() {
    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(Some(json!({
        "type": "object",
        "properties": {
            "name": { "type": "string" }
        },
        "required": ["name"],
        "additionalProperties": false
    })));
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let inserted = qm
        .insert(
            &mut storage,
            "documents",
            &[Value::Text("{\"name\":\"ok\"}".to_string())],
        )
        .expect("insert valid row first");

    let err = qm
        .update(
            &mut storage,
            inserted.row_id,
            &[Value::Text("{\"name\":42}".to_string())],
        )
        .expect_err("invalid update payload must be rejected");
    assert!(
        matches!(&err, QueryError::EncodingError(msg) if msg.contains("JSON schema validation failed for column `payload`")),
        "unexpected error: {err:?}"
    );

    let query = qm.query("documents").build();
    let rows = execute_query(&mut qm, &mut storage, query).expect("query existing row");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("{\"name\":\"ok\"}".to_string())]
    );
}

#[test]
fn synced_insert_log_includes_failing_index_column() {
    let collector = EventCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let sync_manager = SyncManager::new();
    let schema = json_documents_schema(None);
    let descriptor = schema[&TableName::new("documents")].columns.clone();

    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema.clone(), "dev", "main");

    let mut storage = FailOnIndexColumnStorage::new("payload");
    persist_test_schema(&mut storage, &schema);

    let branch = get_branch(&qm);
    let row_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "documents".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let raw_json = json!({
        "content": "x".repeat(4_096),
        "kind": "payload"
    })
    .to_string();
    let row_data = encode_row(&descriptor, &[Value::Text(raw_json)]).unwrap();

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: stored_row_commit(smallvec![], row_data, 1_000, row_id.to_string()).to_row(
                row_id,
                &branch,
                RowState::VisibleDirect,
            ),
        },
    });
    qm.process(&mut storage);

    let event = collector
        .snapshot()
        .into_iter()
        .find(|event| {
            event.level == tracing::Level::ERROR
                && event.message.as_deref() == Some("failed to update indices for synced insert")
        })
        .expect("synced insert failure should be logged");

    assert_eq!(
        event.fields.get("index_column").map(String::as_str),
        Some("payload")
    );
    assert_eq!(
        event.fields.get("table").map(String::as_str),
        Some("documents")
    );
}

#[test]
fn synced_insert_many_large_json_configs_survive_opfs_splits() {
    let collector = EventCollector::default();
    let subscriber = Registry::default().with(collector.clone());
    let _guard = tracing::subscriber::set_default(subscriber);

    let sync_manager = SyncManager::new();
    let schema = visual_description_schema();
    let descriptor = schema[&TableName::new("visual_description")]
        .columns
        .clone();

    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema.clone(), "dev", "main");

    let mut storage = OpfsBTreeStorage::memory(4 * 1024 * 1024).expect("open opfs storage");
    persist_test_schema(&mut storage, &schema);

    let branch = get_branch(&qm);
    let table = "visual_description";
    let max_index_value_segment_len =
        5 * 1024 - (4 + table.len() + 1 + "config".len() + 1 + branch.len() + 1 + 32);
    let max_inline_text_bytes = (max_index_value_segment_len / 2).saturating_sub(1);
    let json_overhead = "{\"config\":\"\"}".len();
    let shared_prefix = "a".repeat(max_inline_text_bytes - json_overhead - 8);

    for i in 0..128 {
        let row_id = ObjectId::new();
        let mut metadata = HashMap::new();
        metadata.insert(MetadataKey::Table.to_string(), table.to_string());
        put_test_row_metadata(&mut storage, row_id, metadata);

        let payload = if i % 8 == 7 {
            format!("{{\"config\":\"b{i:08x}\"}}")
        } else {
            format!("{{\"config\":\"{shared_prefix}{i:08x}\"}}")
        };
        let row_data = encode_row(&descriptor, &[Value::Text(payload)]).unwrap();

        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(ServerId::new()),
            payload: SyncPayload::RowBatchCreated {
                metadata: None,
                row: stored_row_commit(smallvec![], row_data, 1_000 + i as u64, row_id.to_string())
                    .to_row(row_id, &branch, RowState::VisibleDirect),
            },
        });
    }

    qm.process(&mut storage);

    let failures = collector
        .snapshot()
        .into_iter()
        .filter(|event| {
            event.level == tracing::Level::ERROR
                && event.message.as_deref() == Some("failed to update indices for synced insert")
        })
        .collect::<Vec<_>>();
    assert!(
        failures.is_empty(),
        "synced inserts should not hit opfs split failures: {failures:?}"
    );

    let query = qm.query(table).build();
    let rows = execute_query(&mut qm, &mut storage, query).expect("query synced rows");
    assert_eq!(rows.len(), 128, "all synced rows should remain queryable");
}
