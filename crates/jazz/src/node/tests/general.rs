#[test]
fn subscription_equivalence_preserves_physical_to_public_provenance_changes() {
    fn current_row(
        physical: bool,
        created_by: AuthorSubject,
        created_at: u64,
        updated_by: AuthorSubject,
        updated_at: u64,
        title: &str,
    ) -> CurrentRow {
        let row_uuid = row(0x68);
        let (descriptor, values) = if physical {
            (
                records::RecordDescriptor::new([
                    ("branch_key".to_owned(), records::ValueType::Bytes),
                    ("row_uuid".to_owned(), records::ValueType::Uuid),
                    ("schema_version".to_owned(), records::ValueType::U64),
                    ("created_by".to_owned(), records::ValueType::String),
                    ("created_at".to_owned(), records::ValueType::U64),
                    ("updated_by".to_owned(), records::ValueType::String),
                    ("updated_at".to_owned(), records::ValueType::U64),
                    ("user_title".to_owned(), records::ValueType::String),
                ]),
                vec![
                    Value::Bytes(Vec::new()),
                    Value::Uuid(row_uuid.0),
                    Value::U64(1),
                    Value::String(created_by.canonical().to_owned()),
                    Value::U64(created_at),
                    Value::String(updated_by.canonical().to_owned()),
                    Value::U64(updated_at),
                    Value::String(title.to_owned()),
                ],
            )
        } else {
            (
                records::RecordDescriptor::new([
                    ("row_uuid".to_owned(), records::ValueType::Uuid),
                    ("title".to_owned(), records::ValueType::String),
                    ("$createdBy".to_owned(), records::ValueType::String),
                    ("$createdAt".to_owned(), records::ValueType::U64),
                    ("$updatedBy".to_owned(), records::ValueType::String),
                    ("$updatedAt".to_owned(), records::ValueType::U64),
                ]),
                vec![
                    Value::Uuid(row_uuid.0),
                    Value::String(title.to_owned()),
                    Value::String(created_by.canonical().to_owned()),
                    Value::U64(created_at),
                    Value::String(updated_by.canonical().to_owned()),
                    Value::U64(updated_at),
                ],
            )
        };
        let raw = descriptor.create(&values).unwrap();
        CurrentRow::new("todos", OwnedRecord::new(raw, descriptor))
    }

    let created_by = AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([0x68; 16]));
    let original_updated_by = AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([0x69; 16]));
    let changed_updated_by = AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([0x6a; 16]));
    let physical = current_row(true, created_by, 10, original_updated_by, 20, "same title");
    let same_public = current_row(false, created_by, 10, original_updated_by, 20, "same title");
    let changed_provenance = current_row(false, created_by, 10, changed_updated_by, 21, "same title");
    let changed_content = current_row(false, created_by, 10, original_updated_by, 20, "new title");

    assert!(physical.subscription_equivalent(&same_public));
    assert!(!physical.subscription_equivalent(&changed_provenance));
    assert!(!physical.subscription_equivalent(&changed_content));
}

#[test]
fn subscription_equivalence_canonicalizes_wide_rows_without_repeated_decoding() {
    const CELL_COUNT: usize = 512;
    let row_uuid = row(0x6b);
    let mut physical_fields = vec![
        ("branch_key".to_owned(), records::ValueType::Bytes),
        ("row_uuid".to_owned(), records::ValueType::Uuid),
        ("schema_version".to_owned(), records::ValueType::U64),
        ("created_by".to_owned(), records::ValueType::String),
        ("created_at".to_owned(), records::ValueType::U64),
        ("updated_by".to_owned(), records::ValueType::String),
        ("updated_at".to_owned(), records::ValueType::U64),
    ];
    let mut physical_values = vec![
        Value::Bytes(Vec::new()),
        Value::Uuid(row_uuid.0),
        Value::U64(1),
        Value::String(AuthorSubject::SYSTEM.canonical().to_owned()),
        Value::U64(10),
        Value::String(AuthorSubject::SYSTEM.canonical().to_owned()),
        Value::U64(20),
    ];
    let mut public_fields = vec![
        ("row_uuid".to_owned(), records::ValueType::Uuid),
        ("$createdBy".to_owned(), records::ValueType::String),
        ("$createdAt".to_owned(), records::ValueType::U64),
        ("$updatedBy".to_owned(), records::ValueType::String),
        ("$updatedAt".to_owned(), records::ValueType::U64),
    ];
    let mut public_values = vec![
        Value::Uuid(row_uuid.0),
        Value::String(AuthorSubject::SYSTEM.canonical().to_owned()),
        Value::U64(10),
        Value::String(AuthorSubject::SYSTEM.canonical().to_owned()),
        Value::U64(20),
    ];
    for idx in 0..CELL_COUNT {
        physical_fields.push((format!("user_column_{idx}"), records::ValueType::U64));
        physical_values.push(Value::U64(idx as u64));
    }
    // Reverse public descriptor order to ensure equality is independent of
    // layout while still decoding only the two linear cell iterators once.
    for idx in (0..CELL_COUNT).rev() {
        public_fields.push((format!("column_{idx}"), records::ValueType::U64));
        public_values.push(Value::U64(idx as u64));
    }
    let physical_descriptor = records::RecordDescriptor::new(physical_fields);
    let public_descriptor = records::RecordDescriptor::new(public_fields);
    let physical_raw = physical_descriptor.create(&physical_values).unwrap();
    let public_raw = public_descriptor.create(&public_values).unwrap();
    let physical = CurrentRow::new(
        "todos",
        OwnedRecord::new(physical_raw, physical_descriptor),
    );
    let public = CurrentRow::new("todos", OwnedRecord::new(public_raw, public_descriptor));

    assert!(physical.subscription_equivalent(&public));
}

#[test]
fn ordinary_oversized_scalar_write_is_staged_indirect_and_reads_logically_inline() {
    let schema = two_column_schema();
    let node_uuid = node(0x71);
    let (temp_dir, mut node) = open_node_with_schema(node_uuid, schema.clone());
    let body = "large logical body/".repeat(20_000);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x71), 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("title".to_owned())),
            ("body".to_owned(), Value::String(body.clone())),
        ])),
    )
    .unwrap();

    let stored = node.query_table_versions("todos").unwrap();
    assert!(matches!(
        stored[0].cell(node.table("todos").unwrap(), "body"),
        Ok(Some(Value::Large(_)))
    ));
    let current = node.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(
        current[0].cell(node.table("todos").unwrap(), "body"),
        Some(Value::String(body.clone()))
    );
    node.database.close().unwrap();
    drop(node);
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let mut reopened = NodeState::new(node_uuid, schema, storage).unwrap();
    assert_eq!(
        reopened.current_rows("todos", DurabilityTier::Local).unwrap()[0]
            .cell(reopened.table("todos").unwrap(), "body"),
        Some(Value::String(body))
    );
}

#[test]
fn failed_large_scalar_staging_publishes_no_row() {
    #[derive(Clone)]
    struct FailingStage;
    impl groove::chunks::ChunkStorage for FailingStage {
        fn get(
            &self,
            _locator: groove::large_values::Locator,
            _expected_hash: groove::large_values::ContentHash,
        ) -> groove::chunks::ChunkFuture<'_, Result<bytes::Bytes, groove::chunks::ChunkStorageError>> {
            Box::pin(async { Err(groove::chunks::ChunkStorageError::Unavailable) })
        }

        fn stage(
            &self,
            _chunks: Vec<groove::large_values::StagedChunk>,
        ) -> groove::chunks::ChunkFuture<
            '_,
            Result<
                groove::large_values::StagedLargeValueAccounting,
                groove::chunks::ChunkStorageError,
            >,
        > {
            Box::pin(async { Err(groove::chunks::ChunkStorageError::Backend("planted".to_owned())) })
        }
    }

    let schema = two_column_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x72), schema);
    node.set_chunk_storage(std::rc::Rc::new(FailingStage));
    let result = node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x72), 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("title".to_owned())),
            ("body".to_owned(), Value::String("x".repeat(70_000))),
        ])),
    );

    assert!(matches!(
        result,
        Err(Error::Groove(GrooveDbError::IvmRuntime(
            groove::ivm::runtime::IvmRuntimeError::Chunk(
                groove::chunks::ChunkError::Backend(message)
            )
        ))) if message.contains("planted")
    ));
    assert!(node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn jazz_incoming_data_rate_limit_evicts_the_rejected_root_and_publishes_no_row() {
    let schema = two_column_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x7a), schema);
    node.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: 1,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    let result = node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x7a), 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("title".to_owned())),
            ("body".to_owned(), Value::String("x".repeat(70_000))),
        ])),
    );

    assert!(matches!(result, Err(Error::LargeValueIngressRateLimited)));
    assert!(node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn default_large_value_staging_policy_is_finite() {
    let policy = LargeValueStagingPolicy::default();
    assert_eq!(policy.incoming_bytes_per_window, 256 * 1024 * 1024);
    assert_eq!(policy.window_ms, 1_000);
    assert_eq!(policy.max_age_ms, 10 * 60 * 1_000);
}

#[test]
fn upload_start_is_rate_admitted_before_pending_metadata_is_written() {
    let schema = two_column_schema();
    let (_temp_dir, mut receiver) = open_node_with_schema(node(0x82), schema);
    receiver.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: 0,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    let prepared = groove::large_values::prepare(
        groove::large_values::LargeValueKind::String,
        b"rate-admitted upload start",
    )
    .unwrap();
    let outcome = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadStart(crate::protocol::ChunkUploadStart {
                value_ref: prepared.value_ref,
            }),
            Some(CommitUnitIngestContext {
                identity: AuthorSubject::SYSTEM,
                trust: CommitUnitTrust::Session,
                edge_authority: false,
            }),
        )
        .resolve()
        .unwrap()
        .value;

    assert!(matches!(
        outcome.as_slice(),
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::RateLimited,
            ..
        })]
    ));
    assert!(
        crate::db::block_on(receiver.database.pending_large_value_uploads())
            .unwrap()
            .is_empty(),
        "rate-limited starts must not create durable pending metadata"
    );
}

#[test]
fn expired_staged_tree_requires_reupload_before_row_publication() {
    let schema = two_column_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x7b), schema);
    node.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 1_000,
        max_age_ms: 1,
    });
    let logical = "expired staged body/".repeat(8_000);
    let (commit, _) = crate::db::block_on(node.attach_large_cell_for_test(
        MergeableCommit::new("todos", row(0x7b), 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("title".to_owned()),
        )])),
        "body",
        groove::large_values::LargeValueKind::String,
        logical.as_bytes(),
    ))
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(3));
    assert_eq!(
        crate::db::block_on(node.evict_expired_staged_large_values()).unwrap(),
        1,
        "host maintenance evicts the abandoned staged root"
    );

    assert!(matches!(
        node.commit_mergeable_settled(commit),
        Err(Error::LargeValueStageExpired)
    ));
    assert!(node
        .current_rows("todos", DurabilityTier::Local)
        .unwrap()
        .is_empty());
}

#[test]
fn delayed_staged_tree_publishes_while_receipt_remains_present() {
    let schema = two_column_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x7c), schema);
    node.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 1_000,
        max_age_ms: 0,
    });
    let logical = "delayed staged body/".repeat(8_000);
    let (commit, _) = crate::db::block_on(node.attach_large_cell_for_test(
        MergeableCommit::new("todos", row(0x7c), 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("title".to_owned()),
        )])),
        "body",
        groove::large_values::LargeValueKind::String,
        logical.as_bytes(),
    ))
    .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(2));

    node.commit_mergeable_settled(commit)
        .expect("wall-clock age alone must not reject a present receipt");
    assert_eq!(node.current_rows("todos", DurabilityTier::Local).unwrap().len(), 1);
}

#[test]
fn pushed_chunks_must_be_staged_before_the_referencing_authority_commit() {
    let schema = two_column_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x7c), schema.clone());
    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0x7d), schema.clone());
    let (_missing_dir, mut missing) = open_node_with_schema(node(0x7e), schema);
    let logical = "pushed body/".repeat(8_000);
    let (commit, value_ref) = crate::db::block_on(writer.attach_large_cell_for_test(
        MergeableCommit::new("todos", row(0x7c), 10).cells(BTreeMap::from([(
            "title".to_owned(),
            Value::String("title".to_owned()),
        )])),
        "body",
        groove::large_values::LargeValueKind::String,
        logical.as_bytes(),
    ))
    .unwrap();
    let (_, unit) = writer.commit_mergeable_unit_settled(commit).unwrap();
    let context = Some(CommitUnitIngestContext {
        identity: AuthorSubject::SYSTEM,
        trust: CommitUnitTrust::Session,
        edge_authority: false,
    });
    assert!(matches!(
        missing
            .apply_sync_message_with_ingest_context(unit.clone(), context)
            .resolve(),
        Err(Error::LargeValueStageExpired)
    ));

    let mut upload_result = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadStart(crate::protocol::ChunkUploadStart {
                value_ref: value_ref.clone(),
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    loop {
        let status = match upload_result.as_slice() {
            [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                status,
                ..
            })] => status.clone(),
            other => panic!("unexpected upload result: {other:?}"),
        };
        match status {
            crate::protocol::ChunkUploadStatus::Need(nodes) => {
                let chunks = nodes
                    .into_iter()
                    .map(|node_ref| {
                        let encoded = crate::db::block_on(writer.local_chunk(
                            node_ref.locator,
                            node_ref.object_hash,
                        ))
                        .expect("writer retains each requested immutable node");
                        groove::large_values::StagedChunk {
                            node_ref,
                            encoded: encoded.to_vec(),
                        }
                    })
                    .collect();
                upload_result = receiver
                    .apply_sync_message_with_ingest_context(
                        SyncMessage::ChunkUploadNodes(crate::protocol::ChunkUploadNodes {
                            value_ref: value_ref.clone(),
                            chunks,
                        }),
                        context,
                    )
                    .resolve()
                    .unwrap()
                    .value;
            }
            crate::protocol::ChunkUploadStatus::Staged => break,
            other => panic!("upload failed: {other:?}"),
        }
    }
    let outcome = receiver
        .apply_sync_message_with_ingest_context(unit, context)
        .resolve()
        .unwrap();
    settle_outcome(&mut receiver, outcome).unwrap();
}

#[test]
fn corrupt_root_first_upload_is_rejected_without_poisoning_the_receiver() {
    let schema = two_column_schema();
    let (_receiver_dir, mut receiver) = open_node_with_schema(node(0x7f), schema);
    let prepared = groove::large_values::prepare(
        groove::large_values::LargeValueKind::String,
        "corrupt upload/".repeat(8_000).as_bytes(),
    )
    .unwrap();
    let context = Some(CommitUnitIngestContext {
        identity: AuthorSubject::SYSTEM,
        trust: CommitUnitTrust::Session,
        edge_authority: false,
    });
    let mut root = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == prepared.value_ref.root)
        .unwrap()
        .clone();
    root.encoded[0] ^= 0xff;
    let rejected = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadNodes(crate::protocol::ChunkUploadNodes {
                value_ref: prepared.value_ref.clone(),
                chunks: vec![root],
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    assert!(matches!(
        rejected.as_slice(),
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::Rejected,
            ..
        })]
    ));

    let retry = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadStart(crate::protocol::ChunkUploadStart {
                value_ref: prepared.value_ref.clone(),
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    assert!(matches!(
        retry.as_slice(),
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::Need(nodes),
            ..
        })] if nodes == &[prepared.value_ref.root]
    ));
}

#[test]
fn rate_limited_upload_preserves_pending_claim_for_retry() {
    let schema = two_column_schema();
    let node_uuid = node(0x80);
    let (_temp_dir, mut receiver) = open_node_with_schema(node_uuid, schema.clone());
    let prepared = groove::large_values::prepare(
        groove::large_values::LargeValueKind::String,
        "terminal cleanup/".repeat(20_000).as_bytes(),
    )
    .unwrap();
    let context = Some(CommitUnitIngestContext {
        identity: AuthorSubject::SYSTEM,
        trust: CommitUnitTrust::Session,
        edge_authority: false,
    });
    let start = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadStart(crate::protocol::ChunkUploadStart {
                value_ref: prepared.value_ref.clone(),
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    let root = match start.as_slice() {
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::Need(nodes),
            ..
        })] => nodes[0].clone(),
        other => panic!("unexpected upload start result: {other:?}"),
    };
    let root_chunk = prepared
        .staged_chunks
        .iter()
        .find(|chunk| chunk.node_ref == root)
        .unwrap()
        .clone();
    let accepted = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadNodes(crate::protocol::ChunkUploadNodes {
                value_ref: prepared.value_ref.clone(),
                chunks: vec![root_chunk.clone()],
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    let mut pending_nodes = match accepted.as_slice() {
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::Need(nodes),
            ..
        })] => nodes.clone(),
        other => panic!("expected a partial upload frontier: {other:?}"),
    };
    receiver.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: 1,
        window_ms: 60_000,
        max_age_ms: 10 * 60 * 1_000,
    });
    let rate_limited = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadNodes(crate::protocol::ChunkUploadNodes {
                value_ref: prepared.value_ref.clone(),
                chunks: vec![root_chunk.clone()],
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    assert!(matches!(
        rate_limited.as_slice(),
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::RateLimited,
            ..
        })]
    ));
    assert_eq!(
        crate::db::block_on(receiver.database.pending_large_value_uploads())
            .unwrap()
            .len(),
        1,
        "rate limiting is resumable and retains prior accepted nodes"
    );
    receiver.set_large_value_staging_policy(LargeValueStagingPolicy::default());
    loop {
        let chunks = pending_nodes
            .into_iter()
            .map(|node_ref| {
                prepared
                    .staged_chunks
                    .iter()
                    .find(|chunk| chunk.node_ref == node_ref)
                    .unwrap()
                    .clone()
            })
            .collect();
        let retried = receiver
            .apply_sync_message_with_ingest_context(
                SyncMessage::ChunkUploadNodes(crate::protocol::ChunkUploadNodes {
                    value_ref: prepared.value_ref.clone(),
                    chunks,
                }),
                context,
            )
            .resolve()
            .unwrap()
            .value;
        match retried.as_slice() {
            [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                status: crate::protocol::ChunkUploadStatus::Need(nodes),
                ..
            })] => pending_nodes = nodes.clone(),
            [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                status: crate::protocol::ChunkUploadStatus::Staged,
                ..
            })] => break,
            other => panic!("retry did not resume the upload: {other:?}"),
        }
    }
}

#[test]
fn maintenance_evicts_pending_upload_after_the_configured_age() {
    let schema = two_column_schema();
    let (_temp_dir, mut receiver) = open_node_with_schema(node(0x81), schema);
    let prepared = groove::large_values::prepare(
        groove::large_values::LargeValueKind::String,
        "pending expiry/".repeat(20_000).as_bytes(),
    )
    .unwrap();
    let context = Some(CommitUnitIngestContext {
        identity: AuthorSubject::SYSTEM,
        trust: CommitUnitTrust::Session,
        edge_authority: false,
    });
    let _ = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadStart(crate::protocol::ChunkUploadStart {
                value_ref: prepared.value_ref,
            }),
            context,
        )
        .resolve()
        .unwrap();
    receiver.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 1_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        crate::db::block_on(receiver.evict_expired_staged_large_values()).unwrap(),
        1
    );
    assert!(crate::db::block_on(receiver.database.pending_large_value_uploads())
        .unwrap()
        .is_empty());
}

/// An authenticated sender starts a large upload, then waits past the receiver
/// configured TTL without running maintenance. The still-present journal may
/// continue all the way through finalization: TTL is GC policy, not a
/// synchronous admission deadline.
///
/// ```text
/// alice ──start──► receiver ──Need(root)──► alice
/// alice ──delay──► receiver ──nodes──► Staged
/// ```
#[test]
fn delayed_chunk_upload_succeeds_while_pending_journal_remains_present() {
    let schema = two_column_schema();
    let (_temp_dir, mut receiver) = open_node_with_schema(node(0x82), schema);
    let prepared = groove::large_values::prepare(
        groove::large_values::LargeValueKind::String,
        "delayed finalization/".repeat(20_000).as_bytes(),
    )
    .unwrap();
    let context = Some(CommitUnitIngestContext {
        identity: AuthorSubject::SYSTEM,
        trust: CommitUnitTrust::Session,
        edge_authority: false,
    });
    let started = receiver
        .apply_sync_message_with_ingest_context(
            SyncMessage::ChunkUploadStart(crate::protocol::ChunkUploadStart {
                value_ref: prepared.value_ref.clone(),
            }),
            context,
        )
        .resolve()
        .unwrap()
        .value;
    let mut pending_nodes = match started.as_slice() {
        [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
            status: crate::protocol::ChunkUploadStatus::Need(nodes),
            ..
        })] => nodes.clone(),
        other => panic!("unexpected upload start: {other:?}"),
    };
    receiver.set_large_value_staging_policy(LargeValueStagingPolicy {
        incoming_bytes_per_window: u64::MAX,
        window_ms: 1_000,
        max_age_ms: 0,
    });
    std::thread::sleep(std::time::Duration::from_millis(2));
    loop {
        let chunks = pending_nodes
            .into_iter()
            .map(|node_ref| {
                prepared
                    .staged_chunks
                    .iter()
                    .find(|chunk| chunk.node_ref == node_ref)
                    .unwrap()
                    .clone()
            })
            .collect();
        let response = receiver
            .apply_sync_message_with_ingest_context(
                SyncMessage::ChunkUploadNodes(crate::protocol::ChunkUploadNodes {
                    value_ref: prepared.value_ref.clone(),
                    chunks,
                }),
                context,
            )
            .resolve()
            .unwrap()
            .value;
        match response.as_slice() {
            [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                status: crate::protocol::ChunkUploadStatus::Need(nodes),
                ..
            })] => pending_nodes = nodes.clone(),
            [SyncMessage::ChunkUploadResult(crate::protocol::ChunkUploadResult {
                status: crate::protocol::ChunkUploadStatus::Staged,
                ..
            })] => break,
            other => panic!("delayed upload did not resume: {other:?}"),
        }
    }
    assert!(crate::db::block_on(receiver.database.pending_large_value_uploads())
        .unwrap()
        .is_empty());
    assert_eq!(
        crate::db::block_on(receiver.database.staged_large_values())
            .unwrap()
            .len(),
        1
    );
}

#[test]
fn synced_descriptor_reads_through_shared_opaque_chunk_backend() {
    let schema = two_column_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(0x73), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(0x74), schema);
    let backend = std::rc::Rc::new(groove::chunks::MemoryChunkStorage::new());
    writer.set_chunk_storage(backend.clone());
    reader.set_chunk_storage(backend);
    let body = "shared backend value/".repeat(15_000);
    let (_, unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row(0x73), 10).cells(BTreeMap::from([
                ("title".to_owned(), Value::String("title".to_owned())),
                ("body".to_owned(), Value::String(body.clone())),
            ])),
        )
        .unwrap();

    reader.apply_sync_message_settled(unit).unwrap();

    let rows = reader.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(
        rows[0].cell(reader.table("todos").unwrap(), "body"),
        Some(Value::String(body))
    );
}

#[test]
fn handcrafted_large_descriptor_is_rejected_but_node_staged_preparation_can_publish() {
    let schema = two_column_schema();
    let (_temp_dir, mut node) = open_node_with_schema(node(0x75), schema);
    let logical = "prepared logical value/".repeat(10_000);
    let prepared = groove::large_values::prepare(
        groove::large_values::LargeValueKind::String,
        logical.as_bytes(),
    )
    .unwrap();
    let forged = MergeableCommit::new("todos", row(0x75), 10).cells(BTreeMap::from([
        ("title".to_owned(), Value::String("title".to_owned())),
        ("body".to_owned(), Value::Large(prepared.value_ref.clone())),
    ]));
    assert!(matches!(
        node.commit_mergeable_settled(forged),
        Err(Error::InvalidMergeableCommit(_))
    ));

    let logical_commit = MergeableCommit::new("todos", row(0x75), 11).cells(BTreeMap::from([(
        "title".to_owned(),
        Value::String("title".to_owned()),
    )]));
    let (admitted, _) = crate::db::block_on(node.attach_large_cell_for_test(
        logical_commit,
        "body",
        groove::large_values::LargeValueKind::String,
        logical.as_bytes(),
    ))
    .unwrap();
    node.commit_mergeable_settled(admitted).unwrap();

    let rows = node.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(
        rows[0].cell(node.table("todos").unwrap(), "body"),
        Some(Value::String(logical))
    );
}

#[test]
fn parent_tuple_encoding_matches_tx_id_tuple_order() {
    let tx_id = TxId::new(TxTime::from(0x0102_0304_0506), node(0x12));
    let parent_value = Value::Tuple(vec![Value::U64(tx_id.time.0), Value::Uuid(tx_id.node.0)]);
    let descriptor = groove::records::RecordDescriptor::new([(
        "parents",
        groove::records::ValueType::Array(Box::new(groove::records::ValueType::Tuple(vec![
            groove::records::ValueType::U64,
            groove::records::ValueType::Uuid,
        ]))),
    )]);
    let record = descriptor
        .create(&[Value::Array(vec![parent_value.clone()])])
        .unwrap();

    let mut expected = Vec::new();
    expected.extend_from_slice(&tx_id.time.0.to_be_bytes());
    expected.extend_from_slice(tx_id.node.as_bytes());
    assert_eq!(record, expected);
    assert_eq!(
        descriptor.bind(&record).get_array_element(0, 0).unwrap(),
        parent_value
    );
}

#[test]
fn lowered_record_wrapper_field_indexes_match_open_descriptors() {
    let schema = two_column_schema();
    debug_assert_lowered_layouts(&schema);
    let (_temp_dir, mut node) = open_node_with_schema(node(0x19), schema.clone());
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row(0x19), 10).cells(BTreeMap::from([
            ("title".to_owned(), Value::String("layout".to_owned())),
            ("body".to_owned(), Value::String("descriptor".to_owned())),
        ])),
    )
    .unwrap();

    let rows = node.current_rows("todos", DurabilityTier::Local).unwrap();
    assert_eq!(rows[0].row_uuid(), row(0x19));
    assert_eq!(rows[0].cell_at(0), Some(Value::String("layout".to_owned())));
    assert_eq!(
        rows[0].cell_at(1),
        Some(Value::String("descriptor".to_owned()))
    );
}

#[test]
fn policy_graph_perf_fixture_version_layouts_round_trip_all_storage_records() {
    fn fixture_schema() -> JazzSchema {
        let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../packages/jazz-tools/src/testing/fixtures/policy-graph-perf/schema-source.json");
        let source: serde_json::Value =
            serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
        let source = serde_json::from_value::<std::collections::BTreeMap<_, _>>(
            source["mergedSchema"].clone(),
        )
        .unwrap()
        .into_iter()
        .collect();
        crate::schema::JazzSchema::new(&source).unwrap()
    }

    fn sample_value(column_type: &groove::schema::ColumnType, seed: u8) -> Value {
        match column_type {
            groove::schema::ColumnType::U8 => Value::U8(seed),
            groove::schema::ColumnType::U16 => Value::U16(u16::from(seed) * 17),
            groove::schema::ColumnType::U32 => Value::U32(u32::from(seed) * 65_537),
            groove::schema::ColumnType::U64 => Value::U64(u64::MAX - u64::from(seed)),
            groove::schema::ColumnType::I32 => Value::I32(i32::from(seed) - 128),
            groove::schema::ColumnType::I64 => Value::I64(i64::from(seed) - 128),
            groove::schema::ColumnType::F64 => Value::F64(f64::from(seed) + 0.5),
            groove::schema::ColumnType::Bool => Value::Bool(seed & 1 == 0),
            groove::schema::ColumnType::String => Value::String(format!("fixture-value-{seed}")),
            groove::schema::ColumnType::Bytes => Value::Bytes(vec![seed, seed.wrapping_add(1)]),
            groove::schema::ColumnType::Internal(_) => {
                panic!("logical fixture values cannot target internal physical columns")
            }
            groove::schema::ColumnType::Uuid => Value::Uuid(uuid::Uuid::from_bytes([seed; 16])),
            groove::schema::ColumnType::EnumTag(_) => Value::EnumTag(0),
            groove::schema::ColumnType::Tuple(members) => Value::Tuple(
                members
                    .iter()
                    .enumerate()
                    .map(|(idx, member)| sample_value(member, seed.wrapping_add(idx as u8 + 1)))
                    .collect(),
            ),
            groove::schema::ColumnType::Array(member) => Value::Array(vec![
                sample_value(member, seed.wrapping_add(1)),
                sample_value(member, seed.wrapping_add(2)),
            ]),
            groove::schema::ColumnType::Nullable(member) => {
                Value::Nullable(Some(Box::new(sample_value(member, seed.wrapping_add(1)))))
            }
            groove::schema::ColumnType::Record(descriptor) => {
                let values = descriptor
                    .fields()
                    .iter()
                    .enumerate()
                    .map(|(idx, field)| sample_value(&field.value_type, seed.wrapping_add(idx as u8 + 1)))
                    .collect::<Vec<_>>();
                Value::Record(groove::records::OwnedRecord::new(
                    descriptor.create(&values).unwrap(),
                    **descriptor,
                ))
            }
            groove::schema::ColumnType::Enum(_) => {
                panic!("Jazz public schemas do not expose whole-row Groove enums")
            }
        }
    }

    fn parts_for(table: &TableSchema, seed: u8, deletion: Option<DeletionEvent>) -> VersionRowParts {
        VersionRowParts {
            table: table.name.clone(),
            branch_key: BranchKey::default(),
            row_uuid: RowUuid(uuid::Uuid::from_bytes([seed; 16])),
            tx_node_alias: NodeAlias(u64::from(seed) + 10),
            schema_version_alias: SchemaVersionAlias(u64::from(seed) + 20),
            tx_time: TxTime::from(u64::from(seed) + 30),
            parents: vec![TxId::new(
                TxTime::from(u64::from(seed) + 1),
                node(seed.wrapping_add(1)),
            )],
            created_by: AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([seed.wrapping_add(2); 16])),
            created_at: TxTime::from(u64::from(seed) + 40),
            updated_by: AuthorSubject::for_test_uuid(uuid::Uuid::from_bytes([seed.wrapping_add(3); 16])),
            updated_at: TxTime::from(u64::from(seed) + 50),
            cells: table
                .columns
                .iter()
                .enumerate()
                .map(|(idx, column)| {
                    let seed = seed.wrapping_add((idx as u8).wrapping_add(4));
                    // JSON remains string-shaped in the public schema, but
                    // its schema-derived storage descriptor uses the sealed
                    // kind-witnessed scalar representation. Feed it valid
                    // JSON so the physical codec can construct that record.
                    let value = if column.large_value_kind
                        == crate::schema::LargeValueSemanticKind::Json
                    {
                        Value::String(format!(r#"{{"fixture":{seed}}}"#))
                    } else {
                        sample_value(&column.column_type, seed)
                    };
                    (
                        column.name.clone(),
                        value,
                    )
                })
                .collect(),
            authored_columns: deletion
                .is_none()
                .then(|| table.columns.iter().map(|column| column.name.clone()).collect()),
            deletion,
        }
    }

    let schema = fixture_schema();
    assert!(!schema.tables.is_empty());
    for (idx, table) in schema.tables.iter().enumerate() {
        let seed = (idx as u8).wrapping_add(1);
        let content = VersionRow::from_parts_with_schema_version(
            table,
            parts_for(table, seed, None),
            None,
            None,
        )
        .unwrap();
        assert_eq!(content.record.descriptor().fields(), table.history_storage_table().record_schema().fields());
        let content_values = content.record.to_values().unwrap();
        assert_eq!(
            table
                .history_storage_table()
                .record_schema()
                .create(&content_values)
                .unwrap(),
            content.record.raw()
        );

        let current_values = global_current_values(table, &content, Some(GlobalTime(7))).unwrap();
        let global_current_table = table.global_current_storage_tables().remove(0);
        global_current_table
            .record_schema()
            .create(&current_values)
            .unwrap();

        let deletion = VersionRow::from_parts_with_schema_version(
            table,
            parts_for(table, seed.wrapping_add(100), Some(DeletionEvent::Deleted)),
            None,
            None,
        )
        .unwrap();
        assert_eq!(deletion.record.descriptor().fields(), table.register_storage_table().record_schema().fields());
        let deletion_values = deletion.record.to_values().unwrap();
        assert_eq!(
            table
                .register_storage_table()
                .record_schema()
                .create(&deletion_values)
                .unwrap(),
            deletion.record.raw()
        );

        let register_current_values =
            register_global_current_values(&deletion, Some(GlobalTime(8)));
        let register_global_current_table = table.global_current_storage_tables().remove(1);
        register_global_current_table
            .record_schema()
            .create(&register_current_values)
            .unwrap();
    }
}

#[test]
fn mergeable_commits_persist_transaction_and_history_rows() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    let tx = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "write tests".to_owned(),
            )])),
        )
        .unwrap();

    assert_eq!(tx.time, TxTime::from(10));
    assert_eq!(
        node.visible_current_cells("todos", row)
            .unwrap()
            .unwrap()
            .get("title")
            .unwrap(),
        &v("write tests")
    );
    let history = node
        .physical_history_source_graph(node.catalogue.current_schema_version_id, "todos")
        .unwrap();
    let mut database = node.into_database();
    assert!(
        !database
            .query(select_all("jazz_transactions"))
            .unwrap()
            .is_empty()
    );
    assert!(database.query_graph(history).unwrap().iter().next().is_some());
}

#[test]
fn authoring_stamps_explicit_child_after_parent_time() {
    let (_temp_dir, mut core) = open_node();
    let parent = TxId::new(TxTime::from(10_000), node(0x77));
    let child = core
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x71), 1)
                .parents(vec![parent])
                .cells(title_cells("child")),
        )
        .unwrap();

    assert!(
        child.time > parent.time,
        "author must stamp explicit child after parent: child={child:?}, parent={parent:?}"
    );
}
#[test]
fn deletion_register_hides_and_restore_reveals_current_content() {
    let (_temp_dir, mut node) = open_node();
    let row = row(7);
    node.commit_mergeable_settled(MergeableCommit::new("todos", row, 10).cells(title_cells("base")))
        .unwrap();
    node.commit_mergeable_settled(MergeableCommit::new("todos", row, 12).deletion(DeletionEvent::Deleted))
        .unwrap();

    assert!(node.visible_current_cells("todos", row).unwrap().is_none());

    node.commit_mergeable_settled(MergeableCommit::new("todos", row, 13).cells(title_cells("revived")))
        .unwrap();
    assert!(node.visible_current_cells("todos", row).unwrap().is_none());

    node.commit_mergeable_settled(MergeableCommit::new("todos", row, 14).deletion(DeletionEvent::Restored))
        .unwrap();

    assert_eq!(
        node.visible_current_cells("todos", row)
            .unwrap()
            .unwrap()
            .get("title")
            .unwrap(),
        &v("revived")
    );
    assert_eq!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.cell(&schema().tables[0], "title").unwrap().to_owned())
            .collect::<Vec<_>>(),
        [v("revived")]
    );
    assert!(
        node.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn durability_tier_ladder_orders_edge_between_local_and_global() {
    assert!(DurabilityTier::None < DurabilityTier::Local);
    assert!(DurabilityTier::Local < DurabilityTier::Edge);
    assert!(DurabilityTier::Edge < DurabilityTier::Global);
}

#[test]
fn edge_current_rows_exclude_purely_local_pending_writes() {
    let (_temp_dir, mut node) = open_node();
    let row = row(0xe1);
    node.commit_mergeable_settled(
        MergeableCommit::new("todos", row, 10).cells(title_cells("local only")),
    )
    .unwrap();

    assert_eq!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row]
    );
    assert!(
        node.current_rows("todos", DurabilityTier::Edge)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn edge_current_rows_include_edge_accepted_ahead_versions() {
    let (_temp_dir, mut node) = open_node();
    let row = row(0xe2);
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row, 10).cells(title_cells("edge accepted")),
        )
        .unwrap();

    // E1: edge-accept produced directly; E2 wires the acceptance path.
    node.apply_fate_update(tx_id, Fate::Accepted, None, Some(DurabilityTier::Edge))
        .unwrap();

    assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);
    assert_eq!(
        node.current_rows("todos", DurabilityTier::Edge)
            .unwrap()
            .into_iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row]
    );
    assert!(
        node.current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn global_fate_cleans_ahead_current_overlay() {
    let (_temp_dir, mut node) = open_node();
    let row = row(0xe3);
    let tx_id = node
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row, 10).cells(title_cells("globally accepted")),
        )
        .unwrap();
    assert_eq!(ahead_current_row_count(&mut node, "todos"), 1);

    node.apply_fate_update(
        tx_id,
        Fate::Accepted,
        Some(GlobalTime(1)),
        Some(DurabilityTier::Global),
    )
    .unwrap();

    assert_eq!(ahead_current_row_count(&mut node, "todos"), 0);
    assert_eq!(
        node.current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row, title_cells("globally accepted"))])
    );
}

#[test]
fn writer_subscription_reads_own_pending_at_local_tier() {
    let (_client_dir, mut client) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let mut peer = PeerState::new();
    let row = row(7);
    let (tx_id, unit) = client
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row, 10).cells(BTreeMap::from([(
                "title".to_owned(),
                "optimistic".to_owned(),
            )])),
        )
        .unwrap();

    assert_eq!(
        client
            .subscription_current_rows("todos", DurabilityTier::Local)
            .unwrap(),
        vec![(row, title_cells("optimistic"))]
    );
    assert!(
        client
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap()
            .is_empty()
    );

    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit_settled(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    client.apply_sync_message_settled(fate).unwrap();
    assert_eq!(
        client.transaction_state_settled(tx_id).unwrap(),
        (Fate::Accepted, Some(GlobalTime::new(10, 0).unwrap()), DurabilityTier::Global)
    );

    let update = peer.current_rows_update(&mut core, "todos").unwrap();
    client.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        client
            .subscription_current_rows("todos", DurabilityTier::Global)
            .unwrap(),
        vec![(row, title_cells("optimistic"))]
    );
}


#[test]
fn late_lower_hlc_child_is_rejected_at_admission() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let parent = TxId::new(TxTime::from(200), node(1));
    let child = TxId::new(TxTime::from(50), node(1));

    let [parent_fate] = core
        .ingest_commit_unit_settled(
            Transaction {
                tx_id: parent,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version_record(row, Vec::new(), title_cells("parent"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        parent_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            ..
        }
    ));

    let [child_fate] = core
        .ingest_commit_unit_settled(
            Transaction {
                tx_id: child,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version_record(
                row,
                vec![parent],
                title_cells("child"),
                None,
            )],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        child_fate,
        SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::CausalityViolation),
            global_time: None,
            durability: None,
        }
    );
    assert!(
        core.row_history("todos", row)
            .unwrap()
            .iter()
            .all(|entry| entry.tx_id() != child)
    );
    assert_eq!(
        core.transaction_record(child).unwrap().fate,
        Fate::Rejected(RejectionReason::CausalityViolation)
    );
}
#[test]
fn unlawful_child_with_known_parent_rejects_before_global_state() {
    let (_dir, mut core) = open_node_with_uuid(node(9));
    let row = row(7);
    let parent = TxId::new(TxTime::from(400), node(1));
    let child = TxId::new(TxTime::from(100), node(1));

    let parent_state = core
        .ingest_commit_unit_settled(
            Transaction {
                tx_id: parent,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version_record(row, Vec::new(), title_cells("parent"), None)],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert!(matches!(
        parent_state.as_slice(),
        [SyncMessage::FateUpdate {
            fate: Fate::Accepted,
            global_time: Some(_),
            ..
        }]
    ));

    let child_state = core
        .ingest_commit_unit_settled(
            Transaction {
                tx_id: child,
                kind: TxKind::Mergeable,
                n_total_writes: 1,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            vec![version_record(
                row,
                vec![parent],
                title_cells("child"),
                None,
            )],
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert_eq!(
        child_state,
        vec![SyncMessage::FateUpdate {
            tx_id: child,
            fate: Fate::Rejected(RejectionReason::CausalityViolation),
            global_time: None,
            durability: None,
        }]
    );
    assert_eq!(
        global_winner_tx(&mut core, "todos", row, VersionLayer::Content),
        Some(parent)
    );
    assert_eq!(
        core.current_rows("todos", DurabilityTier::Global).unwrap(),
        vec![(row, title_cells("parent"))]
    );
}
