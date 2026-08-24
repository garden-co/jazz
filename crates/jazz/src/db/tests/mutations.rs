//! Facade mutation lifecycle, local visibility, and operation-level authorization tests.

use super::*;

fn branch_column_reference_policy_schema() -> JazzSchema {
    let policy = PublicPolicyExpr::Exists {
        table: "branches".to_owned(),
        condition: Box::new(PublicPolicyExpr::And(vec![
            public_outer_eq("branch_key", "branch_id"),
            public_session_eq("owner", &["user_id"]),
        ])),
    };
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("branches")
                    .fk_column("branch_key", "branches")
                    .column("name", PublicColumnType::Text)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(PublicPolicyExpr::True)
                            .with_insert(PublicPolicyExpr::True)
                            .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                            .with_delete(PublicPolicyExpr::True),
                    ),
            )
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .fk_column("branch_id", "branches")
                    .column("title", PublicColumnType::Text)
                    .branch_by("branch_id")
                    .policies(
                        PublicTablePolicies::new()
                            .with_select(policy.clone())
                            .with_insert(policy.clone())
                            .with_update(Some(policy.clone()), policy.clone())
                            .with_delete(policy),
                    ),
            ),
    )
}

#[test]
fn admitted_server_authorizes_branch_write_through_referenced_application_row() {
    let schema = branch_column_reference_policy_schema();
    let owner = AuthorSubject::for_test_bytes([0x76; 16]);
    let outsider = AuthorSubject::for_test_bytes([0x77; 16]);
    let branch = row(0x78);
    let selector = BranchSelector::new([("branch_id", Value::Uuid(branch.0))]);
    let server = open_core(0x75, AuthorSubject::SYSTEM, &schema);
    server
        .insert_with_id(
            "branches",
            branch,
            BTreeMap::from([
                ("branch_key".to_owned(), Value::Uuid(branch.0)),
                ("name".to_owned(), Value::String("draft".to_owned())),
                ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
            ]),
        )
        .unwrap();
    let owner_client = open_db(0x76, owner, &schema);
    let outsider_client = open_db(0x77, outsider, &schema);
    let (owner_transport, owner_server_transport) = duplex();
    let _owner_upstream = crate::db::block_on(owner_client.connect_upstream(owner_transport));
    let _owner_subscriber = server.accept_subscriber(owner_server_transport, owner);
    let (outsider_transport, outsider_server_transport) = duplex();
    let _outsider_upstream =
        crate::db::block_on(outsider_client.connect_upstream(outsider_transport));
    let _outsider_subscriber = server.accept_subscriber(outsider_server_transport, outsider);

    let accepted = owner_client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("allowed".to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(0x79)),
                target: crate::db::ExactWriteTarget::Branch(selector.clone()),
                ..Default::default()
            },
        )
        .unwrap();
    owner_client.tick().unwrap();
    server.tick().unwrap();
    owner_client.tick().unwrap();
    assert_eq!(
        block_on(accepted.wait(DurabilityTier::Global)).unwrap(),
        accepted.mergeable_tx_id()
    );

    let denied = outsider_client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("denied".to_owned()))]),
            crate::db::InsertOptions {
                row_id: Some(row(0x7a)),
                target: crate::db::ExactWriteTarget::Branch(selector),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&outsider_client, &server, &denied);
}

#[test]
fn db_facade_mutation_lifecycle_writes_reads_deletes_and_restores() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells("draft todo", false),
            Default::default(),
        )
        .unwrap();
    let todo = write.row_uuid();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(false)));

    let write = db
        .update(
            "todos",
            todo,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("draft todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));

    let write = db.delete("todos", todo, Default::default()).unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert!(prepared_read(&db, &query).is_empty());

    let write = db
        .restore(
            "todos",
            todo,
            Some(doctest_support::todo_cells("restored todo", true)),
            Default::default(),
        )
        .unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();

    let rows = prepared_read(&db, &query);
    assert_eq!(row_ids(&rows), vec![todo]);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("restored todo".to_owned()))
    );
    assert_eq!(rows[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn high_level_large_value_apis_keep_descriptors_private_and_publish_edits() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let mut title = "a".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 257);
    title.push_str("🙂tail");
    let write = db
        .insert(
            "todos",
            doctest_support::todo_cells(&title, false),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    let original_ref = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("oversized title stayed inline: {other:?}"),
        }
    });

    let unrelated = db
        .update(
            "todos",
            row,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
            Default::default(),
        )
        .unwrap();
    block_on(unrelated.wait(DurabilityTier::Local)).unwrap();
    let after_unrelated = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("updated title became inline: {other:?}"),
        }
    });
    assert_eq!(original_ref, after_unrelated);

    assert_eq!(
        block_on(db.read_value_range("todos", row, "title", 10..18)).unwrap(),
        b"aaaaaaaa"
    );
    assert_eq!(
        block_on(db.read_text_utf16_range(
            "todos",
            row,
            "title",
            title.encode_utf16().count() as u64 - 6..title.encode_utf16().count() as u64 - 4,
        ))
        .unwrap(),
        "🙂"
    );

    let append = block_on(db.append_value("todos", row, "title", b"/appended".to_vec())).unwrap();
    block_on(append.wait(DurabilityTier::Local)).unwrap();
    title.push_str("/appended");

    let splice = block_on(db.splice_value("todos", row, "title", 4, 3, b"XYZ".to_vec())).unwrap();
    block_on(splice.wait(DurabilityTier::Local)).unwrap();
    title.replace_range(4..7, "XYZ");
    let edited_ref = block_on(async {
        let mut node = db.node.node.lock().await;
        match node
            .current_physical_cell_in_schema(db.schema_version_id, "todos", row, "title")
            .await
            .unwrap()
            .unwrap()
        {
            Value::Large(value_ref) => value_ref,
            other => panic!("edited title became inline: {other:?}"),
        }
    });
    assert_eq!(original_ref.root, edited_ref.root);

    let rows = db
        .read(&db.prepare_query(&db.table("todos")).unwrap())
        .unwrap();
    assert_eq!(
        rows[0].cell(&doctest_support::schema().tables[0], "title"),
        Some(Value::String(title))
    );

    let json = format!(
        "{{\"padding\":\"{}\",\"selected\":{{\"answer\":42}}}}",
        "p".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES)
    );
    let json_row = db
        .insert(
            "todos",
            doctest_support::todo_cells(&json, false),
            Default::default(),
        )
        .unwrap()
        .row_uuid();
    assert_eq!(
        block_on(db.read_json_pointer("todos", json_row, "title", "/selected/answer")).unwrap(),
        Some(serde_json::json!(42))
    );
}

#[test]
fn high_level_large_value_reads_authorize_before_descriptor_lookup() {
    let allowed = "readable-large-value/".repeat(5_000);
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("documents")
                .column("body", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::eq_literal(
                            "body",
                            PublicValue::Text(allowed.clone()),
                        ))
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                        .with_delete(PublicPolicyExpr::True),
                ),
        ),
    );
    let reader = AuthorSubject::for_test_bytes([0x4e; 16]);
    let db = open_db(0x4e, reader, &schema);
    let visible = row(0x4e);
    let hidden = row(0x4f);
    db.insert(
        "documents",
        BTreeMap::from([("body".to_owned(), Value::String(allowed.clone()))]),
        InsertOptions {
            row_id: Some(visible),
            ..Default::default()
        },
    )
    .unwrap();
    db.insert(
        "documents",
        BTreeMap::from([(
            "body".to_owned(),
            Value::String(format!("{}x", &allowed[..allowed.len() - 1])),
        )]),
        InsertOptions {
            row_id: Some(hidden),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        block_on(db.read_value_range("documents", visible, "body", 0..8)).unwrap(),
        b"readable"
    );
    let denied = block_on(db.read_value_range("documents", hidden, "body", 0..8)).unwrap_err();
    assert_eq!(denied.code, ErrorCode::NotObserved);
}

#[test]
fn nullable_large_text_uses_the_same_high_level_read_and_edit_surface() {
    let schema = build_public_db_test_schema(PublicSchemaBuilder::new().table(
        PublicTableSchemaBuilder::new("notes").nullable_column("body", PublicColumnType::Text),
    ));
    let db = open_db(0x4d, AuthorSubject::SYSTEM, &schema);
    let row = row(0x4d);
    let body = "n".repeat(groove::large_values::INLINE_VALUE_MAX_BYTES + 73);
    db.insert(
        "notes",
        BTreeMap::from([(
            "body".to_owned(),
            Value::Nullable(Some(Box::new(Value::String(body.clone())))),
        )]),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    )
    .unwrap();

    assert_eq!(
        block_on(db.read_value_range("notes", row, "body", 7..13)).unwrap(),
        b"nnnnnn"
    );
    block_on(db.append_value("notes", row, "body", b"end".to_vec())).unwrap();
    let result = db
        .read(&db.prepare_query(&db.table("notes")).unwrap())
        .unwrap();
    assert_eq!(
        result[0].cell(&schema.tables[0], "body"),
        Some(Value::Nullable(Some(Box::new(Value::String(format!(
            "{body}end"
        ))))))
    );
}

#[test]
fn db_facade_runs_saas_shaped_local_lane_end_to_end() {
    let schema = schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: NodeUuid::from_bytes([0x11; 16]),
            author: owner,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0x11))),
    }))
    .unwrap();

    let query = Query::from("todos");
    let write = db
        .insert(
            "todos",
            cells("ship facade", false, owner),
            Default::default(),
        )
        .unwrap();
    let todo = write.row_uuid();
    let table = &schema.tables[0];
    let rows = prepared_read(&db, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("ship facade".to_owned()))
    );
    block_on(write.wait(DurabilityTier::Local)).unwrap();

    db.update(
        "todos",
        todo,
        BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
        Default::default(),
    )
    .unwrap();
    let updated = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn core_db_self_finalizes_own_writes_to_global() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);

    let write = core
        .insert("todos", cells("authority write", false, owner))
        .unwrap();
    // No upstream, no connection: a Core Db is the authority, so its own
    // write is immediately Accepted/Global.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    assert_eq!(core.read(&Query::from("todos")).unwrap().len(), 1);
}

#[test]
fn db_sync_surface_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    // A local client write is Local and queued for upload.
    let write = client
        .insert(
            "todos",
            cells("from client", false, author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();

    // Drive: client uploads the commit unit -> server (authority) accepts to
    // Global and sends the fate back -> client applies the fate.
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    // The client's own write reached Global once the authority fate landed.
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    // The authority received and applied the uploaded row.
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn byte_wire_uploads_client_writes_for_authority_fate() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = byte_duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert(
            "todos",
            cells("from client", false, author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_uploads_client_exclusive_commit_for_global_fate() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe1);
    let exclusive = client.exclusive_tx().unwrap();
    exclusive
        .insert(
            "todos",
            cells("exclusive", false, author),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    let tx_id = exclusive.commit().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        client.write_state(tx_id).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    let server_rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(server_rows[0].row_uuid(), row);
}

#[test]
fn db_sync_surface_returns_exclusive_conflict_fate_to_client() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe2);
    let first = client.exclusive_tx().unwrap();
    let second = client.exclusive_tx().unwrap();
    first
        .insert(
            "todos",
            cells("first", false, author),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    second
        .insert(
            "todos",
            cells("second", false, author),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    let first_tx = first.commit().unwrap();
    let second_error = second.commit().unwrap_err();
    assert_eq!(second_error.code, ErrorCode::TransactionConflict);
    assert!(second_error.message.contains("visible parent changed"));

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        client.write_state(first_tx).unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    let table = &schema.tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("first".to_owned()))
    );
}

/// An authority rejection with no application waiter is delivered once through
/// the mutation-error callback on the following scheduled database tick. This
/// is an ordinary client connection, so the fate has no edge-forwarding route
/// and must still run the local write-state handler.
#[test]
fn unhandled_rejection_is_delivered_as_mutation_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert(
            "todos",
            cells("rejected", false, author),
            Default::default(),
        )
        .unwrap();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();

    client.tick().unwrap();
    assert!(events.borrow().is_empty());
    client.tick().unwrap();

    let events = events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(
        client.write_state(write.mergeable_tx_id()).unwrap(),
        WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            durability: DurabilityTier::Edge,
        }
    );
    assert_eq!(events[0].code, "permission_denied");
    assert_eq!(
        events[0].transaction.transaction_id,
        TransactionId::from_committed_tx(write.mergeable_tx_id())
    );
    assert_eq!(events[0].transaction.kind, TransactionKind::Mergeable);
}

/// A live application waiter consumes an authority rejection and prevents the
/// fallback mutation-error callback from firing, including when the fate has
/// no edge-forwarding route and only the ordinary local handler can notify it.
#[test]
fn waited_rejection_is_not_delivered_as_mutation_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let client = open_db(0xc2, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert(
            "todos",
            cells("waited rejection", false, author),
            Default::default(),
        )
        .unwrap();
    let wait_result = Rc::new(RefCell::new(None));
    let callback_result = Rc::clone(&wait_result);
    client.wait_for_transaction_with(
        write.mergeable_tx_id(),
        DurabilityTier::Edge,
        move |result| *callback_result.borrow_mut() = Some(result),
    );
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();

    client.tick().unwrap();
    assert_eq!(
        wait_result.borrow_mut().take().unwrap().unwrap_err().code,
        ErrorCode::WriteRejected
    );
    client.tick().unwrap();

    assert!(events.borrow().is_empty());
    assert!(
        client
            .node
            .node()
            .borrow()
            .rejected_transaction(write.mergeable_tx_id())
            .is_none()
    );
}

/// An explicit wait that begins after the rejection was queued still consumes
/// it before the next-tick fallback callback can deliver it.
#[test]
fn wait_after_rejection_suppresses_queued_mutation_error() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let client = open_db(0xc4, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert(
            "todos",
            cells("late wait rejection", false, author),
            Default::default(),
        )
        .unwrap();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();
    client.tick().unwrap();

    let error =
        block_on(client.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Edge))
            .unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
    assert!(error.message.contains("AuthorizationDenied"));
    assert!(
        error
            .message
            .contains(&format!("{:?}", write.mergeable_tx_id()))
    );
    client.tick().unwrap();

    assert!(events.borrow().is_empty());
    assert!(
        client
            .node
            .node()
            .borrow()
            .rejected_transaction(write.mergeable_tx_id())
            .is_none()
    );
}

/// A rejected transaction that was not delivered before shutdown is recovered
/// from durable storage and delivered after the reopened client registers its
/// callback.
#[test]
fn undelivered_mutation_error_is_recovered_after_reopen() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc3; 16]);
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xc3; 16]),
        author,
    };
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
    }))
    .unwrap();
    let (client_transport, mut authority_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let write = client
        .insert(
            "todos",
            cells("rejected before reopen", false, author),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        })
        .unwrap();
    client.tick().unwrap();

    drop(write);
    drop(upstream);
    drop(authority_transport);
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
    }))
    .unwrap();
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    reopened.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));
    reopened.tick().unwrap();

    let events = events.borrow();
    assert_eq!(events.len(), 1);
    assert_eq!(
        events[0].transaction.transaction_id,
        TransactionId::from_committed_tx(tx_id)
    );
    drop(events);
    drop(reopened);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let acknowledged_reopen = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xc3))),
    }))
    .unwrap();
    let replayed_events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&replayed_events);
    acknowledged_reopen.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));
    acknowledged_reopen.tick().unwrap();
    assert!(replayed_events.borrow().is_empty());
}

#[test]
fn write_fate_and_durability_are_queryable_through_facade() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert(
            "todos",
            cells("facade state", false, author),
            Default::default(),
        )
        .unwrap();
    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        }
    );
    assert_eq!(
        client.write_state(write.mergeable_tx_id()).unwrap(),
        write.write_state().unwrap()
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Accepted,
            durability: DurabilityTier::Global,
        }
    );
    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
}

#[test]
fn session_upload_rejects_forged_made_by_without_ingesting_rows() {
    let schema = owner_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let forged_author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(forged_author)
                .cells(cells("forged", false, session_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let handle = WriteHandle {
        node: Rc::downgrade(&client.node.node),
        row_uuid: row(0xf1),
        tx_id,
        local_tier: DurabilityTier::Local,
    };
    let err = block_on(handle.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn session_upload_uses_connection_identity_for_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert(
            "todos",
            cells("honest", false, session_author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
}

// This sync-boundary test is intentionally lower-level: the public policy
// test app reaches this same prepared server write-policy path, but cannot
// distinguish a malformed prepared claim binding from an ordinary denial.
#[test]
fn admitted_server_prepared_write_policy_binds_text_user_id_claim() {
    let schema = owner_id_session_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let alice_client = open_db(0xa1, alice, &schema);
    let bob_client = open_db(0xb2, bob, &schema);
    let alice_claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String("alice-subject".to_owned()),
    )]);
    alice_client.set_identity_claims(alice, alice_claims.clone());
    let bob_claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String("bob-subject".to_owned()),
    )]);
    bob_client.set_identity_claims(bob, bob_claims.clone());

    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_client.connect_upstream(alice_transport));
    let _alice_subscriber =
        server.accept_subscriber_with_claims(alice_server_transport, alice, alice_claims);
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = crate::db::block_on(bob_client.connect_upstream(bob_transport));
    let _bob_subscriber =
        server.accept_subscriber_with_claims(bob_server_transport, bob, bob_claims);

    let accepted = alice_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("owned by alice".to_owned()),
                ),
                (
                    "owner_id".to_owned(),
                    Value::String("alice-subject".to_owned()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    alice_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    assert_eq!(
        block_on(accepted.wait(DurabilityTier::Global)).unwrap(),
        accepted.mergeable_tx_id(),
        "the admitted server must bind public session.user_id as Text in its prepared write-policy plan"
    );

    let denied = bob_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("spoofed by bob".to_owned()),
                ),
                (
                    "owner_id".to_owned(),
                    Value::String("alice-subject".to_owned()),
                ),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(0xb2)),
                identity: crate::db::WriteIdentity::Session(bob),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&bob_client, &server, &denied);
    let rows = server.read(&Query::from("messages")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), accepted.row_uuid());
}

#[test]
fn admitted_server_prepared_write_policy_coerces_string_user_id_to_uuid_column() {
    let schema = owner_uuid_session_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa3; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb3; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let alice_client = open_db(0xa3, alice, &schema);
    let bob_client = open_db(0xb3, bob, &schema);
    let alice_claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String(alice.test_uuid().to_string()),
    )]);
    let bob_claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String(bob.test_uuid().to_string()),
    )]);
    alice_client.set_identity_claims(alice, alice_claims.clone());
    bob_client.set_identity_claims(bob, bob_claims.clone());

    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_client.connect_upstream(alice_transport));
    let _alice_subscriber =
        server.accept_subscriber_with_claims(alice_server_transport, alice, alice_claims);
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = crate::db::block_on(bob_client.connect_upstream(bob_transport));
    let _bob_subscriber =
        server.accept_subscriber_with_claims(bob_server_transport, bob, bob_claims);

    let accepted = alice_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("owned by alice".to_owned()),
                ),
                ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();
    alice_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    assert_eq!(
        block_on(accepted.wait(DurabilityTier::Global)).unwrap(),
        accepted.mergeable_tx_id(),
        "the prepared descriptor must preserve UUID policy columns while coercing public user_id text"
    );

    let denied = bob_client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("spoofed by bob".to_owned()),
                ),
                ("owner_id".to_owned(), Value::Uuid(alice.test_uuid())),
            ]),
            crate::db::InsertOptions {
                row_id: Some(row(0xb3)),
                identity: crate::db::WriteIdentity::Session(bob),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&bob_client, &server, &denied);
    let rows = server.read(&Query::from("messages")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), accepted.row_uuid());
}

#[test]
fn admitted_server_prepared_write_policy_fails_closed_for_wrong_user_id_type() {
    let schema = owner_id_session_write_schema();
    let author = AuthorSubject::for_test_bytes([0xa4; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa4, author, &schema);
    let claims = BTreeMap::from([("user_id".to_owned(), Value::Bool(true))]);
    client.set_identity_claims(author, claims.clone());

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(server_transport, author, claims);
    let write = client
        .insert(
            "messages",
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("must not ingest".to_owned()),
                ),
                ("owner_id".to_owned(), Value::String("true".to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();

    client.tick().unwrap();
    let error = server.tick().unwrap_err();
    assert!(
        error.to_string().contains("user_id has wrong type"),
        "a non-coercible claim must fail before authorization support can admit the write: {error}"
    );
    assert!(
        server.read(&Query::from("messages")).unwrap().is_empty(),
        "a malformed session claim must never ingest a protected row"
    );
    drop(write);
}

#[test]
fn session_delete_uses_current_row_for_owner_write_policy() {
    let schema = owner_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let other_author = AuthorSubject::for_test_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert(
            "todos",
            cells("owned", false, session_author),
            Default::default(),
        )
        .unwrap();
    let row = write.row_uuid();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();

    let bad_delete = client
        .delete(
            "todos",
            row,
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(other_author),
                ..Default::default()
            },
        )
        .unwrap();
    assert_authority_rejects_staged_write(&client, &server, &bad_delete);
    let client_rows = prepared_read(&client, &Query::from("todos"));
    assert_eq!(client_rows.len(), 1);
    assert_eq!(client_rows[0].row_uuid(), row);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    let delete = client
        .delete(
            "todos",
            row,
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(session_author),
                ..Default::default()
            },
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_upload_uses_backend_policy_and_stores_user_made_by() {
    let schema = owner_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(backend.connect_upstream(backend_transport));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );
    backend.set_identity_claims(attributed_user, test_provider_claims(attributed_user));
    backend.tick().unwrap();
    server.tick().unwrap();

    let tx_id = backend
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xf2), backend.next_now_ms())
                .made_by(attributed_user)
                .permission_subject(backend_author)
                .cells(cells("attributed", false, backend_author)),
        )
        .unwrap();
    backend
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    let SyncMessage::CommitUnit { tx, .. } =
        server.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        panic!("expected stored commit unit");
    };
    assert_eq!(tx.made_by, attributed_user);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xf2));
}

#[test]
fn trusted_backend_upload_applies_session_claim_assertions_for_write_policy() {
    let schema = editor_claim_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let editor_author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(backend.connect_upstream(backend_transport));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    backend.set_identity_claims(
        editor_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = backend
        .insert(
            "todos",
            cells("claim-backed", false, editor_author),
            crate::db::InsertOptions {
                row_id: Some(row(0xe1)),
                identity: crate::db::WriteIdentity::Session(editor_author),
                ..Default::default()
            },
        )
        .unwrap();

    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(write.wait(DurabilityTier::Global)).unwrap(),
        write.mergeable_tx_id()
    );
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row(0xe1));
}

#[test]
fn session_claim_assertions_require_trusted_backend_upload() {
    let schema = editor_claim_write_schema();
    let session_author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xe1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    client.set_identity_claims(
        session_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = client
        .insert(
            "todos",
            cells("claim-backed", false, session_author),
            crate::db::InsertOptions {
                row_id: Some(row(0xe2)),
                identity: crate::db::WriteIdentity::Session(session_author),
                ..Default::default()
            },
        )
        .unwrap();

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let err = block_on(write.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn trusted_backend_delete_uses_permission_subject_parent_for_write_policy() {
    let schema = owner_write_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(backend.connect_upstream(backend_transport));
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );
    // The trusted backend may attribute a mutation to this admitted provider
    // session, but its UUID owner policy still reads the raw provider claim.
    backend.set_identity_claims(attributed_user, test_provider_claims(attributed_user));
    backend.tick().unwrap();
    server.tick().unwrap();

    let insert = backend
        .insert(
            "todos",
            cells("attributed", false, attributed_user),
            crate::db::InsertOptions {
                row_id: Some(row(0xf3)),
                identity: crate::db::WriteIdentity::Session(attributed_user),
                ..Default::default()
            },
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    block_on(insert.wait(DurabilityTier::Global)).unwrap();

    let delete = backend
        .delete(
            "todos",
            row(0xf3),
            crate::db::DeleteOptions {
                identity: crate::db::WriteIdentity::Session(attributed_user),
                ..Default::default()
            },
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();

    assert_eq!(
        block_on(delete.wait(DurabilityTier::Global)).unwrap(),
        delete.mergeable_tx_id()
    );
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn client_insert_advice_is_unknown_without_writing() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    owner_db.set_identity_claims(owner, test_provider_claims(owner));
    other_db.set_identity_claims(other, test_provider_claims(other));

    assert_eq!(
        owner_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap(),
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        other_db
            .can_insert("todos", cells("owned", false, owner))
            .unwrap(),
        PermissionAdvice::Unknown,
    );
    assert_eq!(
        owner_db
            .authorize_insert_for_identity("todos", cells("owned", false, owner), owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_insert_for_identity("todos", cells("owned", false, owner), other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 0);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn client_delete_advice_is_unknown_without_mutating() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let other = AuthorSubject::for_test_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    owner_db.set_identity_claims(owner, test_provider_claims(owner));
    other_db.set_identity_claims(other, test_provider_claims(other));
    let row = row(1);
    let write = owner_db
        .insert(
            "todos",
            cells("owned", false, owner),
            crate::db::InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(
            owner_db
                .node
                .node
                .borrow_mut()
                .commit_unit_for(write.mergeable_tx_id())
                .unwrap(),
        )
        .unwrap();

    assert_eq!(
        owner_db.can_delete("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        other_db.can_delete("todos", row).unwrap(),
        PermissionAdvice::Unknown
    );
    assert_eq!(
        owner_db
            .authorize_delete_for_identity("todos", row, owner)
            .unwrap(),
        PermissionAdvice::Allowed,
    );
    assert_eq!(
        owner_db
            .authorize_delete_for_identity("todos", row, other)
            .unwrap(),
        PermissionAdvice::Denied,
    );
    assert_eq!(prepared_read(&owner_db, &owner_db.table("todos")).len(), 1);
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 0);
}

#[test]
fn core_attributed_insert_uses_core_identity_for_policy_and_user_for_made_by() {
    let schema = owner_write_schema();
    let backend = AuthorSubject::for_test_bytes([0xbe; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, backend, &schema);
    let write = core
        .insert_attributed(
            attributed_user,
            "todos",
            cells("attributed", false, backend),
        )
        .unwrap();

    let unit = core
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, attributed_user);
    assert_eq!(core.read(&core.table("todos")).unwrap().len(), 1);
}

#[test]
fn client_attributed_insert_to_different_user_is_rejected() {
    let schema = owner_write_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let err = match client
        .insert(
            "todos",
            cells("forged", false, client_author),
            crate::db::InsertOptions {
                identity: crate::db::WriteIdentity::Attribution(attributed_user),
                ..Default::default()
            },
        )
        .resolve()
    {
        Ok(_) => panic!("client attribution should be rejected"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert_eq!(prepared_read(&client, &client.table("todos")).len(), 0);
}

#[test]
fn default_insert_keeps_subject_and_made_by_equal() {
    let schema = owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0xa1, owner, &schema);
    let write = db
        .insert("todos", cells("default", false, owner), Default::default())
        .unwrap();
    let unit = db
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, .. } = unit else {
        panic!("commit unit expected");
    };

    assert_eq!(tx.made_by, owner);
    assert_eq!(prepared_read(&db, &db.table("todos")).len(), 1);
}
