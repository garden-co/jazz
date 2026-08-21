//! Facade mutation lifecycle, local visibility, and operation-level authorization tests.

use super::*;

#[test]
fn db_facade_mutation_lifecycle_writes_reads_deletes_and_restores() {
    let db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let table = &doctest_support::schema().tables[0];

    let write = db
        .insert("todos", doctest_support::todo_cells("draft todo", false))
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

    let write = db.delete("todos", todo).unwrap();
    doctest_support::block_on(write.wait(DurabilityTier::Local)).unwrap();
    assert!(prepared_read(&db, &query).is_empty());

    let write = db
        .restore(
            "todos",
            todo,
            doctest_support::todo_cells("restored todo", true),
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
fn db_facade_runs_saas_shaped_local_lane_end_to_end() {
    let schema = schema();
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let owner = AuthorId::from_bytes([0xa1; 16]);
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
        .insert("todos", cells("ship facade", false, owner))
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
    )
    .unwrap();
    let updated = prepared_all(&db, &query, ReadOpts::default());
    assert_eq!(updated.len(), 1);
    assert_eq!(updated[0].cell(table, "done"), Some(Value::Bool(true)));
}

#[test]
fn core_db_self_finalizes_own_writes_to_global() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorId::SYSTEM, &schema);

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
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    // A local client write is Local and queued for upload.
    let write = client
        .insert("todos", cells("from client", false, author))
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
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = byte_duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert("todos", cells("from client", false, author))
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
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe1);
    let exclusive = client.exclusive_tx().unwrap();
    exclusive
        .insert_with_id("todos", row, cells("exclusive", false, author))
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
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let row = row(0xe2);
    let first = client.exclusive_tx().unwrap();
    let second = client.exclusive_tx().unwrap();
    first
        .insert_with_id("todos", row, cells("first", false, author))
        .unwrap();
    second
        .insert_with_id("todos", row, cells("second", false, author))
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
    let author = AuthorId::from_bytes([0xc1; 16]);
    let client = open_db(0xc1, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert("todos", cells("rejected", false, author))
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
    let author = AuthorId::from_bytes([0xc2; 16]);
    let client = open_db(0xc2, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert("todos", cells("waited rejection", false, author))
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
    let author = AuthorId::from_bytes([0xc4; 16]);
    let client = open_db(0xc4, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let events = Rc::new(RefCell::new(Vec::new()));
    let callback_events = Rc::clone(&events);
    client.on_mutation_error(Rc::new(move |event| {
        callback_events.borrow_mut().push(event.clone());
    }));

    let write = client
        .insert("todos", cells("late wait rejection", false, author))
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
    let author = AuthorId::from_bytes([0xc3; 16]);
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
    let upstream = client.connect_upstream(client_transport);
    let write = client
        .insert("todos", cells("rejected before reopen", false, author))
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
    let author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, author);

    let write = client
        .insert("todos", cells("facade state", false, author))
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
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let forged_author = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
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
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert("todos", cells("honest", false, session_author))
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
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
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
    let _alice_upstream = alice_client.connect_upstream(alice_transport);
    let _alice_subscriber =
        server.accept_subscriber_with_claims(alice_server_transport, alice, alice_claims);
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = bob_client.connect_upstream(bob_transport);
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
        .insert_with_id_for_identity(
            bob,
            "messages",
            row(0xb2),
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
    let alice = AuthorId::from_bytes([0xa3; 16]);
    let bob = AuthorId::from_bytes([0xb3; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let alice_client = open_db(0xa3, alice, &schema);
    let bob_client = open_db(0xb3, bob, &schema);
    let alice_claims = BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]);
    let bob_claims = BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]);
    alice_client.set_identity_claims(alice, alice_claims.clone());
    bob_client.set_identity_claims(bob, bob_claims.clone());

    let (alice_transport, alice_server_transport) = duplex();
    let _alice_upstream = alice_client.connect_upstream(alice_transport);
    let _alice_subscriber =
        server.accept_subscriber_with_claims(alice_server_transport, alice, alice_claims);
    let (bob_transport, bob_server_transport) = duplex();
    let _bob_upstream = bob_client.connect_upstream(bob_transport);
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
                ("owner_id".to_owned(), Value::Uuid(alice.0)),
            ]),
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
        .insert_with_id_for_identity(
            bob,
            "messages",
            row(0xb3),
            BTreeMap::from([
                (
                    "body".to_owned(),
                    Value::String("spoofed by bob".to_owned()),
                ),
                ("owner_id".to_owned(), Value::Uuid(alice.0)),
            ]),
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
    let author = AuthorId::from_bytes([0xa4; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xa4, author, &schema);
    let claims = BTreeMap::from([("user_id".to_owned(), Value::Bool(true))]);
    client.set_identity_claims(author, claims.clone());

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
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
    let session_author = AuthorId::from_bytes([0xc1; 16]);
    let other_author = AuthorId::from_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    let write = client
        .insert("todos", cells("owned", false, session_author))
        .unwrap();
    let row = write.row_uuid();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(write.wait(DurabilityTier::Global)).unwrap();

    let bad_delete = client
        .delete_for_identity(other_author, "todos", row)
        .unwrap();
    assert_authority_rejects_staged_write(&client, &server, &bad_delete);
    let client_rows = prepared_read(&client, &Query::from("todos"));
    assert_eq!(client_rows.len(), 1);
    assert_eq!(client_rows[0].row_uuid(), row);
    let rows = server.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    let delete = client
        .delete_for_identity(session_author, "todos", row)
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
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let tx_id = backend
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
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
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let editor_author = AuthorId::from_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
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
        .insert_with_id_for_identity(
            editor_author,
            "todos",
            row(0xe1),
            cells("claim-backed", false, editor_author),
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
    let session_author = AuthorId::from_bytes([0xe1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xe1, session_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, session_author);

    client.set_identity_claims(
        session_author,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    let write = client
        .insert_with_id_for_identity(
            session_author,
            "todos",
            row(0xe2),
            cells("claim-backed", false, session_author),
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
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let backend = open_db(0xb0, backend_author, &schema);

    let (backend_transport, server_transport) = duplex();
    let _upstream = backend.connect_upstream(backend_transport);
    let _subscriber = server.accept_subscriber_with_trust(
        server_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let insert = backend
        .insert_with_id_for_identity(
            attributed_user,
            "todos",
            row(0xf3),
            cells("attributed", false, attributed_user),
        )
        .unwrap();
    backend.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    block_on(insert.wait(DurabilityTier::Global)).unwrap();

    let delete = backend
        .delete_for_identity(attributed_user, "todos", row(0xf3))
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
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);

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
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let other = AuthorId::from_bytes([0xb2; 16]);
    let owner_db = open_db(0xa1, owner, &schema);
    let other_db = open_db(0xb2, other, &schema);
    let row = row(1);
    let write = owner_db
        .insert_with_id("todos", row, cells("owned", false, owner))
        .unwrap();
    other_db
        .node
        .node
        .borrow_mut()
        .apply_sync_message(
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
    assert_eq!(prepared_read(&other_db, &other_db.table("todos")).len(), 1);
}

#[test]
fn core_attributed_insert_uses_core_identity_for_policy_and_user_for_made_by() {
    let schema = owner_write_schema();
    let backend = AuthorId::from_bytes([0xbe; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
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
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);
    let client = open_db(0xc1, client_author, &schema);

    let err = match client.insert_attributed(
        attributed_user,
        "todos",
        cells("forged", false, client_author),
    ) {
        Ok(_) => panic!("client attribution should be rejected"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::WriteRejected);
    assert_eq!(prepared_read(&client, &client.table("todos")).len(), 0);
}

#[test]
fn default_insert_keeps_subject_and_made_by_equal() {
    let schema = owner_write_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let db = open_db(0xa1, owner, &schema);
    let write = db.insert("todos", cells("default", false, owner)).unwrap();
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
