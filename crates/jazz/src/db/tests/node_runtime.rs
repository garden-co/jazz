//! Shared node scheduling, dirty-generation cascades, and connection servicing tests.

use super::*;

#[test]
fn volatile_storage_does_not_change_direct_upstream_subscription_topology() {
    let schema = schema();
    let mut client = open_db(0xc7, AuthorId::from_bytes([0xc7; 16]), &schema);
    client.set_non_durable_client();

    let (transport, _server_transport, outbound) = duplex_with_client_outbound_tap();
    let mut _upstream = client.connect_upstream(transport);
    let prepared = client.prepare_query(&client.table("todos")).unwrap();
    let mut _attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();

    let tier = outbound.borrow().iter().find_map(|message| match message {
        SyncMessage::RegisterShape { opts, .. } => Some(opts.tier),
        _ => None,
    });
    assert_eq!(
        tier,
        Some(DurabilityTier::Global),
        "volatile local storage must not make a direct Core connection advertise a Local upstream"
    );
}

/// A Core immediately refreshes a peer-edge subscriber that was visited before
/// a later client upload in the same service pass, so Bob receives Alice's
/// later canonical row without needing an unrelated next websocket frame.
///
/// ```text
/// bob --empty Global subscribe--> peer edge --> Core
/// alice --later CommitUnit----------------------> Core
///                                                |
///                 Core ViewUpdate <--------------+
/// bob <--- peer-edge local IVM refresh <---------+
/// ```
///
/// The peer connection is deliberately accepted before Alice's connection.
/// That makes Core service the already-covered peer first, then accept Alice's
/// write. The one Core tick following Alice's upload must revisit the earlier
/// peer before it returns; otherwise an event-driven websocket host has no
/// reason to call Core again and Bob stays indefinitely at the old empty cut.
#[test]
fn core_later_client_upload_refreshes_earlier_peer_subscription_in_same_tick() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob_author = AuthorId::from_bytes([0xb1; 16]);
    let core = open_core(0xd1, AuthorId::SYSTEM, &schema);
    let mut peer_edge = open_db(0xd2, AuthorId::SYSTEM, &schema);
    let mut bob = open_db(0xd3, bob_author, &schema);

    // Keep the Core-to-peer queue observable, and accept this peer before
    // Alice so the ordering under test is fixed.
    let (peer_transport, core_transport, core_to_peer) = duplex_with_server_outbound_tap();
    let mut _peer_upstream = peer_edge.connect_upstream(peer_transport);
    let mut _core_peer = core.accept_subscriber_with_trust(
        core_transport,
        AuthorId::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (bob_transport, peer_client_transport) = duplex();
    let mut _bob_upstream = bob.connect_upstream(bob_transport);
    let mut _peer_client = peer_edge.accept_subscriber(peer_client_transport, bob_author);

    let query = bob.table("todos");
    let mut subscription = prepared_subscribe(&mut bob, &query, global_subscribe_opts()).unwrap();
    let opening = (0..32)
        .find_map(|_| {
            bob.tick().unwrap();
            peer_edge.tick().unwrap();
            core.tick().unwrap();
            peer_edge.tick().unwrap();
            bob.tick().unwrap();
            subscription.try_next_event()
        })
        .expect("Bob receives the established empty Global view");
    assert!(event_settled(&opening));
    assert!(opened_rows(opening).is_empty());
    assert!(
        core_to_peer.borrow().is_empty(),
        "the empty opening has been fully consumed before Alice writes"
    );

    let mut alice_edge = open_db(0xd4, alice, &schema);
    let (alice_transport, core_alice_transport) = duplex();
    let mut _alice_upstream = alice_edge.connect_upstream(alice_transport);
    let mut _core_alice = core.accept_subscriber(core_alice_transport, alice);
    let write = alice_edge
        .insert_with_id("todos", row(0xd5), cells("later row", false, alice))
        .unwrap();

    // One edge tick uploads Alice's local commit; one Core tick finalizes it
    // and must also serve the earlier peer connection.
    alice_edge.tick().unwrap();
    core.tick().unwrap();
    let later_view_updates = core_to_peer
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate {
                    result_member_adds,
                    settled_through,
                    ..
                } if *settled_through > GlobalSeq(0)
                    && result_member_adds.iter().any(|member| {
                        member.as_row().is_some_and(|(table, row_uuid, tx_id)| {
                            table.as_str() == "todos"
                                && row_uuid == row(0xd5)
                                && tx_id == write.tx_id
                        })
                    })
            )
        })
        .count();
    assert_eq!(
        later_view_updates, 1,
        "the first Core service pass after Alice's upload sends the later canonical membership to the already-covered peer"
    );

    // Applying that upstream ViewUpdate must dirty and refresh the existing
    // Bob connection in the same peer-edge service pass.
    peer_edge.tick().unwrap();
    bob.tick().unwrap();
    let delivered = subscription
        .try_next_event()
        .expect("Bob receives the later row without a retry or a new query");
    let (added, updated, removed) = delta_rows(delivered);
    assert_eq!(row_ids(&added), vec![row(0xd5)]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    // The bounded second pass clears its dirty work. A quiet later tick must
    // neither replay the unchanged view nor self-arm another serving loop.
    core.tick().unwrap();
    assert!(
        core_to_peer.borrow().is_empty(),
        "a post-cascade idle tick emits no unchanged peer update"
    );
}

/// An Edge immediately flushes an upload queued by a later client connection
/// through the upstream connection that was already visited in the same pass.
///
/// The upstream connection is deliberately installed first. One client tick
/// places the commit on the Edge subscriber transport; one Edge tick must both
/// ingest it and emit the corresponding Core-bound `CommitUnit`.
#[test]
fn edge_later_client_upload_flushes_earlier_upstream_in_same_tick() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_db(0xd1, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xd2, alice, &schema);

    let (edge_transport, _core_transport, edge_to_core) = duplex_with_client_outbound_tap();
    let mut _edge_upstream = edge.connect_upstream(edge_transport);

    let (client_transport, edge_client_transport) = duplex();
    let mut _client_upstream = client.connect_upstream(client_transport);
    let mut _edge_client = edge.accept_subscriber(edge_client_transport, alice);

    let write = client
        .insert_with_id("todos", row(0xd3), cells("later upload", false, alice))
        .unwrap();
    client.tick().unwrap();
    edge.tick().unwrap();

    let uploads = edge_to_core
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id
            )
        })
        .count();
    assert_eq!(
        uploads, 1,
        "one Edge service pass flushes the later client upload through the earlier upstream link"
    );

    edge.tick().unwrap();
    assert_eq!(
        edge_to_core
            .borrow()
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id
                )
            })
            .count(),
        1,
        "a quiet follow-up tick does not replay the same upload"
    );
}

#[test]
fn write_state_waiter_resolves_on_remote_fate_update() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);

    let write = client
        .insert("todos", cells("wait for fate", false, owner))
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    assert_eq!(
        client.write_state(tx_id).unwrap().durability,
        DurabilityTier::Local
    );

    let changed = client.next_write_state_change(tx_id);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(changed);

    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
}

#[test]
fn db_sync_surface_preserves_creator_provenance_across_peer_update() {
    let schema = schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut receiver = open_db(0xc1, alice, &schema);

    let write = server
        .insert_attributed(alice, "todos", cells("created by alice", false, alice))
        .unwrap();
    let row = write.row_uuid();
    let query = Query::from("todos");
    let create_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message(create_unit)
        .unwrap();

    server.next_now_ms.set(2);
    let bob_update = server
        .update_attributed(
            bob,
            "todos",
            row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("updated by bob".to_owned()),
            )]),
        )
        .unwrap();
    block_on(bob_update.wait(DurabilityTier::Global)).unwrap();
    let server_rows = server.read(&query).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(
        server_rows[0].provenance().unwrap().unwrap().updated_by,
        bob
    );
    let update_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = update_unit else {
        panic!("expected update commit unit");
    };
    assert_eq!(versions[0].created_by(), alice);
    assert_eq!(versions[0].updated_by(), bob);
    let receiver_updates = receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert!(
        receiver_updates.iter().any(|message| {
            matches!(
                message,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            )
        }),
        "receiver should accept the update, got {receiver_updates:?}"
    );
    let receiver_unit = receiver
        .node
        .node
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit {
        versions: receiver_versions,
        ..
    } = receiver_unit
    else {
        panic!("expected receiver commit unit");
    };
    assert_eq!(receiver_versions[0].created_by(), alice);
    assert_eq!(receiver_versions[0].updated_by(), bob);

    let alice_rows = prepared_read(&mut receiver, &query);
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].row_uuid(), row);
    let provenance = alice_rows[0]
        .provenance()
        .unwrap()
        .expect("current rows should carry provenance");
    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.updated_by, bob);
    assert!(
        provenance.created_at < provenance.updated_at,
        "updating a row must preserve creator provenance while advancing updater provenance"
    );
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_private_table_query() {
    let schema = owner_id_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut writer = open_db(0xa1, alice, &schema);
    let mut reader = open_db(0xb2, bob, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let mut _writer_upstream = writer.connect_upstream(writer_transport);
    let mut _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                ("owner_id".to_owned(), Value::String(alice.0.to_string())),
            ]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let mut _reader_upstream = reader.connect_upstream(reader_transport);
    let mut _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&mut reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    assert!(prepared_all(&mut reader, &query, edge_subscribe_opts()).is_empty());
}

/// A prepared trusted-serving read binds each request session's text `user_id`
/// independently: Alice receives her seeded message while Bob receives none.
///
/// ```text
/// system ──seed owner_id=alice──► server prepared read
///                                      │
///                         Alice session ─┼──► [alice message]
///                           Bob session ─└──► []
/// ```
#[test]
fn prepared_server_read_binds_text_session_user_id_per_session() {
    // Mirror the public test app: a nullable camel-case `ownerId` grants to
    // its matching session or to every session when unowned. In particular,
    // this exercises the disjunctive policy plan rather than only the
    // scalar-equality fast path.
    let read_policy = Query::from("todos").filter(any_of([
        eq(col("ownerId"), claim("user_id")),
        is_null(col("ownerId")),
    ]));
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("ownerId", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(read_policy))
    .with_write_policy(Policy::public())]);
    let mut server = open_db(0x5e, AuthorId::SYSTEM, &schema);
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let alice_subject = "alice-session-subject";
    let bob_subject = "bob-session-subject";
    let _ = server.set_identity_claims(
        alice,
        BTreeMap::from([(String::from("user_id"), Value::String(alice_subject.into()))]),
    );
    let _ = server.set_identity_claims(
        bob,
        BTreeMap::from([(String::from("user_id"), Value::String(bob_subject.into()))]),
    );

    let seeded = server
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("for alice".to_owned())),
                ("done".to_owned(), Value::Bool(false)),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String(alice_subject.into())))),
                ),
            ]),
        )
        .expect("system seed must write the protected message");
    block_on(seeded.wait(DurabilityTier::Local)).expect("seed must settle locally");

    // The public `where({ id })` facade contributes an ordinary prepared
    // parameter alongside the hidden policy claim. Keep that mixed binding in
    // this regression so the descriptor cannot accidentally bind Alice's
    // claim into the query-id slot (or vice versa).
    let query = Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(seeded.row_uuid().0))));
    let prepared = prepared(&mut server, &query);
    let alice_rows = block_on(server.all_for_identity(&prepared, ReadOpts::default(), alice))
        .expect("Alice's prepared read must evaluate against her session claims");
    let bob_rows = block_on(server.all_for_identity(&prepared, ReadOpts::default(), bob))
        .expect("Bob's prepared read must evaluate against his session claims");

    assert_eq!(row_ids(&alice_rows), vec![seeded.row_uuid()]);
    assert!(bob_rows.is_empty());
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_after_runtime_schema_publish() {
    let public_schema = owner_id_public_schema();
    let permission_schema = owner_id_read_schema();
    let alice = AuthorId::from_bytes([0xa1; 16]);
    let bob = AuthorId::from_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &public_schema);
    let mut writer = open_db(0xa1, alice, &permission_schema);
    let mut alice_reader = open_db(0xa2, alice, &permission_schema);
    let mut reader = open_db(0xb2, bob, &permission_schema);

    let schema_version = SchemaVersion::new(permission_schema.clone());
    let schema_id = schema_version.id;
    let acks = server.publish_schema(schema_version).unwrap();
    assert!(acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));
    let current_acks = server
        .server
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema_id,
            },
        })
        .unwrap();
    assert!(current_acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));

    let (writer_transport, server_writer_transport) = duplex();
    let mut _writer_upstream = writer.connect_upstream(writer_transport);
    let mut _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                ("owner_id".to_owned(), Value::String(alice.0.to_string())),
            ]),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (alice_transport, server_alice_transport) = duplex();
    let mut _alice_upstream = alice_reader.connect_upstream(alice_transport);
    let mut _alice_subscriber = server.accept_subscriber_with_claims(
        server_alice_transport,
        alice,
        BTreeMap::from([("user_id".to_owned(), Value::String(alice.0.to_string()))]),
    );
    let query = Query::from("messages");
    let mut alice_subscription =
        prepared_subscribe(&mut alice_reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(alice_subscription.next_raw()).unwrap()).is_empty());
    alice_reader.tick().unwrap();
    server.tick().unwrap();
    alice_reader.tick().unwrap();
    let mut delta = delta_rows(block_on(alice_subscription.next_raw()).unwrap());
    for _ in 0..2 {
        if !delta.0.is_empty() {
            break;
        }
        delta = delta_rows(block_on(alice_subscription.next_raw()).unwrap());
    }
    let (added, updated, removed) = delta;
    assert_eq!(
        added.len(),
        1,
        "Alice's matching text session claim must read the seeded row"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        row_ids(&prepared_all(
            &mut alice_reader,
            &query,
            edge_subscribe_opts()
        )),
        vec![added[0].row_uuid()],
    );

    let (reader_transport, server_reader_transport) = duplex();
    let mut _reader_upstream = reader.connect_upstream(reader_transport);
    let mut _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([("user_id".to_owned(), Value::String(bob.0.to_string()))]),
    );
    let mut subscription = prepared_subscribe(&mut reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    assert!(prepared_all(&mut reader, &query, edge_subscribe_opts()).is_empty());
}

#[test]
fn detached_subscriber_is_not_served_on_server_tick() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();

    assert!(server.server.detach_connection(&subscriber));
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(prepared_read(&mut client, &query).is_empty());
}

#[test]
fn byte_wire_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_bytes, server_bytes) = byte_duplex_raw();
    let server_inbound = Rc::clone(&server_bytes.inbound);
    let mut _upstream =
        client.connect_upstream(Box::new(WireTransportAdapter::current(client_bytes)));
    let mut _subscriber = server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_bytes)),
        client_author,
    );

    let query = Query::from("todos");
    let mut subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    {
        let queued = server_inbound.borrow();
        let first = queued.front().expect("register shape frame");
        let second = queued.get(1).expect("subscribe frame");
        let mut decoder = WireStreamDecoder::new(current_wire_features()).unwrap();
        let first = match decode_frame(first).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let second = match decode_frame(second).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let SyncMessage::RegisterShape { shape_id, .. } = first else {
            panic!("expected RegisterShape, got {first:?}");
        };
        let SyncMessage::Subscribe(subscribe) = second else {
            panic!("expected Subscribe, got {second:?}");
        };
        assert_eq!(subscribe.shape_id, shape_id);
        assert_eq!(subscribe.subscription.shape_id, shape_id);
    }
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&mut client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&mut client, &query).len(), 2);
}

#[test]
fn single_upstream_tick_applies_multiple_subscription_updates() {
    let schema = issue_schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    let project = row(1);
    server
        .insert_with_id(
            "projects",
            project,
            BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
        )
        .unwrap();
    seed(
        &server,
        "issues",
        issue_cells("API", "open", owner, project, 5, &["api"], None),
    );

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);

    let projects = Query::from("projects");
    let issues = Query::from("issues");
    let mut project_subscription =
        prepared_subscribe(&mut client, &projects, global_subscribe_opts()).unwrap();
    let mut issue_subscription =
        prepared_subscribe(&mut client, &issues, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(project_subscription.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(issue_subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    let stats = client.tick().unwrap();

    assert_eq!(prepared_read(&mut client, &projects).len(), 1);
    assert_eq!(prepared_read(&mut client, &issues).len(), 1);
    assert_eq!(stats.subscription_events, 2);
    assert_eq!(
        delta_rows(block_on(project_subscription.next_raw()).unwrap())
            .0
            .len(),
        1
    );
    assert_eq!(
        delta_rows(block_on(issue_subscription.next_raw()).unwrap())
            .0
            .len(),
        1
    );
}

#[test]
fn subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    // The subscriber registers the whole-table query shape; explicit
    // current-row serving then sends the facade-level initial snapshot.
    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&mut client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = duplex();
    let mut _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= full_bytes,
        "resume catch-up should stay bounded by the initial full response"
    );
    assert_eq!(prepared_read(&mut client, &query).len(), 3);
    assert!(
        prepared_read(&mut client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn byte_wire_subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 1);
    let mut _upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&mut client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 2);
    let mut _resumed_upstream = client.connect_upstream(client_transport);
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "byte-wire resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= full_bytes,
        "byte-wire resume catch-up should stay bounded by the initial full response"
    );
    assert_eq!(prepared_read(&mut client, &query).len(), 3);
    assert!(
        prepared_read(&mut client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn connect_upstream_announces_existing_subscriptions_on_first_tick() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let mut _subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let mut _upstream = client.connect_upstream(client_transport);

    client.tick().unwrap();
    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    let SyncMessage::RegisterShape { shape_id, .. } = first else {
        panic!("expected existing subscription shape to be registered upstream first");
    };
    let SyncMessage::Subscribe(subscribe) = second else {
        panic!("expected existing subscription to be announced upstream second");
    };
    assert_eq!(subscribe.shape_id, shape_id);
    assert_eq!(subscribe.subscription.shape_id, shape_id);
}

// SessionClaims has no distinct public state once the receiving NodeState has
// ignored an identical map, so wire-count coverage must inspect the transport.
// The policy-visible integration coverage lives above this facade; this test
// protects the otherwise unobservable wire-chatter contract.
#[test]
fn repeated_identical_session_claims_emit_once_on_a_live_connection() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);

    let _ = client.set_identity_claims(client_author, claims.clone());
    let _ = client.set_identity_claims(client_author, claims);
    client.tick().unwrap();

    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { .. })
    ));
    assert!(
        upstream_transport.try_recv().is_none(),
        "an unchanged claim map must not produce another wire message"
    );
}

// This is lower-level for the same reason as the wire-count test above. In
// particular, it is the regression that a global deduplication would miss:
// each newly attached transport must receive the current map independently.
#[test]
fn current_session_claims_reach_late_and_reconnected_upstreams() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);

    let _ = client.set_identity_claims(client_author, claims.clone());
    let (first_transport, mut first_upstream_transport) = duplex();
    let first_upstream = client.connect_upstream(first_transport);
    client.tick().unwrap();
    assert!(matches!(
        first_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == claims
    ));
    assert!(first_upstream_transport.try_recv().is_none());

    let _ = client.set_identity_claims(client_author, claims.clone());
    assert!(client.detach_connection(&first_upstream));

    let (reconnected_transport, mut reconnected_upstream_transport) = duplex();
    let mut _reconnected_upstream = client.connect_upstream(reconnected_transport);
    client.tick().unwrap();
    assert!(matches!(
        reconnected_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == claims
    ));
    assert!(reconnected_upstream_transport.try_recv().is_none());
}

#[test]
fn changed_session_claims_advance_delivery_after_an_identical_call() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let reader = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    let writer = BTreeMap::from([("role".to_owned(), Value::String("writer".to_owned()))]);

    let _ = client.set_identity_claims(client_author, reader.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { claims, .. }) if claims == reader
    ));

    let _ = client.set_identity_claims(client_author, reader);
    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    let _ = client.set_identity_claims(client_author, writer.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims })
            if identity == client_author && claims == writer
    ));
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn global_subscription_registers_array_subquery_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let mut _subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
}

#[test]
fn array_subquery_attachment_registers_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let prepared = prepared(&mut client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
    client.detach_query(attachment);
}

#[test]
fn upload_is_not_marked_sent_after_one_shot_backpressure_and_retries() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let outbound = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let transport = BackpressureOnceTransport {
        outbound: Rc::clone(&outbound),
        failed: false,
    };
    let mut _upstream = client.connect_upstream(Box::new(transport));

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("retry", false, client_author)),
        )
        .unwrap();
    client
        .node
        .outbox
        .borrow_mut()
        .push(PendingUpload { tx_id, unit: None });

    client.tick().unwrap();
    assert!(outbound.borrow().is_empty());
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .sync_metrics()
            .transport_backpressure_retries,
        1
    );

    client.tick().unwrap();
    let sent = outbound.borrow_mut().pop_front().unwrap();
    let SyncMessage::CommitUnit { tx, .. } = sent else {
        panic!("expected retried commit upload");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert!(outbound.borrow_mut().pop_front().is_none());
}

#[test]
fn local_missing_upload_body_still_kills_sync_driver() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, _server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let missing_tx = TxId::new(
        crate::time::TxTime(client.next_now_ms()),
        NodeUuid::from_bytes([0xee; 16]),
    );
    client.node.outbox.borrow_mut().push(PendingUpload {
        tx_id: missing_tx,
        unit: None,
    });

    let error = client.tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        error.message.contains("missing transaction"),
        "unexpected local-fatal error: {}",
        error.message
    );
}

#[test]
fn detach_connection_removes_connection_from_db_ticks() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let mut _subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let upstream = client.connect_upstream(client_transport);

    assert!(client.detach_connection(&upstream));
    assert!(!client.detach_connection(&upstream));

    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn accepted_subscriber_is_served_under_subscriber_author_identity() {
    let schema = owner_read_schema();
    let subscriber_author = AuthorId::from_bytes([0xc1; 16]);
    let server_author = AuthorId::from_bytes([0x5e; 16]);
    let other_author = AuthorId::from_bytes([0xd1; 16]);
    let server = open_core(0x5e, server_author, &schema);
    let mut client = open_db(0xc1, subscriber_author, &schema);

    let visible = seed(
        &server,
        "todos",
        cells("for subscriber", false, subscriber_author),
    );
    seed(&server, "todos", cells("for server", false, server_author));
    seed(
        &server,
        "todos",
        cells("for someone else", false, other_author),
    );

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, subscriber_author);
    let query = Query::from("todos");
    let mut subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (rows, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&rows), vec![visible]);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("for subscriber".to_owned()))
    );
}

#[test]
fn client_initial_sync_flush_cadence_preserves_public_snapshot_delivery() {
    let schema = schema();
    let server = open_core(0xd4, AuthorId::SYSTEM, &schema);
    for ordinal in 0..3_u8 {
        server
            .insert_with_id(
                "todos",
                row(0xd0 + ordinal),
                BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String(format!("server {ordinal}")),
                    ),
                    ("done".to_owned(), Value::Bool(false)),
                ]),
            )
            .unwrap();
    }

    let client_author = AuthorId::from_bytes([0xd5; 16]);
    let mut client = open_db(0xd5, client_author, &schema);
    client
        .set_initial_sync_flush_cadence(InitialSyncFlushCadence::every(
            NonZeroUsize::new(2).unwrap(),
        ))
        .unwrap();
    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = client.table("todos");
    let mut subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();

    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event) = subscription.try_next_event()
            && opened_rows(event).len() == 3
        {
            return;
        }
    }
    panic!("client configured with a cadence must receive the initial snapshot");
}
