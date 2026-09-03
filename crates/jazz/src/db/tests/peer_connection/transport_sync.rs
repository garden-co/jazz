//! General and branch-view subscription transport coverage.

use super::*;

fn branch_sync_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("branch_id", PublicColumnType::Uuid)
                .column("title", PublicColumnType::Text)
                .branch_by("branch_id")
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                        .with_delete(PublicPolicyExpr::True),
                ),
        ),
    )
}

/// A malformed authority source-closure frame rejects only its exact public
/// subscription before any receiver state or output is published.
///
/// alice owns the client stream; the authority supplies a real opening, whose
/// duplicated covered-input witness is injected at the transport boundary.
/// bob's later, distinct query proves that the peer remains usable.
///
/// alice ──subscribe──► authority ──forged duplicate──► alice (error)
/// bob   ──subscribe──────────────────────────────────► bob   (valid reset)
#[test]
fn malformed_authority_closure_reaches_only_its_public_subscription() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0x41; 16]);
    let bob = AuthorSubject::for_test_bytes([0x42; 16]);
    let server = open_core(0x43, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0x44, alice, &schema);
    let row_id = row(0x45);
    server
        .insert_with_id("todos", row_id, cells("persisted upstream", false, bob))
        .unwrap();

    let (client_transport, server_transport, _client_sent, server_sent) = duplex_with_taps();
    let _upstream = block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, alice);
    let alice_query = Query::from("todos");
    let mut alice_subscription =
        prepared_subscribe(&client, &alice_query, global_subscribe_opts()).unwrap();
    let local_opening = block_on(alice_subscription.next_raw()).expect("local opening");
    assert!(!event_settled(&local_opening));
    assert!(opened_rows(local_opening).is_empty());

    client.tick().unwrap();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        if server_sent
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ViewUpdate(_)))
        {
            break;
        }
    }
    let subscription = {
        let mut frames = server_sent.borrow_mut();
        let update = frames
            .iter_mut()
            .find_map(|message| match message {
                SyncMessage::ViewUpdate(update) => Some(update),
                _ => None,
            })
            .expect("authority must send alice's opening");
        let subscription = update.subscription;
        let duplicate = update
            .program_fact_adds
            .iter()
            .find_map(|fact| match fact {
                crate::protocol::ProgramFactEntry::CoveredInput(input) => Some(input.clone()),
                _ => None,
            })
            .expect("nonempty authority opening has a covered-input witness");
        update
            .program_fact_adds
            .push(crate::protocol::ProgramFactEntry::CoveredInput(duplicate));
        subscription
    };
    let authority_result = client
        .node
        .node
        .borrow()
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    let receipt_before = client
        .node
        .node
        .borrow()
        .applied_authority_result_generation(&authority_result);

    client
        .tick()
        .expect("malformed closure becomes a subscription error, not a peer-tick failure");
    assert_eq!(
        block_on(alice_subscription.next_raw()),
        Some(SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::InvalidAuthoritySourceClosure {
                transition: "duplicate source-closure addition".to_owned(),
            },
        }),
        "the client receives the exact safe closure-transition error without waiting"
    );
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .applied_authority_result_generation(&authority_result),
        receipt_before,
        "the rejected frame cannot advance the authority receipt"
    );
    assert!(
        prepared_read(&client, &alice_query).is_empty(),
        "the rejected frame cannot publish a partial local result"
    );

    let bob_query = Query::from("todos").filter(eq(col("title"), lit("persisted upstream")));
    let mut bob_subscription =
        prepared_subscribe(&client, &bob_query, global_subscribe_opts()).unwrap();
    let _ = block_on(bob_subscription.next_raw()).expect("bob local opening");
    client.tick().unwrap();
    let mut bob_rows = Vec::new();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = bob_subscription.try_next_event() {
            match event {
                SubscriptionEvent::Delta { added, .. } => bob_rows.extend(added),
                SubscriptionEvent::Rejected { reason } => {
                    panic!("unrelated subscription was rejected: {reason:?}")
                }
                SubscriptionEvent::Closed => panic!("unrelated subscription closed"),
            }
        }
        if bob_rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>()
            == vec![row_id]
        {
            break;
        }
    }
    assert_eq!(
        bob_rows
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

fn branch_sync_selector(byte: u8) -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([byte; 16])))])
}

fn serving_rows_in_read_view(
    server: &CoreDb,
    schema: &JazzSchema,
    query: &Query,
    identity: AuthorSubject,
    read_view: &ReadViewSpec,
) -> Vec<CurrentRow> {
    let shape = query.validate(schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    server
        .node()
        .borrow_mut()
        .query_relation_snapshot_for_serving_in_read_view(
            &shape,
            &binding,
            DurabilityTier::Global,
            identity,
            read_view,
        )
        .unwrap()
        .rows
}

fn write_deletion_register(server: &CoreDb, table: &str, row: RowUuid, branch: BranchSelector) {
    let node = server.node();
    let parents = {
        let mut state = node.borrow_mut();
        block_on(state.local_deletion_winner_tx_id_in_branch(table, &branch, row))
            .unwrap()
            .into_iter()
            .collect()
    };
    let authored_columns = branch.values.keys().cloned().collect::<BTreeSet<_>>();
    let published = block_on(
        node.borrow_mut().commit_mergeable(
            crate::node::MergeableCommit::new(table, row, server.next_now_ms())
                .made_by(AuthorSubject::SYSTEM)
                .branch(branch)
                .parents(parents)
                .authored_columns(authored_columns)
                .deletion(crate::tx::DeletionEvent::Deleted),
        ),
    )
    .unwrap();
    let tx_id = block_on(node.borrow_mut().persist_and_settle_transaction(published)).unwrap();
    let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx_id)).unwrap();
    block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).unwrap();
    server.server.mark_subscriber_connections_dirty();
}

/// Public branch-view mutations use the normal client outbox and subscriber
/// relay. This intentionally exercises both update and existing-target upsert
/// rather than injecting a handcrafted commit unit at authority.
#[test]
fn public_branch_view_update_and_upsert_relay_to_authority() {
    let schema = branch_sync_schema();
    let author = AuthorSubject::for_test_bytes([0x61; 16]);
    let server = open_core(0x62, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0x63, author, &schema);
    let base = branch_sync_selector(0x64);
    let head = branch_sync_selector(0x65);
    let first = row(0x66);
    let second = row(0x67);
    let (client_transport, server_transport) = duplex();
    let _upstream = block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        author,
        test_provider_claims(author),
    );

    for (id, title) in [(first, "first base"), (second, "second base")] {
        let write = client
            .insert(
                "todos",
                BTreeMap::from([
                    (
                        "branch_id".to_owned(),
                        Value::Uuid(uuid::Uuid::from_bytes([0x64; 16])),
                    ),
                    ("title".to_owned(), Value::String(title.to_owned())),
                ]),
                InsertOptions {
                    row_id: Some(id),
                    target: ExactWriteTarget::Branch(base.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        block_on(write.wait(DurabilityTier::Global)).unwrap();
    }

    let (_, tx_id) = block_on(client.transaction_for_identity(author, async |tx| {
        tx.update(
            "todos",
            first,
            BTreeMap::from([("title".to_owned(), Value::String("first head".to_owned()))]),
            UpdateOptions {
                target: WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        )
        .await?;
        tx.upsert(
            "todos",
            second,
            BTreeMap::from([("title".to_owned(), Value::String("second head".to_owned()))]),
            UpsertOptions {
                target: WriteTarget::BranchView {
                    head: head.clone(),
                    base: Some(BranchViewBase::Current(base.clone())),
                },
                ..Default::default()
            },
        )
        .await
    }))
    .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(matches!(
        client.write_state(tx_id).unwrap().fate,
        Fate::Accepted
    ));

    let rows = serving_rows_in_read_view(
        &server,
        &schema,
        &Query::from("todos"),
        author,
        &ReadViewSpec::branch_view(head, Some(BranchViewBase::Current(base))),
    );
    let titles = rows
        .iter()
        .map(|row| match row.cell(&schema.tables[0], "title").unwrap() {
            Value::String(title) => title,
            value => panic!("expected text title, got {value:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        titles,
        BTreeSet::from(["first head".to_owned(), "second head".to_owned()])
    );
}

#[test]
fn branch_view_copy_evidence_storage_is_independent_of_public_write_order() {
    let schema = branch_sync_schema();
    let author = AuthorSubject::for_test_bytes([0x68; 16]);
    let base = branch_sync_selector(0x69);
    let head = branch_sync_selector(0x6a);
    let first = row(0x6b);
    let second = row(0x6c);
    let build = |reverse: bool| {
        let db = open_db(0x6d, author, &schema);
        for (id, title) in [(first, "first base"), (second, "second base")] {
            db.insert(
                "todos",
                BTreeMap::from([
                    (
                        "branch_id".to_owned(),
                        Value::Uuid(uuid::Uuid::from_bytes([0x69; 16])),
                    ),
                    ("title".to_owned(), Value::String(title.to_owned())),
                ]),
                InsertOptions {
                    row_id: Some(id),
                    target: ExactWriteTarget::Branch(base.clone()),
                    ..Default::default()
                },
            )
            .unwrap();
        }
        let order = if reverse {
            [second, first]
        } else {
            [first, second]
        };
        let (_, tx_id) = block_on(db.transaction_for_identity(author, async |tx| {
            for row in order {
                tx.update(
                    "todos",
                    row,
                    BTreeMap::from([(
                        "title".to_owned(),
                        Value::String(if row == first {
                            "first head".to_owned()
                        } else {
                            "second head".to_owned()
                        }),
                    )]),
                    UpdateOptions {
                        target: WriteTarget::BranchView {
                            head: head.clone(),
                            base: Some(BranchViewBase::Current(base.clone())),
                        },
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .unwrap();
        let mut node = db.node.node.borrow_mut();
        let provenance = node
            .transaction_record(tx_id)
            .unwrap()
            .contribution_merge
            .expect("branch-view transaction stores operation evidence");
        provenance
    };

    let forward_provenance = build(false);
    let reverse_provenance = build(true);
    assert_eq!(forward_provenance, reverse_provenance);
}

#[test]
fn db_sync_surface_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
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
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

/// Refresh is a post-durability publication effect for an inbound authority
/// batch. This stays internal because the fault boundary and per-peer progress
/// receipt are not exposed through the public client API.
#[test]
fn persisted_upstream_batch_survives_subscription_refresh_failure_without_redelivery() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa2; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc2, client_author, &schema);
    seed(&server, "todos", cells("persisted upstream", false, owner));

    let (client_transport, server_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    upstream
        .borrow_mut()
        .fail_next_subscription_refresh
        .set(true);

    let applied = block_on(upstream.borrow_mut().tick())
        .expect("a post-persistence refresh failure must not fail the peer tick");
    assert_eq!(
        applied.remote_sync_applied, 1,
        "the durably applied inbound batch must be acknowledged once"
    );
    assert_eq!(
        applied.subscription_events, 1,
        "the routed subscription error must remain visible in tick progress"
    );
    assert_eq!(
        block_on(subscription.next_raw()).expect("refresh failure event"),
        SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::ServerFailure {
                code: SubscribeServerFailureCode::Internal,
            },
        },
        "refresh failure belongs to the affected subscription"
    );
    assert_eq!(
        prepared_read(&client, &query).len(),
        1,
        "refresh failure must not roll back the settled inbound batch"
    );

    let idle = block_on(upstream.borrow_mut().tick())
        .expect("the same peer connection must remain usable");
    assert_eq!(
        idle.remote_sync_applied, 0,
        "the consumed inbound batch must not be reported or applied again"
    );
    assert!(
        client
            .node
            .connections
            .borrow()
            .iter()
            .any(|connection| Rc::ptr_eq(connection, &upstream)),
        "refresh failure must not force reconnect of the peer that applied the batch"
    );
}

/// A globally accepted client write belongs to the authority's durable current
/// state, not to the lifetime of the client connection that first uploaded it.
/// A reader that connects only after the writer has gone away must therefore
/// receive the same current rows from a fresh subscription.
#[test]
fn globally_accepted_client_rows_survive_writer_disconnect_for_fresh_reader() {
    let schema = schema();
    let writer_author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let reader_author = AuthorSubject::for_test_bytes([0xb1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xa1, writer_author, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let writer_subscriber = server.accept_subscriber(server_writer_transport, writer_author);

    let mut expected_rows = Vec::new();
    for index in 0..12 {
        let write = writer
            .insert(
                "todos",
                cells(&format!("durable-writer-row-{index}"), false, writer_author),
                Default::default(),
            )
            .unwrap();
        expected_rows.push(write.row_uuid());
        writer.tick().unwrap();
        server.tick().unwrap();
        writer.tick().unwrap();
        assert_eq!(
            block_on(write.wait(DurabilityTier::Global)).unwrap(),
            write.mergeable_tx_id(),
            "writer row {index} must not report Global before authority acceptance"
        );
    }
    expected_rows.sort();
    assert_eq!(
        row_ids(&server.read(&Query::from("todos")).unwrap()),
        expected_rows,
        "the authority must retain all globally accepted writer rows before disconnect"
    );

    assert!(server.server.detach_connection(&writer_subscriber));
    assert!(writer.detach_connection(&upstream));
    drop(writer);

    let reader = open_db(0xb1, reader_author, &schema);
    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = crate::db::block_on(reader.connect_upstream(reader_transport));
    let _reader_subscriber = server.accept_subscriber(server_reader_transport, reader_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&reader, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let mut received = RelationSnapshot::default();
    for _ in 0..32 {
        reader.tick().unwrap();
        server.tick().unwrap();
        reader.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            apply_subscription_event(&mut received, event);
        }
        if row_ids(&received.rows) == expected_rows {
            break;
        }
    }
    assert_eq!(
        row_ids(&received.rows),
        expected_rows,
        "a fresh reader must receive durable authority rows after the writer disconnects"
    );
}

#[test]
fn large_logical_snapshot_crosses_byte_peer_transport_and_settles() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0x71; 16]);
    let client_author = AuthorSubject::for_test_bytes([0x72; 16]);
    let server = open_core(0x73, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0x74, client_author, &schema);
    let expected = 900;

    for idx in 0..expected {
        seed(
            &server,
            "todos",
            cells(&format!("row-{idx}-{}", "x".repeat(4096)), false, owner),
        );
    }

    let (client_transport, server_transport) = byte_duplex_uncompressed();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    for _ in 0..200 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            let settled = event_settled(&event);
            let snapshot = snapshot_from_event(event);
            if settled {
                assert_eq!(snapshot.rows.len(), expected);
                return;
            }
        }
    }

    let rows = prepared_read(&client, &query);
    panic!(
        "large logical snapshot subscription did not settle; currently visible rows={}",
        rows.len()
    );
}

#[test]
fn branch_view_subscription_projects_base_resumes_and_unsubscribes_exact_view() {
    let schema = branch_sync_schema();
    let client_author = AuthorSubject::for_test_bytes([0x32; 16]);
    let server = open_core(0x33, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0x34, client_author, &schema);
    let base = branch_sync_selector(0x35);
    let sibling = branch_sync_selector(0x36);
    let head = branch_sync_selector(0x3b);
    let selected_row = RowUuid::from_bytes([0x37; 16]);
    let sibling_row = RowUuid::from_bytes([0x38; 16]);
    server
        .insert_with_id_in_branch(
            "todos",
            base.clone(),
            selected_row,
            BTreeMap::from([("title".to_owned(), Value::String("selected".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id_in_branch(
            "todos",
            sibling.clone(),
            sibling_row,
            BTreeMap::from([("title".to_owned(), Value::String("sibling".to_owned()))]),
        )
        .unwrap();

    let (client_transport, server_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let opts = global_subscribe_opts()
        .branch_view(head.clone(), Some(BranchViewBase::Current(base.clone())));
    let mut subscription = prepared_subscribe(&client, &query, opts.clone()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let mut snapshot = RelationSnapshot::default();
    for _ in 0..10 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
        if snapshot.rows.len() == 1 {
            break;
        }
    }
    assert_eq!(row_ids(&snapshot.rows), vec![selected_row]);
    assert_eq!(
        snapshot.rows[0].cell(&schema.tables[0], "branch_id"),
        Some(head.values["branch_id"].decode().unwrap()),
        "an inherited base row must project the requested head coordinate"
    );
    assert_eq!(
        row_ids(&prepared_all(&client, &query, opts.clone())),
        vec![selected_row],
        "a strict receiver-local relation snapshot must retain the requested head projection"
    );

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    assert!(server.server.detach_connection(&subscriber));
    assert!(client.detach_connection(&upstream));
    let (client_transport, server_transport) = duplex();
    let _resumed_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);
    for _ in 0..10 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
    }
    assert_eq!(row_ids(&snapshot.rows), vec![selected_row]);

    server
        .insert_with_id_in_branch(
            "todos",
            sibling,
            RowUuid::from_bytes([0x39; 16]),
            BTreeMap::from([("title".to_owned(), Value::String("hidden".to_owned()))]),
        )
        .unwrap();
    let added_after_resume = RowUuid::from_bytes([0x3a; 16]);
    server
        .insert_with_id_in_branch(
            "todos",
            base,
            added_after_resume,
            BTreeMap::from([("title".to_owned(), Value::String("visible".to_owned()))]),
        )
        .unwrap();
    for _ in 0..10 {
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
        if snapshot.rows.len() == 2 {
            break;
        }
    }
    assert_eq!(
        row_ids(&snapshot.rows),
        vec![selected_row, added_after_resume]
    );

    drop(subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let served = match &resumed.borrow().link {
        ConnectionLink::Subscriber(state) => state.served.len(),
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(served, 0, "unsubscribe must release the exact branch view");
}

#[test]
fn branch_view_subscriptions_disambiguate_same_row_and_tx_by_branch() {
    let schema = branch_sync_schema();
    let client_author = AuthorSubject::for_test_bytes([0x41; 16]);
    let server = open_core(0x42, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0x43, client_author, &schema);
    let left = branch_sync_selector(0x44);
    let right = branch_sync_selector(0x45);
    let row = RowUuid::from_bytes([0x46; 16]);
    server
        .insert_same_row_in_branches(
            "todos",
            row,
            [
                (
                    left.clone(),
                    BTreeMap::from([("title".to_owned(), Value::String("left".to_owned()))]),
                ),
                (
                    right.clone(),
                    BTreeMap::from([("title".to_owned(), Value::String("right".to_owned()))]),
                ),
            ],
        )
        .unwrap();
    let query = Query::from("todos");
    for (branch, title) in [(&left, "left"), (&right, "right")] {
        let read_view = crate::protocol::ReadViewSpec::branch_view(branch.clone(), None);
        let shape = query.validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let rows = server
            .node()
            .borrow_mut()
            .query_relation_snapshot_for_serving_in_read_view(
                &shape,
                &binding,
                DurabilityTier::Global,
                client_author,
                &read_view,
            )
            .unwrap()
            .rows;
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].cell(&schema.tables[0], "title"),
            Some(Value::String(title.to_owned()))
        );
    }

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let mut left_subscription = prepared_subscribe(
        &client,
        &query,
        global_subscribe_opts().branch_view(left.clone(), None),
    )
    .unwrap();
    let mut right_subscription = prepared_subscribe(
        &client,
        &query,
        global_subscribe_opts().branch_view(right.clone(), None),
    )
    .unwrap();
    assert!(opened_rows(block_on(left_subscription.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(right_subscription.next_raw()).unwrap()).is_empty());

    let mut left_snapshot = RelationSnapshot::default();
    let mut right_snapshot = RelationSnapshot::default();
    for _ in 0..20 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = left_subscription.try_next_event() {
            assert!(
                !matches!(
                    event,
                    SubscriptionEvent::Rejected { .. } | SubscriptionEvent::Closed
                ),
                "left branch subscription failed: {event:?}"
            );
            apply_subscription_event(&mut left_snapshot, event);
        }
        while let Some(event) = right_subscription.try_next_event() {
            assert!(
                !matches!(
                    event,
                    SubscriptionEvent::Rejected { .. } | SubscriptionEvent::Closed
                ),
                "right branch subscription failed: {event:?}"
            );
            apply_subscription_event(&mut right_snapshot, event);
        }
        if left_snapshot.rows.len() == 1 && right_snapshot.rows.len() == 1 {
            break;
        }
    }

    let served = match &subscriber.borrow().link {
        ConnectionLink::Subscriber(state) => state.served.len(),
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        served, 2,
        "both branch views must remain independently served"
    );
    let table = &schema.tables[0];
    assert_eq!(row_ids(&left_snapshot.rows), vec![row]);
    assert_eq!(row_ids(&right_snapshot.rows), vec![row]);
    assert_eq!(
        left_snapshot.rows[0].cell(table, "title"),
        Some(Value::String("left".to_owned()))
    );
    assert_eq!(
        right_snapshot.rows[0].cell(table, "title"),
        Some(Value::String("right".to_owned()))
    );
    assert_eq!(
        left_snapshot.rows[0].cell(table, "branch_id"),
        Some(left.values["branch_id"].decode().unwrap())
    );
    assert_eq!(
        right_snapshot.rows[0].cell(table, "branch_id"),
        Some(right.values["branch_id"].decode().unwrap())
    );
}

/// A default/current subscription emits a non-reset removal for a deletion witness.
///
/// alice deletes a row on the server; bob's default/current subscription receives
/// a delta removal and remains equivalent to alice's fresh current read.
///
/// alice (server) ──delete witness──► bob (default/current delta removal)
#[test]
fn default_current_subscription_reconciles_deletion_witness_without_reset() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0x50; 16]);
    let client_author = AuthorSubject::for_test_bytes([0x51; 16]);
    let server = open_core(0x52, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0x53, client_author, &schema);
    let current_row = RowUuid::from_bytes([0x54; 16]);
    server
        .insert_with_id("todos", current_row, cells("current", false, owner))
        .unwrap();
    let query = Query::from("todos");
    let current_view = ReadViewSpec::default();
    assert_eq!(
        row_ids(&serving_rows_in_read_view(
            &server,
            &schema,
            &query,
            client_author,
            &current_view,
        )),
        vec![current_row]
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let mut snapshot = RelationSnapshot::default();
    for _ in 0..10 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
        if row_ids(&snapshot.rows) == vec![current_row] {
            break;
        }
    }
    assert_eq!(row_ids(&snapshot.rows), vec![current_row]);

    write_deletion_register(&server, "todos", current_row, BranchSelector::default());
    let mut saw_removal = false;
    for _ in 0..10 {
        server.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            match &event {
                SubscriptionEvent::Delta {
                    reset,
                    added,
                    removed,
                    ..
                } => {
                    assert!(!reset, "deletion-witness reconcile must remain a delta");
                    assert!(
                        added.is_empty(),
                        "default/current deletion reconcile must not add rows"
                    );
                    saw_removal |= removed
                        .iter()
                        .any(|removed| removed.row_uuid == current_row);
                }
                SubscriptionEvent::Rejected { reason } => {
                    panic!("default/current subscription was rejected: {reason:?}")
                }
                SubscriptionEvent::Closed => panic!("default/current subscription closed"),
            }
            apply_subscription_event(&mut snapshot, event);
        }
        if saw_removal {
            break;
        }
    }
    assert!(
        saw_removal,
        "default/current reconcile must remove the deleted row"
    );
    let fresh = serving_rows_in_read_view(&server, &schema, &query, client_author, &current_view);
    assert_eq!(row_ids(&snapshot.rows), row_ids(&fresh));
}
