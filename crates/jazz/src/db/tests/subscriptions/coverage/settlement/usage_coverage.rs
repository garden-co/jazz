//! One-shot usage attachment, grouping, ownership, opening, and cleanup.

use super::*;

#[test]
fn one_shot_propagated_query_records_empty_remote_coverage() {
    let mut schema = schema();
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);

    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);

    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);

    let mut attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    assert!(!client.query_attachment_is_covered(&attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&attachment));
    assert!(prepared_read(&mut client, &query).is_empty());
    client.detach_query(attachment);
}

#[test]
fn one_shot_edge_global_coverage_requires_current_authority_after_reconnect() {
    let mut schema = schema();
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    let (first_client_transport, first_server_transport) = duplex();
    let mut first_upstream = client.connect_upstream(first_client_transport);
    let mut _first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&attachment));

    assert!(client.detach_connection(&first_upstream));
    assert!(
        !client.query_attachment_is_covered(&attachment),
        "disconnect must invalidate an Edge/Global one-shot coverage witness"
    );

    let (second_client_transport, second_server_transport) = duplex();
    let mut _second_upstream = client.connect_upstream(second_client_transport);
    let mut _second_subscriber = server.accept_subscriber(second_server_transport, client_author);
    assert!(
        !client.query_attachment_is_covered(&attachment),
        "reconnect must wait for the newly selected authority's response"
    );
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (binding_view, required_after) = attachment.required_after[0];
    assert!(
        client
            .node
            .node
            .borrow()
            .applied_view_update_generation(binding_view)
            > required_after,
        "the reconnect response must advance the attachment generation"
    );
    let mut receipt_views = client
        .node
        .active_authority_view_receipts
        .borrow()
        .as_ref()
        .map(|receipts| receipts.binding_views.clone())
        .unwrap_or_default();
    assert!(
        receipt_views.contains(&binding_view),
        "the reconnect response must establish the selected authority receipt: expected {binding_view:?}, got {receipt_views:?}"
    );
    assert!(client.query_attachment_is_covered(&attachment));
    client.detach_query(attachment);
}

#[test]
fn one_shot_local_coverage_does_not_require_authority_continuity() {
    let mut schema = schema();
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    client.node.set_non_durable_client();
    client
        .node
        .set_upstream_durability_floor(DurabilityTier::Local);
    let (client_transport, mut authority_transport) = duplex();
    let mut upstream = client.connect_upstream(client_transport);
    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut attachment = client
        .attach_query_with_opts(
            &prepared,
            ReadOpts {
                tier: DurabilityTier::Local,
                propagation: Propagation::LocalOnly,
                ..ReadOpts::default()
            },
        )
        .unwrap();
    client.tick().unwrap();
    let mut subscription = loop {
        match authority_transport.try_recv().unwrap() {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    authority_transport
        .send(SyncMessage::ViewUpdate {
            subscription,
            settled_through: GlobalSeq(1),
            reset_result_set: true,
            version_carriers: Vec::new(),
            version_bundles: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
        .unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&attachment));

    assert!(client.detach_connection(&upstream));
    assert!(
        client.query_attachment_is_covered(&attachment),
        "Local coverage remains process-local and does not depend on authority continuity"
    );
    client.detach_query(attachment);
}

#[test]
fn one_shot_propagated_query_attaches_fresh_usage_subscription_for_covered_binding() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);

    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);

    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(prepared_read(&mut client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    let mut second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(prepared_read(&mut client, &query).len(), 2);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

/// Ensures a dormant propagated query coverage group releases its maintained
/// server runtime receiver before the same logical query opens again.
///
/// This is intentionally an internal lifecycle test: the public query API can
/// observe the later re-open, but cannot expose the authority's receiver
/// count. The count is the exact ownership boundary that prevents a dropped
/// one-shot handle from retaining stale maintained source state.
///
/// ```text
/// client ──open coverage──► server maintained receiver
/// client ──drop last handle► server receiver removed
/// client ──re-open────────► exactly one fresh receiver
/// ```
#[test]
fn final_query_coverage_drop_releases_server_maintained_receiver_before_reopen() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("initial", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);
    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut baseline_receivers = server
        .node()
        .borrow()
        .runtime_stats_for_test()
        .active_subscriptions;

    let mut attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .expect("attach propagated one-shot coverage");
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&attachment));
    assert_eq!(
        server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        baseline_receivers + 1,
        "the server owns one maintained receiver while coverage is live"
    );

    client.detach_query(attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        baseline_receivers,
        "dropping the final coverage handle must unregister its server receiver"
    );

    let mut reopened = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .expect("re-open propagated one-shot coverage");
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&reopened));
    assert_eq!(
        server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        baseline_receivers + 1,
        "re-open installs exactly one fresh maintained receiver"
    );
    client.detach_query(reopened);
}

#[test]
fn one_shot_borrowed_stream_coverage_stays_pinned_until_query_detach() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("pinned", false, owner));
    let (client_transport, server_transport) = duplex();
    let mut upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, client_author);
    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut stream = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let mut borrowed_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let mut owned_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();

    client.detach_query(owned_attachment);
    let mut stream_two = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    drop(stream_two);

    drop(stream);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        client.query_attachment_is_covered(&borrowed_attachment),
        "dropping the stream must not strand its borrowing one-shot query"
    );
    assert_eq!(prepared_read(&mut client, &query).len(), 1);

    client.detach_query(borrowed_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    {
        let mut subscriber_ref = subscriber.borrow();
        let ConnectionLink::Subscriber { served, .. } = &subscriber_ref.link else {
            panic!("expected subscriber connection");
        };
        assert!(served.is_empty(), "final query detach must unsubscribe");
    }
    assert!(client.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(client.node.query_coverage_registrations.borrow().is_empty());

    let mut query_first = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let mut borrowing_stream =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(borrowing_stream.next_raw()).unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(borrowing_stream._state.borrow().settled);
    client.detach_query(query_first);
    assert!(client.detach_connection(&upstream));
    assert!(server.server.detach_connection(&subscriber));
    let (reconnected_client_transport, reconnected_server_transport) = duplex();
    let mut _reconnected_upstream = client.connect_upstream(reconnected_client_transport);
    let reconnected_subscriber =
        server.accept_subscriber(reconnected_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        borrowing_stream._state.borrow().settled,
        "detaching the owning query must not unsubscribe its live borrowing stream"
    );
    drop(borrowing_stream);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut subscriber_ref = reconnected_subscriber.borrow();
    let ConnectionLink::Subscriber { served, .. } = &subscriber_ref.link else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty(), "final stream drop must unsubscribe");
    assert!(client.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(client.node.query_coverage_registrations.borrow().is_empty());
}

#[test]
fn reconnect_replays_each_distinct_usage_subscription_key() {
    let mut schema = schema();
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);

    let (first_client_transport, first_server_transport) = duplex();
    let mut first_upstream = client.connect_upstream(first_client_transport);
    let mut first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let mut stream = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let mut borrowed = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let mut owned = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.detach_connection(&first_upstream));
    assert!(server.server.detach_connection(&first_subscriber));

    let (second_client_transport, second_server_transport) = duplex();
    let mut second_upstream = client.connect_upstream(second_client_transport);
    let mut second_subscriber = server.accept_subscriber(second_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let mut served_len = match &second_subscriber.borrow().link {
        ConnectionLink::Subscriber { served, .. } => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 2, "reconnect must replay S/q1 and distinct q2");

    client.detach_query(owned);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut served_len = match &second_subscriber.borrow().link {
        ConnectionLink::Subscriber { served, .. } => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 1);
    drop(stream);
    client.detach_query(borrowed);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut served_len = match &second_subscriber.borrow().link {
        ConnectionLink::Subscriber { served, .. } => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 0);
    assert!(client.detach_connection(&second_upstream));
    assert!(server.server.detach_connection(&second_subscriber));

    let mut first_query = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let mut second_query = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let (third_client_transport, third_server_transport) = duplex();
    let mut _third_upstream = client.connect_upstream(third_client_transport);
    let mut third_subscriber = server.accept_subscriber(third_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.detach_query(second_query);
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_query));
    server.tick().unwrap();
    client.detach_query(first_query);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut served_len = match &third_subscriber.borrow().link {
        ConnectionLink::Subscriber { served, .. } => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 0);
    assert!(client.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(client.node.query_coverage_registrations.borrow().is_empty());
}

#[test]
fn subscriber_connection_groups_duplicate_usage_subscriptions_by_coverage_key() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);

    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, client_author);

    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let mut second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let mut subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        peer,
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 2);
    assert_eq!(coverage_groups.len(), 1);
    let mut group = coverage_groups
        .values()
        .next()
        .expect("duplicate usage subscriptions should share one coverage group");
    assert_eq!(group.subscribers.len(), 2);
    let mut maintained_metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(maintained_metrics.hits_out, 2);
    assert_eq!(maintained_metrics.footprint.result_rows, 1);
    assert_eq!(prepared_read(&mut client, &query).len(), 1);
    drop(subscriber_ref);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty());
    assert!(coverage_groups.is_empty());
}

#[test]
fn subscription_opening_publication_follows_upstream_coverage_lifecycle() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    let mut seeded = seed(&server, "todos", cells("first", false, owner));
    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);
    let mut query = Query::from("todos");

    let mut first = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    // NAPI drains subscriptions through try_next_event, so protect that exact
    // host path before any authority response exists.
    assert!(
        first.try_next_event().is_none(),
        "new remote coverage must not publish its provisional local opening"
    );
    let mut duplicate_before_authority =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(
        duplicate_before_authority.try_next_event().is_none(),
        "shared coverage must remain provisional until its first authority response"
    );

    // WASM erases SubscriptionStream behind dyn Stream, so separately protect
    // the poll_next path with a different newly-created coverage key.
    let mut empty_query = Query::from("todos").filter(eq(col("done"), lit(true)));
    let mut wasm_path =
        prepared_subscribe(&mut client, &empty_query, global_subscribe_opts()).unwrap();
    let mut waker = Waker::noop();
    let mut cx = Context::from_waker(waker);
    assert!(matches!(
        Pin::new(&mut wasm_path).poll_next(&mut cx),
        Poll::Pending
    ));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(first.next_raw()).unwrap())),
        vec![seeded]
    );
    assert_eq!(
        row_ids(&opened_rows(
            block_on(duplicate_before_authority.next_raw()).unwrap()
        )),
        vec![seeded],
        "the first authority reset must reach every owner of shared coverage"
    );
    assert!(
        opened_rows(match Pin::new(&mut wasm_path).poll_next(&mut cx) {
            Poll::Ready(Some(event)) => event,
            other => panic!("authority must publish the settled empty opening: {other:?}"),
        })
        .is_empty()
    );

    let mut duplicate = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert_eq!(
        row_ids(&opened_rows(duplicate.try_next_event().expect(
            "coverage-sharing subscription must publish its current local snapshot immediately"
        ))),
        vec![seeded]
    );

    let mut local = prepared_subscribe(
        &mut client,
        &query,
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    )
    .unwrap();
    assert_eq!(
        row_ids(&opened_rows(local.try_next_event().expect(
            "LocalOnly subscription must always publish its local opening immediately"
        ))),
        vec![seeded]
    );
}

#[test]
fn local_tier_full_propagation_publishes_truthful_empty_opening() {
    let mut schema = schema();
    let mut client = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let mut query = Query::from("todos");

    let mut subscription = prepared_subscribe(&mut client, &query, ReadOpts::default()).unwrap();
    let mut event = subscription
        .try_next_event()
        .expect("Local tier must publish its local opening while propagation continues upstream");
    let SubscriptionEvent::Delta {
        reset,
        publishable,
        tier,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("Local tier opening must be a delta");
    };
    assert!(reset);
    assert!(publishable);
    assert_eq!(tier, DurabilityTier::Local);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn malformed_authority_opening_keeps_shared_coverage_provisional() {
    let mut schema = schema();
    let mut client = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let (client_transport, mut authority_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut query = Query::from("todos");
    let mut first = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    client.tick().unwrap();
    let mut subscription = loop {
        match authority_transport.try_recv().unwrap() {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    let mut update = |version_bundles| SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(1),
        reset_result_set: true,
        version_carriers: Vec::new(),
        version_bundles,
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    authority_transport
        .send(update(vec![crate::protocol::VersionBundle {
            tx: crate::tx::Transaction {
                tx_id: TxId::new(TxTime::from(44), NodeUuid::from_bytes([0x44; 16])),
                kind: crate::tx::TxKind::Mergeable,
                n_total_writes: 0,
                made_by: AuthorId::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                target_lineage: crate::tx::BranchLineage::Root,
                branch_merge: None,
            },
            versions: Vec::new(),
            fate: crate::tx::Fate::Accepted,
            global_seq: Some(GlobalSeq(44)),
            durability: DurabilityTier::Edge,
        }]))
        .unwrap();
    assert!(
        client.tick().is_err(),
        "missing payload must reject the update"
    );

    let mut duplicate = prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(
        duplicate.try_next_event().is_none(),
        "a rejected authority opening must not make shared coverage publishable"
    );

    authority_transport.send(update(Vec::new())).unwrap();
    client.tick().unwrap();
    assert!(opened_rows(block_on(first.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(duplicate.next_raw()).unwrap()).is_empty());
    let mut after_success =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(
        opened_rows(
            after_success
                .try_next_event()
                .expect("valid clear publishes coverage")
        )
        .is_empty()
    );
}

#[test]
fn authoritative_empty_branch_opening_does_not_wait_for_metadata() {
    let mut schema = schema();
    let mut client = open_db(0xc1, AuthorId::from_bytes([0xc1; 16]), &schema);
    let (client_transport, mut authority_transport) = duplex();
    let mut upstream = client.connect_upstream(client_transport);
    let mut parked_query = Query::from("todos");
    let mut valid_query = Query::from("todos").filter(eq(col("done"), lit(true)));
    let mut branch = BranchId::from_bytes([0x42; 16]);
    let mut branch_stream =
        prepared_subscribe(&mut client, &parked_query, global_subscribe_opts()).unwrap();
    let mut global_stream =
        prepared_subscribe(&mut client, &valid_query, global_subscribe_opts()).unwrap();
    client.tick().unwrap();
    let mut subscriptions = Vec::new();
    while let Some(message) = authority_transport.try_recv() {
        if let SyncMessage::Subscribe(subscribe) = message {
            subscriptions.push(subscribe.subscription);
        }
    }
    assert_eq!(subscriptions.len(), 2);
    let mut branch_subscription = subscriptions[0];
    let mut global_subscription = subscriptions[1];
    {
        let mut upstream = upstream.borrow_mut();
        let ConnectionLink::Upstream { branch_views, .. } = &mut upstream.link else {
            unreachable!()
        };
        branch_views.insert(branch_subscription, branch);
    }
    let mut empty_update = |subscription| SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(1),
        reset_result_set: true,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    authority_transport
        .send(empty_update(branch_subscription))
        .unwrap();
    authority_transport
        .send(empty_update(global_subscription))
        .unwrap();
    client.tick().unwrap();
    assert!(opened_rows(block_on(global_stream.next_raw()).unwrap()).is_empty());
    let mut branch_duplicate =
        prepared_subscribe(&mut client, &parked_query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(branch_stream.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(branch_duplicate.next_raw()).unwrap()).is_empty());

    authority_transport
        .send(SyncMessage::BranchMetadata(BranchMetadata {
            branch_id: branch,
            created_by: AuthorId::SYSTEM,
            parent: None,
            base: None,
            open: true,
        }))
        .unwrap();
    client.tick().unwrap();
    let mut after_repair =
        prepared_subscribe(&mut client, &parked_query, global_subscribe_opts()).unwrap();
    assert!(
        opened_rows(
            after_repair
                .try_next_event()
                .expect("branch repair clears coverage")
        )
        .is_empty()
    );
}

#[test]
fn dropping_live_subscriptions_detaches_usage_subscriptions() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);

    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    let mut seeded = seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, client_author);

    let mut query = Query::from("todos");
    let mut first_subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    let mut second_subscription =
        prepared_subscribe(&mut client, &query, global_subscribe_opts()).unwrap();
    assert!(first_subscription.try_next_event().is_none());
    assert!(opened_rows(block_on(second_subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        row_ids(&opened_rows(
            block_on(first_subscription.next_raw()).unwrap()
        )),
        vec![seeded]
    );

    let mut subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 1);
    assert_eq!(coverage_groups.len(), 1);
    let mut group = coverage_groups
        .values()
        .next()
        .expect("propagating subscriptions should share one forwarded coverage group");
    assert_eq!(group.subscribers.len(), 1);
    drop(subscriber_ref);

    drop(first_subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 1);
    assert_eq!(coverage_groups.len(), 1);
    drop(subscriber_ref);

    drop(second_subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let mut subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber {
        served,
        coverage_groups,
        ..
    } = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty());
    assert!(coverage_groups.is_empty());
}

#[test]
fn one_shot_edge_query_attaches_fresh_usage_subscription_for_covered_binding() {
    let mut schema = schema();
    let mut owner = AuthorId::from_bytes([0xa1; 16]);
    let mut client_author = AuthorId::from_bytes([0xc1; 16]);

    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);

    let mut query = Query::from("todos");
    let mut prepared = prepared(&mut client, &query);
    let mut first_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(prepared_read(&mut client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    let mut second_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(prepared_read(&mut client, &query).len(), 2);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn missing_permissions_head_gates_sessions_but_not_trusted_backend_query_coverage() {
    // This stays at the transport boundary because the behavior under test is
    // the authenticated link's trust discriminator, which the public query API
    // deliberately does not expose.
    let mut schema = schema();
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    server.server.set_permissions_ready(false).unwrap();

    let mut backend_author = AuthorId::from_bytes([0xb0; 16]);
    let mut backend = open_db(0xb0, backend_author, &schema);
    let (backend_transport, server_backend_transport) = duplex();
    let mut _backend_upstream = backend.connect_upstream(backend_transport);
    let mut _backend_subscriber = server.accept_subscriber_with_trust(
        server_backend_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let mut session_author = AuthorId::from_bytes([0xc1; 16]);
    let mut session = open_db(0xc1, session_author, &schema);
    let (session_transport, server_session_transport) = duplex();
    let mut _session_upstream = session.connect_upstream(session_transport);
    let mut _session_subscriber =
        server.accept_subscriber(server_session_transport, session_author);

    let mut backend_query = prepared(&mut backend, &Query::from("todos"));
    let mut backend_attachment = backend
        .attach_query_with_opts(&backend_query, edge_subscribe_opts())
        .unwrap();
    let mut session_query = prepared(&mut session, &Query::from("todos"));
    let mut session_attachment = session
        .attach_query_with_opts(&session_query, edge_subscribe_opts())
        .unwrap();

    backend.tick().unwrap();
    session.tick().unwrap();
    server.tick().unwrap();
    backend.tick().unwrap();
    session.tick().unwrap();

    assert!(backend.query_attachment_is_covered(&backend_attachment));
    assert!(!session.query_attachment_is_covered(&session_attachment));

    server.server.set_permissions_ready(true).unwrap();
    server.tick().unwrap();
    session.tick().unwrap();
    assert!(session.query_attachment_is_covered(&session_attachment));
}

#[test]
fn one_shot_edge_query_attaches_fresh_claim_bound_usage_subscription_for_covered_binding() {
    let mut schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats").filter(any_of([])).policy_branch(
            crate::query::PolicyBranch::single_alternative_from_query(
                Query::from("chats").filter(eq(col("joinCode"), crate::query::claim("join_code"))),
            ),
        ),
    ))
    .with_write_policy(Policy::public())]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut reader = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, reader, &schema);
    let mut join_code = "invite-code-123";
    client.set_identity_claims(
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let mut first = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let mut query = Query::from("chats");
    let mut prepared = prepared(&mut client, &query);
    let mut first_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(
        row_ids(&prepared_all(&mut client, &query, edge_subscribe_opts())),
        vec![first]
    );

    let mut second = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );
    let mut second_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(
        row_ids(&prepared_all(&mut client, &query, edge_subscribe_opts())),
        vec![first, second]
    );
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn edge_subscription_with_claim_bound_policy_emits_later_matching_server_write() {
    let mut schema = JazzSchema::new([TableSchema::new(
        "chats",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("joinCode", ColumnType::String.nullable()),
        ],
    )
    .with_read_policy(Policy::shape(
        Query::from("chats").filter(any_of([])).policy_branch(
            crate::query::PolicyBranch::single_alternative_from_query(
                Query::from("chats").filter(eq(col("joinCode"), crate::query::claim("join_code"))),
            ),
        ),
    ))
    .with_write_policy(Policy::public())]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut reader = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, reader, &schema);
    let mut join_code = "invite-code-123";
    let mut claims =
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]);
    client.set_identity_claims(reader, claims.clone());

    let mut _first = seed(
        &server,
        "chats",
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            (
                "joinCode".to_owned(),
                Value::Nullable(Some(Box::new(Value::String(join_code.to_owned())))),
            ),
        ]),
    );

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber_with_claims(server_transport, reader, claims);

    let mut query = Query::from("chats");
    let mut subscription = prepared_subscribe(&mut client, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected subscription delta after upstream coverage");
    };
    assert_eq!(added.len(), 1);
}
