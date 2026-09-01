//! One-shot usage attachment, grouping, ownership, opening, and cleanup.

use super::*;

#[test]
fn one_shot_propagated_query_records_empty_remote_coverage() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);

    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    assert!(!client.query_attachment_is_covered(&attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&attachment));
    assert!(prepared_read(&client, &query).is_empty());
    client.detach_query(attachment);
}

#[test]
fn one_shot_edge_global_coverage_requires_current_authority_after_reconnect() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (first_client_transport, first_server_transport) = duplex();
    let first_upstream = crate::db::block_on(client.connect_upstream(first_client_transport));
    let _first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let attachment = client
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
    let _second_upstream = crate::db::block_on(client.connect_upstream(second_client_transport));
    let _second_subscriber = server.accept_subscriber(second_server_transport, client_author);
    assert!(
        !client.query_attachment_is_covered(&attachment),
        "reconnect must wait for the newly selected authority's response"
    );
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let (binding_view, required_after) = attachment.required_after[0];
    let authority_result_key = client
        .node
        .node
        .borrow()
        .authority_result_key_for_subscription(attachment.subscription())
        .unwrap();
    assert_eq!(authority_result_key.binding_view, binding_view);
    assert!(
        client
            .node
            .node
            .borrow()
            .applied_authority_result_generation(&authority_result_key)
            > required_after,
        "the reconnect response must advance the attachment generation"
    );
    let receipt_views = client
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
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    client.node.set_non_durable_client();
    let (client_transport, mut authority_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let attachment = client
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
    let subscription = loop {
        match authority_transport.try_recv().unwrap() {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    authority_transport
        .send(SyncMessage::ViewUpdate(
            crate::protocol::ViewUpdatePayload {
                subscription,
                settled_through: GlobalTime(1),
                reset_result_set: true,
                version_carriers: Vec::new(),
                peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
                result_member_adds: Vec::new(),
                result_member_removes: Vec::new(),
                program_fact_adds: Vec::new(),
                program_fact_removes: Vec::new(),
            },
        ))
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
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 2);
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
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("initial", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let baseline_receivers = server
        .node()
        .borrow()
        .runtime_stats_for_test()
        .active_subscriptions;

    let attachment = client
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

    let reopened = client
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
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("pinned", false, owner));
    let (client_transport, server_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let stream = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let borrowed_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let owned_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();

    client.detach_query(owned_attachment);
    let stream_two = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    drop(stream_two);

    drop(stream);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        client.query_attachment_is_covered(&borrowed_attachment),
        "dropping the stream must not strand its borrowing one-shot query"
    );
    assert_eq!(prepared_read(&client, &query).len(), 1);

    client.detach_query(borrowed_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    {
        let subscriber_ref = subscriber.borrow();
        let ConnectionLink::Subscriber(SubscriberConnectionState { served, .. }) =
            &subscriber_ref.link
        else {
            panic!("expected subscriber connection");
        };
        assert!(served.is_empty(), "final query detach must unsubscribe");
    }
    assert!(client.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(client.node.query_coverage_registrations.borrow().is_empty());

    let query_first = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let mut borrowing_stream =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(borrowing_stream.next_raw()).unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(borrowing_stream._state.borrow().settled);
    client.detach_query(query_first);
    assert!(client.detach_connection(&upstream));
    assert!(server.server.detach_connection(&subscriber));
    let (reconnected_client_transport, reconnected_server_transport) = duplex();
    let _reconnected_upstream =
        crate::db::block_on(client.connect_upstream(reconnected_client_transport));
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
    let subscriber_ref = reconnected_subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState { served, .. }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty(), "final stream drop must unsubscribe");
    assert!(client.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(client.node.query_coverage_registrations.borrow().is_empty());
}

#[test]
fn reconnect_replays_each_distinct_usage_subscription_key() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    let query = Query::from("todos");
    let prepared = prepared(&client, &query);

    let (first_client_transport, first_server_transport) = duplex();
    let first_upstream = crate::db::block_on(client.connect_upstream(first_client_transport));
    let first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let stream = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let borrowed = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let owned = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.detach_connection(&first_upstream));
    assert!(server.server.detach_connection(&first_subscriber));

    let (second_client_transport, second_server_transport) = duplex();
    let second_upstream = crate::db::block_on(client.connect_upstream(second_client_transport));
    let second_subscriber = server.accept_subscriber(second_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let served_len = match &second_subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState { served, .. }) => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 2, "reconnect must replay S/q1 and distinct q2");

    client.detach_query(owned);
    client.tick().unwrap();
    server.tick().unwrap();
    let served_len = match &second_subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState { served, .. }) => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 1);
    drop(stream);
    client.detach_query(borrowed);
    client.tick().unwrap();
    server.tick().unwrap();
    let served_len = match &second_subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState { served, .. }) => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 0);
    assert!(client.detach_connection(&second_upstream));
    assert!(server.server.detach_connection(&second_subscriber));

    let first_query = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let second_query = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let (third_client_transport, third_server_transport) = duplex();
    let _third_upstream = crate::db::block_on(client.connect_upstream(third_client_transport));
    let third_subscriber = server.accept_subscriber(third_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.detach_query(second_query);
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_query));
    server.tick().unwrap();
    client.detach_query(first_query);
    client.tick().unwrap();
    server.tick().unwrap();
    let served_len = match &third_subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState { served, .. }) => served.len(),
        _ => panic!("expected subscriber connection"),
    };
    assert_eq!(served_len, 0);
    assert!(client.node.upstream_coverage_refcounts.borrow().is_empty());
    assert!(client.node.query_coverage_registrations.borrow().is_empty());
}

#[test]
fn subscriber_connection_groups_duplicate_usage_subscriptions_by_coverage_key() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        peer,
        served,
        coverage_groups,
        ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 2);
    assert_eq!(coverage_groups.len(), 1);
    let group = coverage_groups
        .values()
        .next()
        .expect("duplicate usage subscriptions should share one coverage group");
    assert_eq!(group.subscribers.len(), 2);
    let maintained_metrics = peer.maintained_subscription_view_metrics();
    assert_eq!(maintained_metrics.hits_out, 2);
    assert_eq!(maintained_metrics.footprint.result_rows, 1);
    assert_eq!(prepared_read(&client, &query).len(), 1);
    drop(subscriber_ref);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
    client.tick().unwrap();
    server.tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        served,
        coverage_groups,
        ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty());
    assert!(coverage_groups.is_empty());
}

#[test]
fn subscription_opening_publication_follows_upstream_coverage_lifecycle() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    let seeded = seed(&server, "todos", cells("first", false, owner));
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");

    let mut first = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    // NAPI drains subscriptions through try_next_event, so protect that exact
    // host path before any authority response exists.
    assert!(
        first.try_next_event().is_none(),
        "new remote coverage must not publish its provisional local opening"
    );
    let mut duplicate_before_authority =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(
        duplicate_before_authority.try_next_event().is_none(),
        "shared coverage must remain provisional until its first authority response"
    );

    // WASM erases SubscriptionStream behind dyn Stream, so separately protect
    // the poll_next path with a different newly-created coverage key.
    let empty_query = Query::from("todos").filter(eq(col("done"), lit(true)));
    let mut wasm_path = prepared_subscribe(&client, &empty_query, global_subscribe_opts()).unwrap();
    let waker = Waker::noop();
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

    let mut duplicate = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert_eq!(
        row_ids(&opened_rows(duplicate.try_next_event().expect(
            "coverage-sharing subscription must publish its current local snapshot immediately"
        ))),
        vec![seeded]
    );

    let mut local = prepared_subscribe(
        &client,
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
    let schema = schema();
    let client = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let query = Query::from("todos");

    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    let event = subscription
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
    let schema = schema();
    let client = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let (client_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let query = Query::from("todos");
    let mut first = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    client.tick().unwrap();
    let subscription = loop {
        match authority_transport.try_recv().unwrap() {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    let update = |version_bundles| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(1),
            reset_result_set: true,
            version_carriers: crate::protocol::build_version_carriers_from_singletons(
                version_bundles,
            )
            .expect("test bundles form valid carriers"),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
    };
    authority_transport
        .send(update(vec![crate::protocol::VersionBundle {
            scope: crate::protocol::VersionBundleScope::CompleteTransaction,
            tx: crate::tx::Transaction {
                tx_id: TxId::new(TxTime::from(44), NodeUuid::from_bytes([0x44; 16])),
                kind: crate::tx::TxKind::Mergeable,
                n_total_writes: 0,
                made_by: AuthorSubject::SYSTEM,
                permission_subject: None,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
                contribution_merge: None,
            },
            versions: Vec::new(),
            fate: crate::tx::Fate::Accepted,
            global_time: Some(GlobalTime(44)),
            durability: DurabilityTier::Edge,
        }]))
        .unwrap();
    assert!(
        client.tick().is_err(),
        "missing payload must reject the update"
    );

    let mut duplicate = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(
        duplicate.try_next_event().is_none(),
        "a rejected authority opening must not make shared coverage publishable"
    );

    authority_transport.send(update(Vec::new())).unwrap();
    client.tick().unwrap();
    assert!(opened_rows(block_on(first.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(duplicate.next_raw()).unwrap()).is_empty());
    let mut after_success = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
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
fn dropping_live_subscriptions_detaches_usage_subscriptions() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let seeded = seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut first_subscription =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let mut second_subscription =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
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

    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        served,
        coverage_groups,
        ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 1);
    assert_eq!(coverage_groups.len(), 1);
    let group = coverage_groups
        .values()
        .next()
        .expect("propagating subscriptions should share one forwarded coverage group");
    assert_eq!(group.subscribers.len(), 1);
    drop(subscriber_ref);

    drop(first_subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        served,
        coverage_groups,
        ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(served.len(), 1);
    assert_eq!(coverage_groups.len(), 1);
    drop(subscriber_ref);

    drop(second_subscription);
    client.tick().unwrap();
    server.tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        served,
        coverage_groups,
        ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert!(served.is_empty());
    assert!(coverage_groups.is_empty());
}

#[test]
fn one_shot_edge_query_attaches_fresh_usage_subscription_for_covered_binding() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 1);

    seed(&server, "todos", cells("second", false, owner));
    let second_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(prepared_read(&client, &query).len(), 2);
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn missing_permissions_head_gates_sessions_but_not_trusted_backend_query_coverage() {
    // This stays at the transport boundary because the behavior under test is
    // the authenticated link's trust discriminator, which the public query API
    // deliberately does not expose.
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    server.server.set_permissions_ready(false).unwrap();

    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let backend = open_db(0xb0, backend_author, &schema);
    let (backend_transport, server_backend_transport) = duplex();
    let _backend_upstream = crate::db::block_on(backend.connect_upstream(backend_transport));
    let _backend_subscriber = server.accept_subscriber_with_trust(
        server_backend_transport,
        backend_author,
        CommitUnitTrust::TrustedBackend,
    );

    let session_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let session = open_db(0xc1, session_author, &schema);
    let (session_transport, server_session_transport) = duplex();
    let _session_upstream = crate::db::block_on(session.connect_upstream(session_transport));
    let _session_subscriber = server.accept_subscriber(server_session_transport, session_author);

    let backend_query = prepared(&backend, &Query::from("todos"));
    let backend_attachment = backend
        .attach_query_with_opts(&backend_query, edge_subscribe_opts())
        .unwrap();
    let session_query = prepared(&session, &Query::from("todos"));
    let session_attachment = session
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
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("chats")
                .column("title", PublicColumnType::Text)
                .nullable_column("joinCode", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("joinCode", &["claims", "join_code"])),
                ),
        ),
    );
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let reader = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, reader, &schema);
    let join_code = "invite-code-123";
    client.set_identity_claims(
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let first = seed(
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
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        reader,
        BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]),
    );

    let query = Query::from("chats");
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first]
    );

    let second = seed(
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
    let second_attachment = client
        .attach_query_with_opts(&prepared, edge_subscribe_opts())
        .unwrap();
    assert!(client.query_attachment_is_covered(&first_attachment));
    assert!(!client.query_attachment_is_covered(&second_attachment));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&second_attachment));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, edge_subscribe_opts())),
        vec![first, second]
    );
    client.detach_query(first_attachment);
    client.detach_query(second_attachment);
}

#[test]
fn edge_subscription_with_claim_bound_policy_emits_later_matching_server_write() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("chats")
                .column("title", PublicColumnType::Text)
                .nullable_column("joinCode", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("joinCode", &["claims", "join_code"])),
                ),
        ),
    );
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let reader = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, reader, &schema);
    let join_code = "invite-code-123";
    let claims = BTreeMap::from([("join_code".to_owned(), Value::String(join_code.to_owned()))]);
    client.set_identity_claims(reader, claims.clone());

    let _first = seed(
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
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(server_transport, reader, claims);

    let query = Query::from("chats");
    let mut subscription = prepared_subscribe(&client, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let SubscriptionEvent::Delta { added, .. } = block_on(subscription.next_raw()).unwrap() else {
        panic!("expected subscription delta after upstream coverage");
    };
    assert_eq!(added.len(), 1);
}
