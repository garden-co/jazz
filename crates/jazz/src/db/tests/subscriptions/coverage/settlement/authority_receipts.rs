//! Authority selection, receipt freshness, fallback cuts, and reconnect continuity.

use super::*;

#[test]
fn subscription_emits_when_remote_coverage_settles_without_row_changes() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

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

    let settled = block_on(subscription.next_raw()).unwrap();
    assert!(event_settled(&settled));
    let (added, updated, removed) = delta_rows(settled);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn edge_global_settlement_requires_a_fresh_current_connection_view_receipt() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("cached", false, owner));

    let (first_client_transport, first_server_transport) = duplex();
    let first_upstream = crate::db::block_on(client.connect_upstream(first_client_transport));
    let _first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(!event_settled(&block_on(subscription.next_raw()).unwrap()));

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(event_settled(&block_on(subscription.next_raw()).unwrap()));

    assert!(client.detach_connection(&first_upstream));
    let disconnected = block_on(subscription.next_raw()).unwrap();
    assert!(
        !event_settled(&disconnected),
        "disconnect must immediately demote cached Edge/Global rows to unsettled"
    );
    assert_eq!(
        prepared_read(&client, &query).len(),
        1,
        "disconnect keeps the durable cached row as local data"
    );

    let (reconnected_client_transport, reconnected_server_transport) = duplex();
    let _reconnected_upstream =
        crate::db::block_on(client.connect_upstream(reconnected_client_transport));
    let _reconnected_subscriber =
        server.accept_subscriber(reconnected_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        event_settled(&block_on(subscription.next_raw()).unwrap()),
        "only a fresh view from the current upstream epoch may re-settle the cache"
    );
}

#[test]
fn nonselected_upstream_update_demotes_selected_receipt_before_publication() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("initial", false, owner));

    let (old_client_transport, old_server_transport) = duplex();
    let old_upstream = crate::db::block_on(client.connect_upstream(old_client_transport));
    let _old_subscriber = server.accept_subscriber(old_server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(event_settled(&block_on(subscription.next_raw()).unwrap()));

    let (new_client_transport, new_server_transport) = duplex();
    let new_upstream = crate::db::block_on(client.connect_upstream(new_client_transport));
    let _new_subscriber = server.accept_subscriber(new_server_transport, client_author);
    assert!(!event_settled(&block_on(subscription.next_raw()).unwrap()));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(event_settled(&block_on(subscription.next_raw()).unwrap()));

    seed(
        &server,
        "todos",
        cells("nonselected A update", false, owner),
    );
    server.tick().unwrap();
    old_upstream.borrow_mut().tick().unwrap();
    assert!(
        !event_settled(&block_on(subscription.next_raw()).unwrap()),
        "A's row-changing update must retire B's receipt before publication"
    );

    new_upstream.borrow_mut().tick().unwrap();
    assert!(
        event_settled(&block_on(subscription.next_raw()).unwrap()),
        "B's own queued response may re-establish the selected receipt"
    );
}

#[test]
fn nonselected_view_update_demotes_receipts_for_other_recomputed_views() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let all_query = Query::from("todos");
    let filtered_query = Query::from("todos").filter(eq(col("title"), lit("matching")));
    let view_update = |subscription, settled_through| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through,
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
    };

    let (old_client_transport, mut old_authority) = duplex();
    let _old_upstream = crate::db::block_on(client.connect_upstream(old_client_transport));
    let mut all_subscription =
        prepared_subscribe(&client, &all_query, global_subscribe_opts()).unwrap();
    let mut filtered_subscription =
        prepared_subscribe(&client, &filtered_query, global_subscribe_opts()).unwrap();
    let _ = block_on(all_subscription.next_raw()).unwrap();
    let _ = block_on(filtered_subscription.next_raw()).unwrap();
    client.tick().unwrap();
    let mut old_keys = BTreeMap::new();
    while old_keys.len() < 2 {
        if let SyncMessage::Subscribe(subscribe) = old_authority.try_recv().unwrap() {
            old_keys.insert(subscribe.subscription.shape_id, subscribe.subscription);
        }
    }
    for subscription in old_keys.values().copied() {
        old_authority
            .send(view_update(subscription, GlobalTime(1)))
            .unwrap();
    }
    client.tick().unwrap();
    assert!(all_subscription._state.borrow().settled);
    assert!(filtered_subscription._state.borrow().settled);

    let (new_client_transport, mut new_authority) = duplex();
    let _new_upstream = crate::db::block_on(client.connect_upstream(new_client_transport));
    assert!(!all_subscription._state.borrow().settled);
    assert!(!filtered_subscription._state.borrow().settled);
    client.tick().unwrap();
    let mut new_keys = BTreeMap::new();
    while new_keys.len() < 2 {
        if let SyncMessage::Subscribe(subscribe) = new_authority.try_recv().unwrap() {
            new_keys.insert(subscribe.subscription.shape_id, subscribe.subscription);
        }
    }
    for subscription in new_keys.values().copied() {
        new_authority
            .send(view_update(subscription, GlobalTime(2)))
            .unwrap();
    }
    client.tick().unwrap();
    assert!(all_subscription._state.borrow().settled);
    assert!(filtered_subscription._state.borrow().settled);

    let all_key = old_keys[&all_query.validate(&schema).unwrap().shape_id()];
    old_authority
        .send(view_update(all_key, GlobalTime(3)))
        .unwrap();
    client.tick().unwrap();
    assert!(!all_subscription._state.borrow().settled);
    assert!(
        !filtered_subscription._state.borrow().settled,
        "an X update may recompute filtered Y, so no selected receipt survives"
    );

    for subscription in new_keys.values().copied() {
        new_authority
            .send(view_update(subscription, GlobalTime(2)))
            .unwrap();
    }
    client.tick().unwrap();
    assert!(!all_subscription._state.borrow().settled);
    assert!(
        !filtered_subscription._state.borrow().settled,
        "B@2 cannot re-receipt state after nonselected A@3 was applied"
    );
    for subscription in new_keys.values().copied() {
        new_authority
            .send(view_update(subscription, GlobalTime(3)))
            .unwrap();
    }
    client.tick().unwrap();
    assert!(all_subscription._state.borrow().settled);
    assert!(filtered_subscription._state.borrow().settled);
}

#[test]
fn stale_old_upstream_epoch_cannot_settle_after_edge_switch_or_fallback() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(
        &server,
        "todos",
        cells("served by either edge", false, owner),
    );

    let (old_client_transport, old_server_transport) = duplex();
    let old_upstream = crate::db::block_on(client.connect_upstream(old_client_transport));
    let _old_subscriber = server.accept_subscriber(old_server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(!event_settled(&block_on(subscription.next_raw()).unwrap()));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(event_settled(&block_on(subscription.next_raw()).unwrap()));

    // Switching links immediately retires the old receipt, even while the old
    // transport remains alive long enough to race one more response.
    let (new_client_transport, new_server_transport) = duplex();
    let new_upstream = crate::db::block_on(client.connect_upstream(new_client_transport));
    let _new_subscriber = server.accept_subscriber(new_server_transport, client_author);
    assert!(
        !event_settled(&block_on(subscription.next_raw()).unwrap()),
        "edge switch must immediately demote the prior edge receipt"
    );

    // Confirm B before forcing fallback, so the test proves that staging A's
    // old traffic cannot transiently publish under B's receipt during detach.
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        event_settled(&block_on(subscription.next_raw()).unwrap()),
        "the selected B edge must have a real receipt before the fallback test"
    );

    seed(
        &server,
        "todos",
        cells("queued pre-fallback old-edge update", false, owner),
    );
    server.tick().unwrap();
    // Leave A's response staged while B is selected, then select A again by
    // detaching B. The selection barrier must consume that stale frame without
    // treating it as a new A receipt.
    assert!(client.detach_connection(&new_upstream));
    client.tick().unwrap();
    let mut drained_events = 0;
    while let Ok(event) = subscription.receiver.try_recv() {
        drained_events += 1;
        assert!(
            !event_settled(&event),
            "a ViewUpdate queued before fallback selection must not settle the subscription"
        );
    }
    assert!(
        drained_events > 0,
        "the row-changing staged A update must publish an explicitly unsettled event"
    );

    seed(
        &server,
        "todos",
        cells("fresh fallback-edge update", false, owner),
    );
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(
        event_settled(&block_on(subscription.next_raw()).unwrap()),
        "after the selected edge detaches, the surviving edge may settle only with its own fresh response"
    );

    drop(old_upstream);
}

#[test]
fn fallback_staged_cut_blocks_older_selected_confirmation() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let query = Query::from("todos");
    let update = |subscription, settled_through| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through,
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
    };

    let (old_client_transport, mut old_authority) = duplex();
    let _old_upstream = crate::db::block_on(client.connect_upstream(old_client_transport));
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();
    client.tick().unwrap();
    let old_key = loop {
        if let SyncMessage::Subscribe(subscribe) = old_authority.try_recv().unwrap() {
            break subscribe.subscription;
        }
    };
    old_authority.send(update(old_key, GlobalTime(1))).unwrap();
    client.tick().unwrap();
    assert!(subscription._state.borrow().settled);

    let (new_client_transport, mut new_authority) = duplex();
    let new_upstream = crate::db::block_on(client.connect_upstream(new_client_transport));
    client.tick().unwrap();
    let new_key = loop {
        if let SyncMessage::Subscribe(subscribe) = new_authority.try_recv().unwrap() {
            break subscribe.subscription;
        }
    };
    new_authority.send(update(new_key, GlobalTime(1))).unwrap();
    client.tick().unwrap();
    assert!(subscription._state.borrow().settled);

    old_authority.send(update(old_key, GlobalTime(3))).unwrap();
    assert!(client.detach_connection(&new_upstream));
    client.tick().unwrap();
    assert!(!subscription._state.borrow().settled);

    old_authority.send(update(old_key, GlobalTime(2))).unwrap();
    client.tick().unwrap();
    assert!(
        !subscription._state.borrow().settled,
        "eligible A@2 cannot receipt state after fallback-staged A@3"
    );
    old_authority.send(update(old_key, GlobalTime(3))).unwrap();
    client.tick().unwrap();
    assert!(subscription._state.borrow().settled);
}

#[test]
fn fallback_replay_of_preselection_row_repair_cannot_settle() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let query = Query::from("todos");

    let (old_client_transport, mut old_authority_transport) = duplex();
    let old_upstream = crate::db::block_on(client.connect_upstream(old_client_transport));
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();
    client.tick().unwrap();
    let old_subscription = loop {
        match old_authority_transport.try_recv().unwrap() {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    let view_update = |subscription, settled_through| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through,
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
    };
    old_authority_transport
        .send(view_update(old_subscription, GlobalTime(1)))
        .unwrap();
    client.tick().unwrap();
    assert!(subscription._state.borrow().settled);

    let (new_client_transport, mut new_authority_transport) = duplex();
    let new_upstream = crate::db::block_on(client.connect_upstream(new_client_transport));
    client.tick().unwrap();
    let new_subscription = loop {
        match new_authority_transport.try_recv().unwrap() {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    new_authority_transport
        .send(view_update(new_subscription, GlobalTime(2)))
        .unwrap();
    client.tick().unwrap();
    assert!(subscription._state.borrow().settled);

    let mut old = old_upstream.borrow_mut();
    let ConnectionLink::Upstream(UpstreamConnectionState {
        pending_row_version_repairs,
        ..
    }) = &mut old.link
    else {
        unreachable!("expected old upstream")
    };
    pending_row_version_repairs.push_back(PendingRowVersionRepair {
        requests: Vec::new(),
        update: view_update(old_subscription, GlobalTime(3)),
        authority_receipt_eligible: true,
    });
    drop(old);

    assert!(client.detach_connection(&new_upstream));
    old_authority_transport
        .send(SyncMessage::RowVersionPayloads {
            version_bundles: Vec::new(),
        })
        .unwrap();
    client.tick().unwrap();
    assert!(
        !subscription._state.borrow().settled,
        "a repair ViewUpdate deferred before fallback must not become A's receipt"
    );

    old_authority_transport
        .send(view_update(old_subscription, GlobalTime(4)))
        .unwrap();
    client.tick().unwrap();
    assert!(subscription._state.borrow().settled);
}

#[test]
fn restarted_client_reuses_durable_cursor_but_waits_for_current_authority_receipt() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_node = NodeUuid::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    seed(&server, "todos", cells("durable cache", false, owner));
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: client_node,
            author: client_author,
        },
        id_source: None,
    }))
    .unwrap();
    let (first_client_transport, first_server_transport) = duplex();
    let first_upstream = crate::db::block_on(client.connect_upstream(first_client_transport));
    let first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let query = Query::from("todos");
    let mut first_subscription =
        prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(!event_settled(
        &block_on(first_subscription.next_raw()).unwrap()
    ));
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(event_settled(
        &block_on(first_subscription.next_raw()).unwrap()
    ));
    drop(first_subscription);
    assert!(client.detach_connection(&first_upstream));
    assert!(server.server.detach_connection(&first_subscriber));
    drop(first_upstream);
    drop(first_subscriber);
    client.close().unwrap();
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: client_node,
            author: client_author,
        },
        id_source: None,
    }))
    .unwrap();
    let mut subscription = prepared_subscribe(&reopened, &query, global_subscribe_opts()).unwrap();
    assert!(
        !event_settled(&block_on(subscription.next_raw()).unwrap()),
        "an offline Edge/Global subscription must expose durable cached rows as unsettled"
    );
    let (reopened_client_transport, reopened_server_transport) = duplex();
    let _reopened_upstream =
        crate::db::block_on(reopened.connect_upstream(reopened_client_transport));
    let _reopened_subscriber = server.accept_subscriber(reopened_server_transport, client_author);
    reopened.tick().unwrap();
    server.tick().unwrap();
    reopened.tick().unwrap();
    assert!(event_settled(&block_on(subscription.next_raw()).unwrap()));
}
