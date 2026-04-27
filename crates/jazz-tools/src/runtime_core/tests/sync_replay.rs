use super::*;

#[test]
fn rc_replays_downstream_query_when_upstream_added_late() {
    // Build A <-> B first (no B <-> C yet), so B processes a downstream
    // query subscription before it has any upstream server.
    let schema = test_schema();
    let app_id = AppId::from_name("query-replay-test");

    let mgr_a =
        SchemaManager::new(SyncManager::new(), schema.clone(), app_id, "dev", "main").unwrap();
    let mut a = new_test_core(mgr_a, MemoryStorage::new(), NoopScheduler);

    let mgr_b = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
        schema.clone(),
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut b = new_test_core(mgr_b, MemoryStorage::new(), NoopScheduler);

    let mgr_c = SchemaManager::new(
        SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer),
        schema,
        app_id,
        "dev",
        "main",
    )
    .unwrap();
    let mut c = new_test_core(mgr_c, MemoryStorage::new(), NoopScheduler);

    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    {
        b.add_client(a_client_of_b, None);
        b.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(a_client_of_b, ClientRole::Peer);
    }
    a.add_server(b_server_for_a);

    // Clear any startup sync traffic.
    a.immediate_tick();
    b.immediate_tick();
    c.immediate_tick();
    a.batched_tick();
    b.batched_tick();
    c.batched_tick();
    a.sync_sender().take();
    b.sync_sender().take();
    c.sync_sender().take();

    // Downstream client A subscribes before B has an upstream.
    let _handle = a.subscribe(Query::new("users"), |_delta| {}, None).unwrap();

    // Deliver only A -> B messages.
    a.batched_tick();
    for entry in a.sync_sender().take() {
        if entry.destination == Destination::Server(b_server_for_a) {
            b.park_sync_message(InboxEntry {
                source: Source::Client(a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    b.batched_tick();
    b.immediate_tick();
    b.batched_tick();
    b.sync_sender().take();

    // Bring up B <-> C after B already has active downstream query state.
    {
        c.add_client(b_client_of_c, None);
        c.schema_manager_mut()
            .query_manager_mut()
            .sync_manager_mut()
            .set_client_role(b_client_of_c, ClientRole::Peer);
    }
    b.add_server(c_server_for_b);
    b.batched_tick();

    let forwarded_query_subscriptions = b
        .sync_sender()
        .take()
        .into_iter()
        .filter(|entry| {
            matches!(
                &entry.destination,
                Destination::Server(server_id) if *server_id == c_server_for_b
            ) && matches!(&entry.payload, SyncPayload::QuerySubscription { .. })
        })
        .count();

    assert!(
        forwarded_query_subscriptions > 0,
        "Expected B to replay existing downstream QuerySubscription(s) when adding upstream"
    );
}

#[test]
fn rc_replays_active_queries_on_upstream_reconnect() {
    let mut s = create_3tier_rc();

    let _handle =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();
    pump_a_to_b(&mut s);

    let initial_forwarded = s.b.sync_sender().take();
    assert!(
        count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
        "Expected initial QuerySubscription forwarding from B to C"
    );

    // Simulate upstream disconnect/reconnect.
    s.b.remove_server(s.c_server_for_b);
    s.b.add_server(s.c_server_for_b);
    s.b.batched_tick();

    let replayed_forwarded = s.b.sync_sender().take();
    assert!(
        count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b) > 0,
        "Expected active QuerySubscription replay after upstream reconnect"
    );
}

#[test]
fn rc_does_not_replay_unsubscribed_queries_on_upstream_reconnect() {
    let mut s = create_3tier_rc();

    let handle =
        s.a.subscribe(Query::new("users"), |_delta| {}, None)
            .unwrap();
    pump_a_to_b(&mut s);

    let initial_forwarded = s.b.sync_sender().take();
    assert!(
        count_query_subscriptions_to_server(&initial_forwarded, s.c_server_for_b) > 0,
        "Expected initial QuerySubscription forwarding from B to C"
    );

    s.a.unsubscribe(handle);
    pump_a_to_b(&mut s);
    s.b.sync_sender().take(); // Drain unsubscription forwarding and unrelated traffic.

    // Reconnect upstream and ensure replay no longer includes this query.
    s.b.remove_server(s.c_server_for_b);
    s.b.add_server(s.c_server_for_b);
    s.b.batched_tick();

    let replayed_forwarded = s.b.sync_sender().take();
    assert_eq!(
        count_query_subscriptions_to_server(&replayed_forwarded, s.c_server_for_b),
        0,
        "Unsubscribed query must not be replayed after upstream reconnect"
    );
}
