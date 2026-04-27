use super::*;

#[test]
fn remove_client_cleans_up_server_subscriptions() {
    //
    // alice (client) ──subscribes──▶ server
    //
    // alice subscribes to two queries, then disconnects.
    // server_subscriptions should be empty after remove_client.
    //
    use crate::sync_manager::{ClientId, QueryId};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, alice);

    let query = server_qm.query("users").build();
    push_query_subscription(&mut server_qm, alice, 1, query.clone());
    push_query_subscription(&mut server_qm, alice, 2, query);
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    assert!(
        server_qm
            .server_subscriptions
            .contains_key(&(alice, QueryId(1)))
    );
    assert!(
        server_qm
            .server_subscriptions
            .contains_key(&(alice, QueryId(2)))
    );

    server_qm.remove_client(alice);

    assert!(
        server_qm.server_subscriptions.is_empty(),
        "server_subscriptions should be empty after client disconnect"
    );
    assert!(
        server_qm.sync_manager().get_client(alice).is_none(),
        "client should be removed from SyncManager"
    );
}

#[test]
fn remove_client_preserves_other_clients_subscriptions() {
    //
    // alice (client) ──subscribes──▶ server ◀──subscribes── bob (client)
    //
    // Both subscribe, then alice disconnects.
    // bob's subscription should survive.
    //
    use crate::sync_manager::{ClientId, QueryId};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let bob = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, alice);
    connect_client(&mut server_qm, &storage, bob);

    let query = server_qm.query("users").build();
    push_query_subscription(&mut server_qm, alice, 1, query.clone());
    push_query_subscription(&mut server_qm, bob, 1, query);
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    assert_eq!(server_qm.server_subscriptions.len(), 2);

    server_qm.remove_client(alice);

    assert_eq!(
        server_qm.server_subscriptions.len(),
        1,
        "only bob's subscription should remain"
    );
    assert!(
        server_qm
            .server_subscriptions
            .contains_key(&(bob, QueryId(1)))
    );
    assert!(server_qm.sync_manager().get_client(bob).is_some());
}

#[test]
fn remove_client_is_idempotent() {
    //
    // Calling remove_client twice should not panic or corrupt state.
    //
    use crate::sync_manager::ClientId;
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut server_qm, &storage, alice);

    let query = server_qm.query("users").build();
    push_query_subscription(&mut server_qm, alice, 1, query);
    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    server_qm.remove_client(alice);
    server_qm.remove_client(alice); // second call — should be a no-op

    assert!(server_qm.server_subscriptions.is_empty());
    assert!(server_qm.sync_manager().get_client(alice).is_none());
}
