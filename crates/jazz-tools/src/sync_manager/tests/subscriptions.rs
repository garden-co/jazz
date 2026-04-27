use super::*;

#[test]
fn send_query_subscription_includes_session() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let server_id = ServerId::new();
    add_server(&mut sm, &io, server_id);
    sm.take_outbox();

    let query = QueryBuilder::new("users").branch("main").build();
    let session = crate::query_manager::session::Session::new("alice");
    sm.send_query_subscription_to_servers(
        QueryId(7),
        query.clone(),
        Some(session.clone()),
        QueryPropagation::Full,
        vec![],
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::QuerySubscription {
                    query_id,
                    query: sent_query,
                    session: sent_session,
                    propagation,
                    ..
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*query_id, QueryId(7));
            assert_eq!(sent_query.table, query.table);
            assert_eq!(*propagation, QueryPropagation::Full);
            assert_eq!(
                sent_session
                    .as_ref()
                    .map(|session| session.user_id.as_str()),
                Some("alice")
            );
        }
        other => panic!("expected QuerySubscription to server, got {other:?}"),
    }
}

#[test]
fn query_subscription_falls_back_to_client_session_when_payload_omits_it() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
    );

    let pending = push_query_subscription(&mut sm, client_id, None);
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0]
            .session
            .as_ref()
            .map(|session| session.user_id.as_str()),
        Some("alice")
    );
}

#[test]
fn remove_client_cleans_pending_query_subscriptions() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);

    let query = QueryBuilder::new("users").build();
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: alice,
            query_id: QueryId(1),
            query: query.clone(),
            session: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        });
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: bob,
            query_id: QueryId(2),
            query,
            session: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        });

    sm.remove_client(alice);

    assert_eq!(sm.pending_query_subscriptions.len(), 1);
    assert_eq!(sm.pending_query_subscriptions[0].client_id, bob);
}
