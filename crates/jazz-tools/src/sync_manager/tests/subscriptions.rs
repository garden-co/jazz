use super::*;
use crate::row_histories::RowState;

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
        None,
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
fn send_query_subscription_targets_pending_server() {
    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_pending_server(server_id);

    let query = QueryBuilder::new("users").branch("main").build();
    sm.send_query_subscription_to_servers(
        QueryId(7),
        query,
        None,
        None,
        QueryPropagation::Full,
        vec![],
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::QuerySubscription { query_id, .. },
            } if *id == server_id && *query_id == QueryId(7)
        )),
        "query subscriptions must be queued for pending upstream transports"
    );
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
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
            one_shot: false,
        });
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: bob,
            query_id: QueryId(2),
            query,
            session: None,
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
            one_shot: false,
        });

    sm.remove_client(alice);

    assert_eq!(sm.pending_query_subscriptions.len(), 1);
    assert_eq!(sm.pending_query_subscriptions[0].client_id, bob);
}

#[test]
fn initial_query_scope_sends_one_settlement_per_batch() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = BatchId::new();
    let branch = BranchName::new("main");

    add_client(&mut sm, &io, client_id);

    let rows: Vec<_> = (0..3)
        .map(|index| {
            let row_id = ObjectId::new();
            let row = visible_row(
                row_id,
                "main",
                Vec::new(),
                1_000 + index,
                format!("user-{index}").as_bytes(),
            );
            let row = row_with_batch_state(
                row,
                batch_id,
                RowState::VisibleDirect,
                Some(DurabilityTier::GlobalServer),
            );
            seed_visible_row(&mut sm, &mut io, "users", row.clone());
            (row_id, row)
        })
        .collect();

    io.upsert_authoritative_batch_fate(&BatchFate::DurableDirect {
        batch_id,
        confirmed_tier: DurabilityTier::GlobalServer,
    })
    .unwrap();

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        rows.iter()
            .map(|(object_id, _)| (*object_id, branch))
            .collect(),
        None,
    );

    let outbox = sm.take_outbox();
    assert_eq!(
        outbox
            .iter()
            .filter(|entry| matches!(entry.payload, SyncPayload::RowBatchNeeded { .. }))
            .count(),
        3
    );

    let settlements: Vec<_> = outbox
        .iter()
        .filter_map(|entry| match &entry.payload {
            SyncPayload::BatchFate { fate } => Some(fate),
            _ => None,
        })
        .collect();
    assert_eq!(
        settlements.len(),
        1,
        "initial scope replay should coalesce settlement delivery by batch"
    );
    assert!(matches!(
        settlements[0],
        BatchFate::DurableDirect {
            batch_id: settled_batch_id,
            confirmed_tier: DurabilityTier::GlobalServer,
        } if *settled_batch_id == batch_id
    ));
}

/// A coalesced subscribe/unsubscribe pair marks the subscription one-shot
/// and still enqueues the unsubscription for an already-installed
/// subscription from a reconnect replay.
#[test]
fn coalesced_subscribe_unsubscribe_marks_one_shot_and_keeps_unsubscription() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);

    let query = QueryBuilder::new("messages").branch("main").build();
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QueryUnsubscription {
            query_id: QueryId(1),
        },
    });
    sm.process_inbox(&mut io);

    let subscriptions = sm.take_pending_query_subscriptions();
    assert_eq!(subscriptions.len(), 1);
    assert!(subscriptions[0].one_shot);
    let unsubscriptions = sm.take_pending_query_unsubscriptions();
    assert_eq!(unsubscriptions.len(), 1);
    assert_eq!(unsubscriptions[0].client_id, client_id);
    assert_eq!(unsubscriptions[0].query_id, QueryId(1));
}

/// The unsubscribe-then-resubscribe replay pattern must keep the
/// resubscription installed: only a pending subscription that precedes the
/// unsubscription is a coalesced one-shot pair.
#[test]
fn unsubscribe_then_resubscribe_keeps_the_resubscription() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QueryUnsubscription {
            query_id: QueryId(1),
        },
    });
    let query = QueryBuilder::new("messages").branch("main").build();
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    sm.process_inbox(&mut io);

    let subscriptions = sm.take_pending_query_subscriptions();
    assert_eq!(subscriptions.len(), 1);
    assert!(!subscriptions[0].one_shot);
    assert_eq!(sm.take_pending_query_unsubscriptions().len(), 1);
}
