use super::*;

#[test]
fn set_query_scope_stores_session() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();

    add_client(&mut sm, &io, client_id);
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        Some(crate::query_manager::session::Session::new("alice")),
    );

    let query = sm
        .get_client(client_id)
        .expect("client should exist")
        .queries
        .get(&QueryId(1))
        .expect("query should exist");
    assert_eq!(query.scope.len(), 1);
    assert_eq!(
        query
            .session
            .as_ref()
            .map(|session| session.user_id.as_str()),
        Some("alice")
    );
}

#[test]
fn query_settled_from_server_stores_scope_for_query() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let query_id = QueryId(11);
    let first_row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(1));
    let second_row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(2));

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::QuerySettled {
            query_id,
            tier: DurabilityTier::Local,
            scope: vec![
                (second_row_id, BranchName::new("main")),
                (first_row_id, BranchName::new("main")),
            ],
            through_seq: 0,
        },
    );

    assert_eq!(
        sm.remote_query_scope(query_id),
        HashSet::from([
            (first_row_id, BranchName::new("main")),
            (second_row_id, BranchName::new("main")),
        ])
    );
}

#[test]
fn query_settled_from_server_can_be_relayed_to_interested_clients_after_ready() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let query_id = QueryId(15);
    let row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(15));

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    sm.query_origin
        .entry(query_id)
        .or_default()
        .insert(client_id);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::QuerySettled {
            query_id,
            tier: DurabilityTier::Local,
            scope: vec![(row_id, BranchName::new("main"))],
            through_seq: 0,
        },
    );

    assert!(
        sm.take_outbox().is_empty(),
        "server QuerySettled should wait for RuntimeCore's stream watermark before relay"
    );
    sm.relay_query_settled_to_origins(server_id, query_id, DurabilityTier::Local);

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::QuerySettled { query_id: relayed_query_id, scope, .. },
        } if id == client_id
            && relayed_query_id == query_id
            && scope == vec![(row_id, BranchName::new("main"))]
    )));
}

#[test]
fn remove_server_clears_remote_query_scope() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let query_id = QueryId(23);
    let row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(23));

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::QuerySettled {
            query_id,
            tier: DurabilityTier::Local,
            scope: vec![(row_id, BranchName::new("main"))],
            through_seq: 0,
        },
    );
    assert_eq!(
        sm.remote_query_scope(query_id),
        HashSet::from([(row_id, BranchName::new("main"))])
    );
    assert_eq!(
        sm.take_remote_query_scope_dirty(),
        HashSet::from([query_id])
    );

    sm.remove_server(server_id);

    assert!(sm.remote_query_scope(query_id).is_empty());
    assert_eq!(
        sm.take_remote_query_scope_dirty(),
        HashSet::from([query_id])
    );
}
