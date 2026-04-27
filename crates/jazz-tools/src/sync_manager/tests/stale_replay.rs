use super::*;

#[test]
fn stale_row_batch_from_client_replays_upstream_without_regressing_visible_row() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    add_server(&mut sm, &io, server_id);

    let newer = visible_row(row_id, "main", Vec::new(), 2_000, b"newer");
    seed_visible_row(&mut sm, &mut io, "users", newer.clone());

    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: older.clone(),
        },
    );

    let visible = load_visible_row(&io, "users", row_id, "main");
    assert_eq!(visible.batch_id(), newer.batch_id());

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, .. },
        } if id == server_id && row.batch_id() == older.batch_id()
    )));
}

#[test]
fn stale_row_batch_state_change_from_server_does_not_regress_newer_visible_row() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();

    add_server(&mut sm, &io, server_id);

    let newer = row_with_state(
        visible_row(row_id, "main", Vec::new(), 2_000, b"newer"),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::EdgeServer),
    );
    seed_visible_row(&mut sm, &mut io, "users", newer.clone());

    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: older.clone(),
        },
    );

    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").batch_id(),
        newer.batch_id(),
        "stale row replay should not replace the newer visible row",
    );

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id: older.batch_id(),
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").batch_id(),
        newer.batch_id(),
        "confirming a stale replayed row must not regress the visible winner",
    );
}

#[test]
fn stale_divergent_row_batch_from_client_does_not_regress_newer_visible_row() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    add_server(&mut sm, &io, server_id);

    seed_users_schema(&mut io);
    create_test_row_with_id(&mut io, row_id, Some(row_metadata("users")));

    let base = visible_row(row_id, "main", Vec::new(), 1_000, b"alice-v1");
    let alice_v2 = visible_row(row_id, "main", vec![base.batch_id()], 2_000, b"alice-v2");
    let alice_v3 = visible_row(
        row_id,
        "main",
        vec![alice_v2.batch_id()],
        3_000,
        b"alice-v3",
    );
    let alice_v4 = row_with_state(
        visible_row(
            row_id,
            "main",
            vec![alice_v3.batch_id()],
            4_000,
            b"alice-v4",
        ),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::EdgeServer),
    );

    let history = vec![
        base.clone(),
        alice_v2.clone(),
        alice_v3.clone(),
        alice_v4.clone(),
    ];
    io.append_history_region_rows("users", &history).unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(alice_v4.clone(), &history)),
    )
    .unwrap();

    let bob_stale = visible_row(
        row_id,
        "main",
        vec![base.batch_id()],
        2_500,
        b"bob-offline-edit",
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: bob_stale.clone(),
        },
    );

    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").batch_id(),
        alice_v4.batch_id(),
        "stale divergent replay should not replace the newer visible row",
    );

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id: bob_stale.batch_id(),
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").batch_id(),
        alice_v4.batch_id(),
        "acknowledging the stale divergent replay must not regress the visible winner",
    );
}
