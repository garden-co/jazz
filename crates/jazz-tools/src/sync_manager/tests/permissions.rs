use super::*;

#[test]
fn row_batch_created_from_user_with_exact_history_match_skips_permission_check() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();

    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );
    let batch_id = row.batch_id;

    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("bob"),
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );

    let pending = sm.take_pending_permission_checks();
    assert!(
        pending.is_empty(),
        "idempotent replay of an exact stored history row must not queue a permission check, got {} pending",
        pending.len(),
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchSettlement {
                    settlement: BatchSettlement::DurableDirect { batch_id: settled, .. },
                },
            } if *id == client_id && *settled == batch_id
        )),
        "idempotent replay should re-emit the cached settlement to the client, got {outbox:?}",
    );
}

#[test]
fn row_batch_created_from_user_with_same_batch_correction_queues_permission_check() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let batch_id = BatchId::new();

    let stale_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );
    let corrected_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-corrected"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );

    seed_visible_row(&mut sm, &mut io, "users", stale_row.clone());
    persist_visible_row_settlement(&mut io, row_id, &stale_row);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("bob"),
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: corrected_row.clone(),
        },
    );

    let pending = sm.take_pending_permission_checks();
    assert_eq!(
        pending.len(),
        1,
        "same-batch corrections must still run permission checks so the server can learn the corrected payload, got {pending:?}",
    );
    assert_eq!(pending[0].operation, Operation::Insert);
    assert_eq!(pending[0].old_content, None);
    assert_eq!(
        pending[0].new_content,
        Some(corrected_row.data.as_ref().to_vec()),
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "same-batch corrections should not short-circuit to a cached settlement, got {outbox:?}",
    );
}

#[test]
fn cancel_batch_from_unrelated_client_keeps_pending_permission_check() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    let upstream = ServerId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"Alice draft"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, alice);
    sm.set_client_role(alice, ClientRole::User);
    sm.set_client_session(alice, crate::query_manager::session::Session::new("alice"));
    add_client(&mut sm, &io, bob);
    sm.set_client_role(bob, ClientRole::Peer);
    add_server(&mut sm, &io, upstream);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        alice,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );
    assert_eq!(
        sm.pending_permission_checks.len(),
        1,
        "Alice's staged row should be waiting on permission evaluation"
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        bob,
        SyncPayload::CancelBatch {
            batch_id: row.batch_id,
        },
    );

    assert_eq!(
        sm.pending_permission_checks.len(),
        1,
        "Bob's spoofed cancel should not remove Alice's pending permission check"
    );
    assert_eq!(sm.pending_permission_checks[0].client_id, alice);

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().all(|entry| {
            !matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(server_id),
                    payload: SyncPayload::CancelBatch {
                        batch_id: cancelled
                    },
                } if *server_id == upstream && *cancelled == row.batch_id
            )
        }),
        "Bob's spoofed cancel should not be forwarded upstream, got {outbox:?}"
    );
}

#[test]
fn cancel_batch_from_owner_without_local_pending_work_does_not_forward_upstream() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let alice = ClientId::new();
    let upstream = ServerId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"Alice draft"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, alice);
    sm.set_client_role(alice, ClientRole::User);
    sm.set_client_session(alice, crate::query_manager::session::Session::new("alice"));
    add_server(&mut sm, &io, upstream);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        alice,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );
    let pending = sm.take_pending_permission_checks();
    assert_eq!(
        pending.len(),
        1,
        "Alice's staged row should be waiting on permission evaluation"
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        alice,
        SyncPayload::CancelBatch {
            batch_id: row.batch_id,
        },
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().all(|entry| {
            !matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(server_id),
                    payload: SyncPayload::CancelBatch {
                        batch_id: cancelled
                    },
                } if *server_id == upstream && *cancelled == row.batch_id
            )
        }),
        "owner cancel with no matching local pending rows or checks should not fan out upstream, got {outbox:?}"
    );
}

#[test]
fn cancel_batch_from_owner_uses_exact_pending_members_without_locator_scan() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut seeded = MemoryStorage::new();
    let alice = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"Alice draft"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    seed_users_schema(&mut seeded);
    add_client(&mut sm, &seeded, alice);
    sm.set_client_role(alice, ClientRole::Peer);

    let mut io = RowLocatorScanDisabledStorage::new(seeded);
    sm.process_from_client(
        &mut io,
        alice,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );
    sm.take_outbox();
    sm.process_from_client(
        &mut io,
        alice,
        SyncPayload::CancelBatch {
            batch_id: row.batch_id,
        },
    );

    let rows = io
        .scan_history_row_batches("users", row_id)
        .expect("cancel should leave readable row history");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].state,
        crate::row_histories::RowState::Rejected,
        "owner cancel should reject the exact pending row without discovering it through a locator scan"
    );
}

#[test]
fn row_batch_created_from_user_with_older_exact_history_match_skips_permission_check() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();

    let older_row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );
    let newer_row = row_with_state(
        visible_row(
            row_id,
            "main",
            vec![older_row.batch_id],
            1_100,
            b"alice-updated",
        ),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );

    seed_visible_row(&mut sm, &mut io, "users", older_row.clone());
    io.append_history_region_rows("users", std::slice::from_ref(&newer_row))
        .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            newer_row.clone(),
            std::slice::from_ref(&newer_row),
        )),
    )
    .unwrap();
    persist_visible_row_settlement(&mut io, row_id, &older_row);
    persist_visible_row_settlement(&mut io, row_id, &newer_row);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("bob"),
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: older_row.clone(),
        },
    );

    let pending = sm.take_pending_permission_checks();
    assert!(
        pending.is_empty(),
        "idempotent replay of an older stored history row must not queue a permission check, got {pending:?}",
    );

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchSettlement {
                    settlement: BatchSettlement::DurableDirect { batch_id: settled, .. },
                },
            } if *id == client_id && *settled == older_row.batch_id
        )),
        "idempotent replay of an older history row should re-emit its cached settlement, got {outbox:?}",
    );
}
