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

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    });
    sm.process_inbox(&mut io);

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
                payload: SyncPayload::BatchFate { fate: BatchFate::DurableDirect { batch_id: settled, .. },
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
fn approved_user_row_retries_waiting_sealed_batch() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
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
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );

    let mut pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 1);
    sm.approve_permission_check(&mut io, pending.remove(0));

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate: BatchFate::DurableDirect { batch_id: settled, .. },
                },
            } if *id == client_id && *settled == batch_id
        )),
        "approving the row after SealBatch should settle the waiting batch, got {outbox:?}",
    );
}

#[test]
fn rejecting_one_user_write_rejects_the_whole_direct_batch() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);
    let client_id = ClientId::new();
    let batch_id = BatchId::new();
    let alice_id = ObjectId::new();
    let bob_id = ObjectId::new();
    let alice_row = row_with_batch_state(
        visible_row(alice_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );
    let bob_row = row_with_batch_state(
        visible_row(bob_id, "main", Vec::new(), 1_001, b"bob"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::Local),
    );

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::User);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
    );
    sm.take_outbox();

    for (row_id, row) in [(alice_id, alice_row.clone()), (bob_id, bob_row.clone())] {
        sm.process_from_client(
            &mut io,
            client_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row_id,
                    metadata: row_metadata("users"),
                }),
                row,
            },
        );
    }

    let mut pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 2);
    sm.approve_permission_check(&mut io, pending.remove(0));
    sm.reject_permission_check(&mut io, pending.remove(0), "bob denied".to_string());

    assert!(matches!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(BatchFate::Rejected { batch_id: settled, .. }) if settled == batch_id
    ));
    assert_eq!(
        io.load_history_row_batch("users", "main", alice_id, batch_id)
            .unwrap()
            .expect("approved member should have been stored")
            .state,
        crate::row_histories::RowState::Rejected,
        "rejecting one member should roll back the previously approved member in the same batch",
    );
    assert!(
        sm.take_pending_permission_checks().is_empty(),
        "later queued checks for the rejected batch should be cancelled"
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

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: older_row.clone(),
        },
    });
    sm.process_inbox(&mut io);

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
                payload: SyncPayload::BatchFate { fate: BatchFate::DurableDirect { batch_id: settled, .. },
                },
            } if *id == client_id && *settled == older_row.batch_id
        )),
        "idempotent replay of an older history row should re-emit its cached settlement, got {outbox:?}",
    );
}
