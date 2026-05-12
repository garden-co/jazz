use super::*;

#[test]
fn initial_query_sync_replays_current_direct_batch_fate() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let mut row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    row.confirmed_tier = Some(DurabilityTier::Local);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate },
        } if *id == client_id && *fate == BatchFate::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Local,
        }
    )));
}

#[test]
fn initial_query_sync_prefers_authoritative_settlement_over_retained_client_local_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let retained_client_local_settlement = BatchFate::DurableDirect {
        batch_id: row.batch_id,
        confirmed_tier: DurabilityTier::Local,
    };
    let authoritative_settlement = BatchFate::DurableDirect {
        batch_id: row.batch_id,
        confirmed_tier: DurabilityTier::EdgeServer,
    };

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    io.upsert_local_batch_record(&crate::batch_fate::LocalBatchRecord::new(
        row.batch_id,
        crate::batch_fate::BatchMode::Direct,
        true,
        Some(retained_client_local_settlement),
    ))
    .unwrap();
    io.upsert_authoritative_batch_fate(&authoritative_settlement)
        .unwrap();

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let settlements = sm
        .take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate },
            } if id == client_id => Some(fate),
            _ => None,
        })
        .collect::<Vec<_>>();

    assert_eq!(
        settlements,
        vec![authoritative_settlement],
        "server replay should not downgrade an authoritative edge settlement to the retained client-local tier"
    );
}

#[test]
fn server_replay_does_not_send_local_durability_ack_upstream() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row = row_with_batch_state(
        visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice"),
        BatchId::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );

    seed_users_schema(&mut io);
    add_server(&mut sm, &io, server_id);
    sm.take_outbox();

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row.row_id,
                metadata: row_metadata("users"),
            }),
            row,
        },
    );

    assert!(
        sm.take_outbox().into_iter().all(|entry| !matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::BatchFate { .. },
            } if id == server_id
        )),
        "lower-tier nodes must not turn server replay into upstream durability acknowledgements"
    );
}

#[test]
fn client_durability_ack_is_not_authoritative() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = BatchId::new();

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchFate {
            fate: BatchFate::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::Local,
            },
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        None,
        "clients must not be able to create authoritative durability settlements"
    );
    assert!(
        sm.take_pending_batch_fates().is_empty(),
        "ignored client acknowledgements must not be replayed through RuntimeCore"
    );
}

#[test]
fn sealed_direct_client_write_persists_authoritative_fate() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row = visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice");

    seed_users_schema(&mut io);
    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row.row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );
    assert_eq!(
        io.load_authoritative_batch_fate(row.batch_id).unwrap(),
        None,
        "an unsealed direct batch should not become globally authoritative from row arrival alone"
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                row.batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row.row_id,
                    row_digest: row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(row.batch_id).unwrap(),
        Some(BatchFate::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Local,
        }),
        "an authoritative server that accepts a visible direct client write must persist its durable fate"
    );
}

#[test]
fn replayed_approved_user_write_returns_durable_fate_not_missing() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row = visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice");

    seed_users_schema(&mut io);
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
                id: row.row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );

    let pending = sm.take_pending_permission_checks();
    assert_eq!(
        pending.len(),
        1,
        "first user write should wait for permission approval"
    );
    sm.approve_permission_check(&mut io, pending.into_iter().next().unwrap());
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                row.batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row.row_id,
                    row_digest: row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );
    sm.take_outbox();

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row.row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    });
    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate {
                    fate: BatchFate::DurableDirect {
                        batch_id,
                        confirmed_tier: DurabilityTier::Local,
                    },
                },
            } if *id == client_id && *batch_id == row.batch_id
        )),
        "idempotent replay should return the authoritative durable fate, got {outbox:?}"
    );
    assert!(
        outbox.iter().all(|entry| !matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate {
                    fate: BatchFate::Missing { batch_id },
                },
            } if *id == client_id && *batch_id == row.batch_id
        )),
        "accepted client writes must not be reported as missing on replay, got {outbox:?}"
    );
}

#[test]
fn initial_query_sync_sends_only_current_row_for_deep_history() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    let newer = visible_row(row_id, "main", vec![older.batch_id()], 2_000, b"newer");

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_users_schema(&mut io);
    create_test_row_with_id(&mut io, row_id, Some(row_metadata("users")));
    io.append_history_region_rows("users", &[older.clone(), newer.clone()])
        .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            newer.clone(),
            &[older.clone(), newer.clone()],
        )),
    )
    .unwrap();

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let row_payloads: Vec<_> = sm
        .take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::RowBatchNeeded { row, .. },
            } if id == client_id => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_payloads.len(),
        1,
        "initial sync should send only the current row"
    );
    assert_eq!(row_payloads[0].batch_id(), newer.batch_id());
    assert_eq!(row_payloads[0].data, newer.data);
    assert!(
        row_payloads[0].parents.is_empty(),
        "initial sync payload should be self-contained for subscribers"
    );
}

#[test]
fn initial_query_sync_replays_current_accepted_transaction_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Local),
    );

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate },
        } if *id == client_id && *fate == BatchFate::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Local,
        }
    )));
}

#[test]
fn batch_fate_needed_returns_current_accepted_transaction() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Local),
    );

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchFateNeeded {
            batch_ids: vec![row.batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate },
        } if id == client_id && fate == BatchFate::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Local,
        }
    )));
}

#[test]
fn accepted_transaction_settlement_before_rows_materializes_when_row_arrives() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let batch_id = BatchId::new();
    let row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );
    let settlement = BatchFate::AcceptedTransaction {
        batch_id,
        confirmed_tier: DurabilityTier::EdgeServer,
    };

    seed_users_schema(&mut io);
    add_server(&mut sm, &io, server_id);
    sm.take_outbox();

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::BatchFate {
            fate: settlement.clone(),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(settlement),
        "settlement should persist even when its member rows are not present yet"
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "settlement-first delivery should be a no-op until the row exists"
    );

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchNeeded {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row,
        },
    );

    let visible = load_visible_row(&io, "users", row_id, "main");
    assert_eq!(
        visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(visible.confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(visible.batch_id, batch_id);
}

#[test]
fn batch_fate_needed_deduplicates_requested_batch_ids() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::GlobalServer),
    );

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    persist_visible_row_settlement(&mut io, row_id, &row);
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchFateNeeded {
            batch_ids: vec![row.batch_id, row.batch_id, row.batch_id],
        },
    );

    assert_eq!(
        sm.take_outbox()
            .into_iter()
            .filter(|entry| matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::BatchFate { .. },
                } if *id == client_id
            ))
            .count(),
        1,
        "duplicate settlement requests for one batch should produce one response"
    );
}

#[test]
fn replayed_rows_queue_one_settlement_per_batch_after_inbox_batch() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let branch_name = BranchName::new("main");
    let batch_id = BatchId::new();

    add_client(&mut sm, &io, client_id);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
    );
    sm.take_outbox();

    let rows: Vec<_> = (0..3)
        .map(|index| {
            let row_id = ObjectId::new();
            let row = row_with_batch_state(
                visible_row(
                    row_id,
                    "main",
                    Vec::new(),
                    1_000 + index,
                    format!("user-{index}").as_bytes(),
                ),
                batch_id,
                crate::row_histories::RowState::VisibleDirect,
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
            .map(|(object_id, _)| (*object_id, branch_name))
            .collect(),
        None,
    );
    sm.take_outbox();

    for (_, row) in &rows {
        sm.push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::RowBatchNeeded {
                metadata: Some(RowMetadata {
                    id: row.row_id,
                    metadata: row_metadata("users"),
                }),
                row: row.clone(),
            },
        });
    }
    sm.process_inbox(&mut io);

    assert_eq!(
        sm.take_outbox()
            .into_iter()
            .filter(|entry| matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::BatchFate { .. },
                } if *id == client_id
            ))
            .count(),
        1,
        "exact row replay should settle once per batch after the inbox batch"
    );
}

#[test]
fn server_row_state_changes_queue_one_settlement_per_batch_after_inbox_batch() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let branch_name = BranchName::new("main");
    let batch_id = BatchId::new();

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();

    let rows: Vec<_> = (0..3)
        .map(|index| {
            let row_id = ObjectId::new();
            let row = row_with_batch_state(
                visible_row(
                    row_id,
                    "main",
                    Vec::new(),
                    1_000 + index,
                    format!("user-{index}").as_bytes(),
                ),
                batch_id,
                crate::row_histories::RowState::VisibleDirect,
                Some(DurabilityTier::Local),
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
            .map(|(object_id, _)| (*object_id, branch_name))
            .collect(),
        None,
    );
    sm.take_outbox();

    sm.push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::BatchFate {
            fate: BatchFate::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
            },
        },
    });
    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();
    assert_eq!(
        outbox
            .iter()
            .filter(|entry| matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Client(id),
                    payload: SyncPayload::BatchFate { .. },
                } if *id == client_id
            ))
            .count(),
        1,
        "server batch settlement should be forwarded once per interested batch"
    );
}

#[test]
fn seal_batch_accepts_all_staged_transactional_rows_as_one_settlement() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let first_row_id = ObjectId::new();
    let second_row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(first_row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );
    let second_row = row_with_batch_state(
        visible_row(second_row_id, "main", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    for row in [first_row.clone(), second_row.clone()] {
        sm.process_from_client(
            &mut io,
            client_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row.row_id,
                    metadata: row_metadata("users"),
                }),
                row,
            },
        );
    }
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: first_row_id,
                        row_digest: first_row.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: second_row_id,
                        row_digest: second_row.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );

    let settlement = io
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
        .expect("sealed transactional batch should persist an authoritative settlement");
    let BatchFate::AcceptedTransaction {
        batch_id: settled_batch_id,
        confirmed_tier,
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(settled_batch_id, batch_id);
    assert_eq!(confirmed_tier, DurabilityTier::Local);

    let first_visible = io
        .load_visible_region_row("users", "main", first_row_id)
        .unwrap()
        .expect("first row should become visible after seal");
    let second_visible = io
        .load_visible_region_row("users", "main", second_row_id)
        .unwrap()
        .expect("second row should become visible after seal");
    assert_eq!(
        first_visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(
        second_visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate: returned },
        } if *id == client_id && *returned == BatchFate::AcceptedTransaction {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
        }
    )));
}

#[test]
fn seal_batch_rejection_stops_when_settlement_persistence_fails() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = FailingHistoryPatchStorage::new();
    io.fail_authoritative_settlement_upsert = true;
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(io.inner_mut());

    sm.add_client_with_storage(&io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let staged_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: staged_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: staged_row.clone(),
        },
    );
    sm.take_outbox();

    let mut submission = sealed_submission(
        batch_id,
        "main",
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: staged_row.content_digest(),
        }],
        Vec::new(),
    );
    submission.batch_digest = crate::digest::Digest32([255; 32]);

    sm.process_from_client(&mut io, client_id, SyncPayload::SealBatch { submission });

    assert_eq!(io.load_authoritative_batch_fate(batch_id).unwrap(), None);
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "failed settlement persistence should not publish or reject the batch"
    );
    assert!(sm.take_outbox().is_empty());
}
