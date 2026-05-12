use super::*;

#[test]
fn transactional_row_from_client_stays_staged_until_batch_is_sealed() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
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

    let history_rows = io.scan_history_row_batches("users", row_id).unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::StagingPending
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "staging rows should not become visible until the batch is sealed"
    );
    assert_eq!(
        io.load_authoritative_batch_fate(row.batch_id).unwrap(),
        None,
        "authority should not decide a transactional batch before it is sealed"
    );

    assert!(sm.take_outbox().into_iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::BatchFate { .. },
        } if id == client_id
    )));
}

#[test]
fn direct_batch_from_client_sends_one_settlement_on_seal() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let alice_id = ObjectId::new();
    let bob_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let alice = row_with_batch_state(
        visible_row(alice_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    let bob = row_with_batch_state(
        visible_row(bob_id, "main", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );

    for row in [alice.clone(), bob.clone()] {
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

    assert!(sm.take_outbox().into_iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::BatchFate { .. },
        } if id == client_id
    )));

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: alice_id,
                        row_digest: alice.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: bob_id,
                        row_digest: bob.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );

    let outbox = sm.take_outbox();
    let settlements = outbox
        .iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate },
            } if *id == client_id => Some(fate),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(settlements.len(), 1);
    let BatchFate::DurableDirect {
        batch_id: settled_batch_id,
        confirmed_tier,
    } = settlements[0]
    else {
        panic!(
            "expected durable direct settlement, got {:?}",
            settlements[0]
        );
    };
    assert_eq!(*settled_batch_id, batch_id);
    assert_eq!(*confirmed_tier, DurabilityTier::Local);
}

#[test]
fn direct_batch_seal_promotes_existing_local_settlement_to_authority_tier() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
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
    io.upsert_authoritative_batch_fate(&BatchFate::DurableDirect {
        batch_id,
        confirmed_tier: DurabilityTier::Local,
    })
    .unwrap();

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

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(BatchFate::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
        }),
        "sealing a direct batch should promote a stale local settlement to the accepting authority tier"
    );
}

#[test]
fn direct_client_settlement_is_not_authoritative() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
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
        "direct client fate must not become an authoritative settlement"
    );
    assert_eq!(io.load_local_batch_record(batch_id).unwrap(), None);
    assert_eq!(io.load_sealed_batch_submission(batch_id).unwrap(), None);
}

#[test]
fn direct_client_settlement_before_row_is_not_authoritative_after_row() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.add_pending_server(server_id);
    sm.take_outbox();

    let row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
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
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        None,
        "direct client fate must not become authoritative after the row arrives"
    );
    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "a fate received without SealBatch must not synthesize replayable membership; outbox={outbox:?}"
    );
}

#[test]
fn direct_client_settlement_before_row_keeps_sealed_submission_without_server() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
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
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        None,
        "direct client fate must not become authoritative"
    );
    assert_eq!(
        io.load_sealed_batch_submission(batch_id).unwrap(),
        None,
        "a fate received without SealBatch must not synthesize replayable membership"
    );
}

#[test]
fn seal_batch_to_servers_targets_pending_server_transport() {
    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");

    sm.add_pending_server(server_id);
    sm.seal_batch_to_servers(sealed_submission(
        batch_id,
        "main",
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: row.content_digest(),
        }],
        Vec::new(),
    ));

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::SealBatch { submission },
        } if id == server_id && submission.batch_id == batch_id
    )));
}

#[test]
fn direct_batch_from_client_settles_when_rows_arrive_after_seal() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let alice_id = ObjectId::new();
    let bob_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let alice = row_with_batch_state(
        visible_row(alice_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    let bob = row_with_batch_state(
        visible_row(bob_id, "main", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::VisibleDirect,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: alice.row_id,
                metadata: row_metadata("users"),
            }),
            row: alice.clone(),
        },
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: alice_id,
                        row_digest: alice.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: bob_id,
                        row_digest: bob.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: bob.row_id,
                metadata: row_metadata("users"),
            }),
            row: bob,
        },
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate: BatchFate::DurableDirect {
                    batch_id: settled_batch_id,
                    confirmed_tier: DurabilityTier::Local,},
            },
        } if *id == client_id
            && *settled_batch_id == batch_id    )));
}

#[test]
fn seal_batch_collapses_same_row_to_latest_visible_member() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    // client
    //   first staged version  -> same row, same batch
    //   second staged version -> same row, same batch
    //   seal batch
    //
    // authority
    //   settles one visible member for that row
    //   publishes only the latest staged content
    let first_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-updated"),
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
            submission: transactional_sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: second_row.content_digest(),
                }],
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

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("latest row should become visible after seal");
    assert_eq!(visible.batch_id(), batch_id);
    assert!(visible.parents.is_empty());
    assert_eq!(visible.data, second_row.data);
    assert_eq!(visible.batch_id, batch_id);
    assert_eq!(
        visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );

    let history_rows = io.scan_history_row_batches("users", row_id).unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(history_rows[0].batch_id(), batch_id);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(history_rows[0].data, second_row.data);

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate {
                fate: BatchFate::AcceptedTransaction {
                    batch_id: changed_batch_id,
                    confirmed_tier: DurabilityTier::Local,
                },
            },
        } if *id == client_id && *changed_batch_id == batch_id
    )));
    assert!(!outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row, .. },
        } if *id == client_id && row.row_id == row_id
    )));
}

#[test]
fn seal_batch_same_row_preserves_pre_transaction_parent_frontier() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    let base_row = visible_row(row_id, "main", Vec::new(), 900, b"base");
    seed_visible_row(&mut sm, &mut io, "users", base_row.clone());

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(row_id, "main", vec![base_row.batch_id()], 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(
            row_id,
            "main",
            vec![base_row.batch_id()],
            1_100,
            b"alice-updated",
        ),
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
            submission: transactional_sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: second_row.content_digest(),
                }],
                vec![CapturedFrontierMember {
                    object_id: row_id,
                    branch_name: BranchName::new("main"),
                    batch_id: base_row.batch_id(),
                }],
            ),
        },
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("sealed batch should publish the accepted row");
    assert_eq!(visible.batch_id(), batch_id);
    assert_eq!(visible.parents.as_slice(), [base_row.batch_id()]);
    assert_eq!(visible.data, second_row.data);
}

#[test]
fn seal_batch_waits_for_all_declared_rows_before_accepting() {
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
    let first_row_batch_id = first_row.content_digest();
    let second_row_batch_id = second_row.content_digest();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: first_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: first_row,
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: transactional_sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: first_row_id,
                        row_digest: first_row_batch_id,
                    },
                    SealedBatchMember {
                        object_id: second_row_id,
                        row_digest: second_row_batch_id,
                    },
                ],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        None,
        "authority should wait for all declared rows before settling the batch"
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", first_row_id)
            .unwrap(),
        None,
        "partial sealed batches should not publish visible rows yet"
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: second_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: second_row,
        },
    );

    let settlement = io
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
        .expect("authority should settle once all declared rows have arrived");
    let BatchFate::AcceptedTransaction { .. } = settlement else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
}

#[test]
fn seal_batch_waits_for_declared_latest_row_batch_before_accepting() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-updated"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: first_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: first_row.clone(),
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: transactional_sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: second_row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        None,
        "authority should wait for the declared final row batch entry, not just any row for that object"
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "earlier staged versions should not become visible just because the object id was declared"
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: second_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: second_row.clone(),
        },
    );

    let settlement = io
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
        .expect("authority should settle once the declared final row batch entry arrives");
    let BatchFate::AcceptedTransaction {
        batch_id: settled_batch_id,
        confirmed_tier,
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(settled_batch_id, batch_id);
    assert_eq!(confirmed_tier, DurabilityTier::Local);

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("declared final row batch entry should become visible");
    assert_eq!(visible.batch_id(), batch_id);
}

#[test]
fn same_row_staging_in_one_batch_keeps_only_latest_live_pending_member_before_seal() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-updated"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: first_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: first_row.clone(),
        },
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: second_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: second_row.clone(),
        },
    );

    let history_rows = io.scan_history_row_batches("users", row_id).unwrap();
    let live_pending_rows: Vec<_> = history_rows
        .iter()
        .filter(|row| matches!(row.state, crate::row_histories::RowState::StagingPending))
        .collect();
    assert_eq!(
        live_pending_rows.len(),
        1,
        "authority staging should keep one live pending member for a same-row batch rewrite"
    );
    assert_eq!(history_rows.len(), 1);
    assert_eq!(live_pending_rows[0].batch_id(), batch_id);
    assert_eq!(live_pending_rows[0].data, second_row.data);
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "pre-seal transactional rewrites should remain non-visible"
    );
}

#[test]
fn seal_batch_rejects_members_spanning_multiple_target_branches() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let main_row_id = ObjectId::new();
    let draft_row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let main_row = row_with_batch_state(
        visible_row(main_row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let draft_row = row_with_batch_state(
        visible_row(draft_row_id, "draft", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: main_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: main_row.clone(),
        },
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: draft_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: draft_row.clone(),
        },
    );
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
                        object_id: main_row_id,
                        row_digest: main_row.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: draft_row_id,
                        row_digest: draft_row.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(BatchFate::Rejected {
            batch_id,
            code: "invalid_batch_submission".to_string(),
            reason: "sealed batch rows must belong to the declared target branch".to_string(),
        })
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", main_row_id)
            .unwrap(),
        None
    );
    assert_eq!(
        io.load_visible_region_row("users", "draft", draft_row_id)
            .unwrap(),
        None
    );

    let main_history_rows = io.scan_history_row_batches("users", main_row_id).unwrap();
    let draft_history_rows = io.scan_history_row_batches("users", draft_row_id).unwrap();
    assert_eq!(main_history_rows.len(), 1);
    assert_eq!(draft_history_rows.len(), 1);
    assert_eq!(
        main_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
    assert_eq!(
        draft_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn seal_batch_rejects_when_batch_digest_does_not_match_members() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
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

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(BatchFate::Rejected {
            batch_id,
            code: "invalid_batch_submission".to_string(),
            reason: "sealed batch digest does not match declared members".to_string(),
        })
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "invalid batch digests should be rejected before publication"
    );
}

#[test]
fn seal_batch_acceptance_stops_when_submission_persistence_fails() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = FailingHistoryPatchStorage::new();
    io.fail_sealed_submission_upsert = true;
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

    let submission = sealed_submission(
        batch_id,
        "main",
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: staged_row.content_digest(),
        }],
        Vec::new(),
    );

    sm.process_from_client(&mut io, client_id, SyncPayload::SealBatch { submission });

    assert_eq!(io.load_sealed_batch_submission(batch_id).unwrap(), None);
    assert_eq!(io.load_authoritative_batch_fate(batch_id).unwrap(), None);
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "failed sealed-submission persistence should leave the batch unpublished"
    );
    assert!(sm.take_outbox().is_empty());
}

#[test]
fn seal_batch_rejects_when_family_visible_frontier_changed() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let existing_row_id = ObjectId::new();
    let conflicting_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();
    let target_branch = "dev-aaaaaaaaaaaa-main";
    let sibling_branch = "dev-bbbbbbbbbbbb-main";
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let existing_row = visible_row(existing_row_id, target_branch, Vec::new(), 900, b"seen");
    seed_visible_row(&mut sm, &mut io, "users", existing_row.clone());

    let staged_row = row_with_batch_state(
        visible_row(staged_row_id, target_branch, Vec::new(), 1_000, b"alice"),
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

    let conflicting_row = visible_row(
        conflicting_row_id,
        sibling_branch,
        Vec::new(),
        1_050,
        b"bob",
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: conflicting_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: conflicting_row,
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                target_branch,
                vec![SealedBatchMember {
                    object_id: staged_row_id,
                    row_digest: staged_row.content_digest(),
                }],
                vec![CapturedFrontierMember {
                    object_id: existing_row_id,
                    branch_name: BranchName::new(target_branch),
                    batch_id: existing_row.batch_id(),
                }],
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(BatchFate::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "family-visible frontier changed since batch was sealed".to_string(),
        })
    );
    assert_eq!(
        io.load_visible_region_row("users", target_branch, staged_row_id)
            .unwrap(),
        None,
        "conflicted sealed batch should not publish its staged row"
    );

    let history_rows = io.scan_history_row_batches("users", staged_row_id).unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn seal_batch_accepts_when_family_visible_frontier_matches() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let existing_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();
    let target_branch = "dev-aaaaaaaaaaaa-main";
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let existing_row = visible_row(existing_row_id, target_branch, Vec::new(), 900, b"seen");
    seed_visible_row(&mut sm, &mut io, "users", existing_row.clone());

    let staged_row = row_with_batch_state(
        visible_row(staged_row_id, target_branch, Vec::new(), 1_000, b"alice"),
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

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                target_branch,
                vec![SealedBatchMember {
                    object_id: staged_row_id,
                    row_digest: staged_row.content_digest(),
                }],
                vec![CapturedFrontierMember {
                    object_id: existing_row_id,
                    branch_name: BranchName::new(target_branch),
                    batch_id: existing_row.batch_id(),
                }],
            ),
        },
    );

    assert!(matches!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(BatchFate::AcceptedTransaction {
            batch_id: settled_batch_id,
            confirmed_tier: DurabilityTier::Local,
        }) if settled_batch_id == batch_id
    ));
}

#[test]
fn seal_batch_replay_returns_existing_settlement_after_frontier_moves() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let existing_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();
    let newer_row_id = ObjectId::new();
    let target_branch = "dev-aaaaaaaaaaaa-main";
    let sibling_branch = "dev-bbbbbbbbbbbb-main";
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let existing_row = visible_row(existing_row_id, target_branch, Vec::new(), 900, b"seen");
    seed_visible_row(&mut sm, &mut io, "users", existing_row.clone());

    let staged_row = row_with_batch_state(
        visible_row(staged_row_id, target_branch, Vec::new(), 1_000, b"alice"),
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

    let submission = sealed_submission(
        batch_id,
        target_branch,
        vec![SealedBatchMember {
            object_id: staged_row_id,
            row_digest: staged_row.content_digest(),
        }],
        vec![CapturedFrontierMember {
            object_id: existing_row_id,
            branch_name: BranchName::new(target_branch),
            batch_id: existing_row.batch_id(),
        }],
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: submission.clone(),
        },
    );
    sm.take_outbox();

    let accepted_settlement = BatchFate::AcceptedTransaction {
        batch_id,
        confirmed_tier: DurabilityTier::Local,
    };
    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(accepted_settlement.clone())
    );

    seed_visible_row(
        &mut sm,
        &mut io,
        "users",
        visible_row(newer_row_id, sibling_branch, Vec::new(), 1_100, b"bob"),
    );

    sm.process_from_client(&mut io, client_id, SyncPayload::SealBatch { submission });

    assert_eq!(
        io.load_authoritative_batch_fate(batch_id).unwrap(),
        Some(accepted_settlement.clone()),
        "replayed seals must be idempotent once the authority has decided the batch"
    );
    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate },
        } if *id == client_id && *fate == accepted_settlement
    )));
}
