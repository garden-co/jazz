use super::*;

#[test]
fn forward_update_to_servers_with_storage_replays_row_history_without_visible_region() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"history-only");

    add_server(&mut sm, &io, server_id);
    sm.take_outbox();
    seed_users_schema(&mut io);
    create_test_row_with_id(&mut io, row_id, Some(row_metadata("users")));
    io.append_history_region_rows("users", std::slice::from_ref(&row))
        .unwrap();

    sm.forward_update_to_servers_with_storage(&io, row_id, BranchName::new("main"));

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row: created, metadata, .. },
        } if id == server_id && created.batch_id() == row.batch_id() && metadata.is_some()
    )));
}

#[test]
fn add_server_with_storage_syncs_full_row_history_to_server() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    let newer = visible_row(row_id, "main", vec![older.batch_id()], 2_000, b"newer");

    seed_users_schema(&mut io);
    io.put_row_locator(
        row_id,
        Some(
            &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                .expect("row metadata should produce a row locator"),
        ),
    )
    .unwrap();
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

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    let outbox = sm.take_outbox();
    let schema_syncs = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::CatalogueEntryUpdated { .. },
                } if *id == server_id
            )
        })
        .count();
    assert_eq!(schema_syncs, 1);

    let row_syncs = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::RowBatchCreated { .. },
                } if *id == server_id
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(row_syncs.len(), 2);
    assert!(matches!(
        row_syncs[0],
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, metadata, .. },
        } if *id == server_id && row.batch_id() == older.batch_id() && metadata.is_some()
    ));
    assert!(matches!(
        row_syncs[1],
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, metadata, .. },
        } if *id == server_id && row.batch_id() == newer.batch_id() && metadata.is_some()
    ));
}

#[test]
fn add_server_with_storage_skips_rows_already_confirmed_upstream() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let local_pending = visible_row(row_id, "main", Vec::new(), 1_000, b"local-pending");
    let upstream_confirmed = row_with_state(
        visible_row(
            row_id,
            "main",
            vec![local_pending.batch_id()],
            2_000,
            b"upstream",
        ),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::EdgeServer),
    );

    seed_users_schema(&mut io);
    io.put_row_locator(
        row_id,
        Some(
            &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                .expect("row metadata should produce a row locator"),
        ),
    )
    .unwrap();
    io.append_history_region_rows(
        "users",
        &[local_pending.clone(), upstream_confirmed.clone()],
    )
    .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            upstream_confirmed.clone(),
            &[local_pending.clone(), upstream_confirmed.clone()],
        )),
    )
    .unwrap();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    let outbox = sm.take_outbox();
    let pushed_batch_ids: Vec<_> = outbox
        .iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::RowBatchCreated { row, .. },
            } if *id == server_id => Some(row.batch_id()),
            _ => None,
        })
        .collect();

    assert_eq!(pushed_batch_ids, vec![local_pending.batch_id()]);
}

#[test]
fn add_server_with_storage_skips_rows_confirmed_by_authoritative_batch_fate() {
    let mut io = MemoryStorage::new();
    seed_users_schema(&mut io);

    let rows: Vec<_> = (0..3)
        .map(|index| {
            let row_id = ObjectId::new();
            let row = visible_row(
                row_id,
                "main",
                Vec::new(),
                1_000 + index,
                format!("upstream-{index}").as_bytes(),
            );
            io.put_row_locator(
                row_id,
                Some(
                    &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                        .expect("row metadata should produce a row locator"),
                ),
            )
            .unwrap();
            io.append_history_region_rows("users", std::slice::from_ref(&row))
                .unwrap();
            io.upsert_visible_region_rows(
                "users",
                std::slice::from_ref(&VisibleRowEntry::rebuild(
                    row.clone(),
                    std::slice::from_ref(&row),
                )),
            )
            .unwrap();
            io.upsert_authoritative_batch_fate(&BatchFate::DurableDirect {
                batch_id: row.batch_id(),
                confirmed_tier: DurabilityTier::GlobalServer,
            })
            .unwrap();
            row
        })
        .collect();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    let outbox = sm.take_outbox();
    let row_syncs = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::RowBatchCreated { .. },
                } if *id == server_id
            )
        })
        .count();
    assert_eq!(
        row_syncs,
        0,
        "warm reconnect should not replay locally persisted rows whose batches already have authoritative global fate; rows={:?}",
        rows.iter().map(|row| row.batch_id()).collect::<Vec<_>>()
    );
}

#[test]
fn add_server_with_storage_sends_skipped_parent_before_child() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let upstream_confirmed_parent = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"upstream-parent"),
        crate::row_histories::RowState::VisibleDirect,
        Some(DurabilityTier::EdgeServer),
    );
    let local_child = visible_row(
        row_id,
        "main",
        vec![upstream_confirmed_parent.batch_id()],
        2_000,
        b"local-child",
    );

    seed_users_schema(&mut io);
    io.put_row_locator(
        row_id,
        Some(
            &crate::storage::row_locator_from_metadata(&row_metadata("users"))
                .expect("row metadata should produce a row locator"),
        ),
    )
    .unwrap();
    io.append_history_region_rows(
        "users",
        &[upstream_confirmed_parent.clone(), local_child.clone()],
    )
    .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            local_child.clone(),
            &[upstream_confirmed_parent.clone(), local_child.clone()],
        )),
    )
    .unwrap();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    let outbox = sm.take_outbox();
    let pushed_batch_ids: Vec<_> = outbox
        .iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::RowBatchCreated { row, .. },
            } if *id == server_id => Some(row.batch_id()),
            _ => None,
        })
        .collect();

    assert_eq!(
        pushed_batch_ids,
        vec![upstream_confirmed_parent.batch_id(), local_child.batch_id()]
    );
}
