use super::*;

fn forwarding_metadata() -> HashMap<String, String> {
    row_metadata("users")
}

/// Test is pinned to 2000 to have deterministic reproduction.
/// However, a browser worker's wasm shadow stack is ~1 MiB by default,
/// so ~300 ancestors is probably enough to crash a real client.

#[test]
fn forwarding_a_deep_parent_chain_does_not_overflow_the_stack() {
    let depth = 2_000usize;

    let mut inner = MemoryStorage::new();
    seed_users_schema(&mut inner);

    let row_id = ObjectId::new();
    let mut history = Vec::with_capacity(depth + 1);
    let mut current = visible_row(row_id, "main", Vec::new(), 1_000, b"root");
    history.push(current.clone());
    for level in 1..=depth {
        let next = visible_row(
            row_id,
            "main",
            vec![current.batch_id()],
            1_000 + level as u64,
            format!("c{level}").as_bytes(),
        );
        history.push(next.clone());
        current = next;
    }
    let tip = current;

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &inner);

    inner
        .put_row_locator(
            row_id,
            Some(
                &crate::storage::row_locator_from_metadata(&forwarding_metadata())
                    .expect("row metadata should produce a row locator"),
            ),
        )
        .unwrap();
    inner.append_history_region_rows("users", &history).unwrap();

    // Run on a small stack to surface the unbounded recursion deterministically
    // and quickly, mirroring the constrained WASM worker shadow stack.
    let handle = std::thread::Builder::new()
        .stack_size(512 * 1024)
        .spawn(move || {
            sm.forward_row_batch_to_servers_with_storage(
                &inner,
                "users",
                row_id,
                forwarding_metadata(),
                tip,
            );
            sm.take_outbox()
                .into_iter()
                .filter(|entry| {
                    matches!(
                        entry,
                        OutboxEntry {
                            destination: Destination::Server(id),
                            payload: SyncPayload::RowBatchCreated { .. },
                        } if *id == server_id
                    )
                })
                .count()
        })
        .expect("spawning forwarding thread should succeed");

    let queued = handle
        .join()
        .expect("forwarding should not overflow the stack");
    assert_eq!(
        queued,
        depth + 1,
        "every batch in the chain should be queued exactly once"
    );
}

fn hot_row_tips(row_id: ObjectId, count: usize) -> Vec<StoredRowBatch> {
    (0..count)
        .map(|k| {
            visible_row(
                row_id,
                "main",
                Vec::new(),
                1_000 + k as u64,
                format!("v{k}").as_bytes(),
            )
        })
        .collect()
}

#[test]
fn forwarding_hot_row_updates_to_a_server_does_not_clone_the_sent_batch_set() {
    use crate::sync_manager::types::sent_batch_clone_probe;
    const FORWARDS: usize = 256;

    let io = MemoryStorage::new();
    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);
    let row_id = ObjectId::new();

    sent_batch_clone_probe::reset();
    for tip in hot_row_tips(row_id, FORWARDS) {
        sm.forward_row_batch_to_servers_with_storage(
            &io,
            "users",
            row_id,
            forwarding_metadata(),
            tip,
        );
    }

    let synced = sm
        .servers
        .get(&server_id)
        .and_then(|server| {
            server
                .sent_batch_ids
                .get(&(row_id, BranchName::new("main")))
        })
        .map_or(0, |sent| sent.len());
    assert_eq!(
        synced, FORWARDS,
        "each distinct tip should have grown the sent-batch set"
    );
    assert_eq!(
        sent_batch_clone_probe::count(),
        0,
        "server forwarding must test sent-batch membership by borrow, never by cloning the per-row set"
    );
}

#[test]
fn forwarding_hot_row_updates_to_a_client_does_not_clone_the_sent_batch_set() {
    use crate::sync_manager::types::sent_batch_clone_probe;
    const FORWARDS: usize = 256;

    let io = MemoryStorage::new();
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    let row_id = ObjectId::new();
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    sent_batch_clone_probe::reset();
    for tip in hot_row_tips(row_id, FORWARDS) {
        sm.queue_row_to_client(client_id, row_id, forwarding_metadata(), tip, false);
    }

    let synced = sm
        .clients
        .get(&client_id)
        .and_then(|client| {
            client
                .sent_batch_ids
                .get(&(row_id, BranchName::new("main")))
        })
        .map_or(0, |sent| sent.len());
    assert_eq!(
        synced, FORWARDS,
        "each distinct tip should have grown the sent-batch set"
    );
    assert_eq!(
        sent_batch_clone_probe::count(),
        0,
        "client forwarding must test sent-batch membership by borrow, never by cloning the per-row set"
    );
}
