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

/// Scaling guard (a ratio, not a wall-clock budget): forwarding a second batch
/// of updates against a larger already-sent set must not take materially longer
/// than the first. Cloning the sent set per forward made it ~3x slower.
#[test]
fn forwarding_hot_row_updates_do_not_scale_with_synced_history() {
    use std::time::{Duration, Instant};
    const PER_PHASE: usize = 100_000;

    let io = MemoryStorage::new();
    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);
    let row_id = ObjectId::new();

    // Build the tips up front so the timed phases measure only forwarding.
    let tips: Vec<_> = (0..2 * PER_PHASE)
        .map(|k| {
            visible_row(
                row_id,
                "main",
                Vec::new(),
                1_000 + k as u64,
                format!("v{k}").as_bytes(),
            )
        })
        .collect();

    let forward_range = |sm: &mut SyncManager, range: std::ops::Range<usize>| -> Duration {
        let start = Instant::now();
        for tip in &tips[range] {
            sm.forward_row_batch_to_servers_with_storage(
                &io,
                "users",
                row_id,
                forwarding_metadata(),
                tip.clone(),
            );
        }
        start.elapsed()
    };

    // First phase grows the sent-batch set 0 -> PER_PHASE; second PER_PHASE -> 2*PER_PHASE.
    let first = forward_range(&mut sm, 0..PER_PHASE);
    let second = forward_range(&mut sm, PER_PHASE..2 * PER_PHASE);

    let ratio = second.as_secs_f64() / first.as_secs_f64();
    assert!(
        ratio < 2.0,
        "hot-row forwards scaled with synced-history size: the second {PER_PHASE} forwards took \
         {ratio:.2}x the first (first={first:?}, second={second:?}). The per-object sent-batch set \
         is being cloned on every forward."
    );
}
