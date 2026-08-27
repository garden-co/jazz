//! Shared node scheduling, dirty-generation cascades, and connection servicing tests.

use super::*;

#[test]
fn large_write_pushes_staging_before_syncing_its_referencing_row() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let core = open_core(0xc0, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xc1, author, &schema);
    let (writer_transport, core_transport) = duplex();
    let _upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let title = "push-before-row/".repeat(8_000);
    writer
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.clone())),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    for _ in 0..16 {
        writer.tick().unwrap();
        core.tick().unwrap();
        if !core.read(&core.table("todos")).unwrap().is_empty() {
            break;
        }
    }
    let rows = core.read(&core.table("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].cell_at(0), Some(Value::String(title)));
}

/// Internal topology canary: exact push-before-row ordering on both relay legs
/// and pull forwarding after edge chunk eviction are protocol/runtime
/// properties that are not observable through the public client API alone.
/// The accepted write and reconstructed value are still asserted through that
/// API. Every node is opened with its own storage directory.
#[test]
fn large_value_pushes_through_edge_then_pulls_from_core_after_edge_chunk_eviction() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let core = open_core(0xc5, AuthorSubject::SYSTEM, &schema);
    let upload_edge = open_db(0xc6, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xc7, author, &schema);

    let (upload_edge_transport, core_upload_transport, upload_edge_to_core) =
        duplex_with_client_outbound_tap();
    let _upload_edge_upstream =
        crate::db::block_on(upload_edge.connect_upstream(upload_edge_transport));
    let _core_upload_edge = core.accept_subscriber_with_trust(
        core_upload_transport,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (writer_transport, upload_edge_client_transport, writer_to_upload_edge) =
        duplex_with_client_outbound_tap();
    let _writer_upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _upload_edge_writer = upload_edge.accept_subscriber(upload_edge_client_transport, author);

    let title = "multi-hop-large-value/".repeat(8_000);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String(title.clone())),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    let mut writer_messages = Vec::new();
    let mut upload_edge_messages = Vec::new();
    for _ in 0..64 {
        writer.tick().unwrap();
        writer_messages.extend(writer_to_upload_edge.borrow().iter().cloned());
        upload_edge.tick().unwrap();
        upload_edge_messages.extend(upload_edge_to_core.borrow().iter().cloned());
        core.tick().unwrap();
        upload_edge.tick().unwrap();
        writer.tick().unwrap();
        if writer.write_state(write.tx_id).unwrap().durability == DurabilityTier::Global {
            break;
        }
    }
    assert_eq!(
        writer.write_state(write.tx_id).unwrap().durability,
        DurabilityTier::Global
    );
    assert_eq!(
        core.read(&core.table("todos")).unwrap()[0].cell_at(0),
        Some(Value::String(title.clone()))
    );

    for (leg, messages) in [
        ("writer-to-upload-edge", writer_messages),
        ("upload-edge-to-core", upload_edge_messages),
    ] {
        let staged = messages
            .iter()
            .rposition(|message| matches!(message, SyncMessage::ChunkUploadNodes(_)))
            .unwrap_or_else(|| panic!("{leg} sends receiver-requested chunk nodes"));
        let row = messages
            .iter()
            .position(|message| {
                matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id)
            })
            .unwrap_or_else(|| panic!("{leg} sends the referencing row"));
        assert!(staged < row, "{leg} stages the chunks before the row");
    }
    assert_eq!(
        prepared_read(&upload_edge, &upload_edge.table("todos")).len(),
        1,
        "the upload edge retained the accepted row"
    );

    // Retain the accepted row and its disclosed locator, but replace only the
    // edge's Groove chunk backend with an empty independent store. Its only
    // route to the value bytes is now to forward this edge-local access to Core.
    upload_edge
        .node
        .node
        .borrow_mut()
        .set_chunk_storage(Rc::new(groove::chunks::MemoryChunkStorage::new()));
    let query = upload_edge.table("todos");
    let mut subscription = prepared_subscribe(
        &upload_edge,
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    )
    .unwrap();
    let mut received = None;
    let mut snapshot = RelationSnapshot::default();
    let mut pull_messages = Vec::new();
    for _ in 0..128 {
        upload_edge.tick().unwrap();
        pull_messages.extend(upload_edge_to_core.borrow().iter().cloned());
        core.tick().unwrap();
        upload_edge.tick().unwrap();
        while let Some(event) = subscription.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
        received = snapshot.rows.first().and_then(|row| row.cell_at(0));
        if received == Some(Value::String(title.clone())) {
            break;
        }
    }
    assert_eq!(
        snapshot.rows.len(),
        1,
        "the empty edge delivers the referencing row",
    );
    assert!(
        pull_messages
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkRequestBatch(_))),
        "the empty edge requests missing chunks from Core"
    );
    assert_eq!(
        received,
        Some(Value::String(title)),
        "the empty edge forwards the missing chunk pull to Core"
    );
}

#[derive(Clone)]
struct PausedUploadRetryClock(Rc<Cell<u64>>);

impl UploadRetryClock for PausedUploadRetryClock {
    fn now_ms(&self) -> u64 {
        self.0.get()
    }
}

/// Internal transport test: the public write outcome is asserted below, but
/// the exact-batch retry and no-early-resend properties sit below the public
/// API at the peer protocol boundary.
#[test]
fn rate_limited_push_waits_then_retries_the_exact_batch_without_rejecting_the_write() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let core = open_core(0xc3, AuthorSubject::SYSTEM, &schema);
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES
                + 1,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
    let writer = open_db(0xc2, author, &schema);
    let clock = Rc::new(Cell::new(10_000));
    writer
        .node
        .set_upload_retry_clock_for_test(Rc::new(PausedUploadRetryClock(Rc::clone(&clock))));
    let scheduler = Rc::new(RecordingScheduler::default());
    writer.set_tick_scheduler(Some(scheduler.clone()));
    let writer_node = NodeUuid::from_bytes([0xc2; 16]);
    let core_node = NodeUuid::from_bytes([0xc3; 16]);
    let (writer_transport, core_transport, writer_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            writer_node,
            1,
            core_node,
            1,
        );
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("rate-limited/".repeat(8_000)),
                ),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    // Start, receive the requested frontier, then send the first batch that
    // Core rate-limits. Capture it before the Core transport drains it.
    writer.tick().unwrap();
    core.tick().unwrap();
    writer.tick().unwrap();
    let first_batch = writer_outbound
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::ChunkUploadNodes(batch) => Some(batch.clone()),
            _ => None,
        })
        .expect("writer sends the requested chunk batch");
    core.tick().unwrap();
    writer.tick().unwrap();

    assert_eq!(
        scheduler.take_delays(),
        vec![1_000],
        "RateLimited schedules the bounded admission deadline rather than a deferred hot loop"
    );
    assert!(
        !matches!(
            writer.write_state(write.tx_id).unwrap().fate,
            Fate::Rejected(_)
        ),
        "a rate-limited batch remains resumable"
    );

    // Reconnect before the deadline. The old transport's queue-local upload
    // state must transfer only to this same logical destination, while the
    // node-scoped deadline also gates a fresh Start on any replacement link.
    assert!(writer.detach_connection(&upstream));
    let (reconnected_transport, reconnected_core_transport, reconnected_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            writer_node,
            2,
            core_node,
            2,
        );
    let _reconnected_upstream = crate::db::block_on(writer.connect_upstream(reconnected_transport));
    let _reconnected_subscriber = core.accept_subscriber(reconnected_core_transport, author);

    // An unrelated immediate/manual host tick before the deadline must not
    // resend the batch. The paused clock makes this deterministic.
    for _ in 0..3 {
        writer.tick().unwrap();
        assert!(
            reconnected_outbound.borrow().is_empty(),
            "reconnect sends neither Start nor chunk nodes before the admission deadline"
        );
    }

    // The receiver becomes admissible before the scheduled retry, then the
    // fake clock advances exactly to that deadline. The retry is byte-for-byte
    // the same requested batch, not a restarted upload or a new row write.
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy::default());
    clock.set(11_000);
    writer.tick().unwrap();
    assert!(
        !reconnected_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadStart(_))),
        "same-destination reconnect resumes the retained frontier instead of restarting upload"
    );
    let retry_batch = reconnected_outbound
        .borrow()
        .iter()
        .find_map(|message| match message {
            SyncMessage::ChunkUploadNodes(batch) => Some(batch.clone()),
            _ => None,
        })
        .expect("the deadline permits the retained batch to retry");
    assert_eq!(
        retry_batch, first_batch,
        "retry retains the exact failed batch"
    );

    for _ in 0..128 {
        core.tick().unwrap();
        writer.tick().unwrap();
        if writer.write_state(write.tx_id).unwrap().durability == DurabilityTier::Global {
            break;
        }
    }
    assert_eq!(
        writer.write_state(write.tx_id).unwrap().durability,
        DurabilityTier::Global,
        "the delayed retry eventually publishes the original write"
    );
    assert_eq!(core.read(&core.table("todos")).unwrap().len(), 1);
}

/// Unauthenticated links retain the bounded admission deadline but never a
/// receiver-specific frontier; expiry still independently reclaims staging.
#[test]
fn unauthenticated_reconnect_restarts_after_deadline_and_does_not_prevent_ttl_cleanup() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xc8; 16]);
    let core = open_core(0xc9, AuthorSubject::SYSTEM, &schema);
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES
                + 1,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
    let writer = open_db(0xc8, author, &schema);
    let clock = Rc::new(Cell::new(20_000));
    writer
        .node
        .set_upload_retry_clock_for_test(Rc::new(PausedUploadRetryClock(Rc::clone(&clock))));
    let scheduler = Rc::new(RecordingScheduler::default());
    writer.set_tick_scheduler(Some(scheduler.clone()));
    let (writer_transport, core_transport, writer_outbound) = duplex_with_client_outbound_tap();
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                (
                    "title".to_owned(),
                    Value::String("expired-rate-limited/".repeat(8_000)),
                ),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();

    writer.tick().unwrap();
    core.tick().unwrap();
    writer.tick().unwrap();
    assert!(
        writer_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadNodes(_)))
    );
    assert!(
        !writer_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::CommitUnit { .. })),
        "the initial row commit remains behind the rate-limited upload"
    );
    core.tick().unwrap();
    writer.tick().unwrap();
    assert_eq!(scheduler.take_delays(), vec![1_000]);

    assert!(writer.detach_connection(&upstream));
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: u64::MAX,
            window_ms: 60_000,
            max_age_ms: 0,
        });
    std::thread::sleep(std::time::Duration::from_millis(2));
    assert_eq!(
        crate::db::block_on(core.server.evict_expired_staged_large_values()).unwrap(),
        1,
        "the abandoned receiver-side staging claim expires"
    );

    assert!(
        writer.node.detached_large_value_uploads.borrow().is_empty(),
        "a context-free link never retains another receiver's missing-node frontier"
    );
    assert!(
        writer
            .node
            .large_value_upload_retry_deadlines
            .borrow()
            .contains_key(&write.tx_id),
        "the sender retains only the bounded admission deadline"
    );

    let (reconnected_transport, reconnected_core_transport, reconnected_outbound) =
        duplex_with_client_outbound_tap();
    let _reconnected_upstream = crate::db::block_on(writer.connect_upstream(reconnected_transport));
    let _reconnected_subscriber = core.accept_subscriber(reconnected_core_transport, author);
    writer.tick().unwrap();
    assert!(
        reconnected_outbound.borrow().is_empty(),
        "an unauthenticated reconnect remains gated before the deadline"
    );
    clock.set(21_000);
    writer.tick().unwrap();
    assert!(
        reconnected_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadStart(_))),
        "after the deadline an unauthenticated reconnect starts a fresh handshake"
    );
    assert!(
        !reconnected_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadNodes(_))),
        "an unauthenticated reconnect never replays the previous receiver frontier"
    );
}

fn assert_different_authenticated_destination_restarts_upload(
    reconnect_remote_node: NodeUuid,
    reconnect_link_identity: AuthorSubject,
) {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xd2; 16]);
    let writer_node = NodeUuid::from_bytes([0xd2; 16]);
    let core_node = NodeUuid::from_bytes([0xd3; 16]);
    let core = open_core(0xd3, AuthorSubject::SYSTEM, &schema);
    core.node()
        .borrow_mut()
        .set_large_value_staging_policy(crate::node::LargeValueStagingPolicy {
            incoming_bytes_per_window: crate::node::LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES
                + 1,
            window_ms: 60_000,
            max_age_ms: 10 * 60 * 1_000,
        });
    let writer = open_db(0xd2, author, &schema);
    let clock = Rc::new(Cell::new(30_000));
    writer
        .node
        .set_upload_retry_clock_for_test(Rc::new(PausedUploadRetryClock(Rc::clone(&clock))));
    let (writer_transport, core_transport, _writer_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            author,
            writer_node,
            1,
            core_node,
            1,
        );
    let upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let write = writer
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("isolated/".repeat(8_000))),
                ("done".to_owned(), Value::Bool(false)),
                ("owner".to_owned(), Value::Uuid(author.test_uuid())),
            ]),
            Default::default(),
        )
        .unwrap();
    writer.tick().unwrap();
    core.tick().unwrap();
    writer.tick().unwrap();
    core.tick().unwrap();
    writer.tick().unwrap();
    assert!(
        writer
            .node
            .large_value_upload_retry_deadlines
            .borrow()
            .contains_key(&write.tx_id)
    );
    assert!(writer.detach_connection(&upstream));
    assert_eq!(writer.node.detached_large_value_uploads.borrow().len(), 1);

    clock.set(31_000);
    let (reconnect_transport, reconnect_core_transport, reconnect_outbound) =
        duplex_with_admitted_session_context_and_client_outbound_tap(
            reconnect_link_identity,
            writer_node,
            2,
            reconnect_remote_node,
            2,
        );
    let _reconnect = crate::db::block_on(writer.connect_upstream(reconnect_transport));
    let _reconnect_subscriber = core.accept_subscriber(reconnect_core_transport, author);
    writer.tick().unwrap();
    assert!(
        reconnect_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadStart(_))),
        "a mismatched authenticated destination starts a fresh handshake"
    );
    assert!(
        !reconnect_outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::ChunkUploadNodes(_))),
        "a mismatched authenticated destination never receives the retained frontier"
    );
    assert_eq!(
        writer.node.detached_large_value_uploads.borrow().len(),
        1,
        "a mismatched reconnect cannot consume the original destination's frontier"
    );
}

#[test]
fn reconnect_to_different_authenticated_node_never_replays_upload_frontier() {
    assert_different_authenticated_destination_restarts_upload(
        NodeUuid::from_bytes([0xd4; 16]),
        AuthorSubject::for_test_bytes([0xd2; 16]),
    );
}

#[test]
fn reconnect_with_different_authenticated_link_never_replays_upload_frontier() {
    assert_different_authenticated_destination_restarts_upload(
        NodeUuid::from_bytes([0xd3; 16]),
        AuthorSubject::for_test_bytes([0xd5; 16]),
    );
}

/// A Core immediately refreshes a peer-edge subscriber that was visited before
/// a later client upload in the same service pass, so Bob receives Alice's
/// later canonical row without needing an unrelated next websocket frame.
///
/// ```text
/// bob --empty Global subscribe--> peer edge --> Core
/// alice --later CommitUnit----------------------> Core
///                                                |
///                 Core ViewUpdate <--------------+
/// bob <--- peer-edge local IVM refresh <---------+
/// ```
///
/// The peer connection is deliberately accepted before Alice's connection.
/// That makes Core service the already-covered peer first, then accept Alice's
/// write. The one Core tick following Alice's upload must revisit the earlier
/// peer before it returns; otherwise an event-driven websocket host has no
/// reason to call Core again and Bob stays indefinitely at the old empty cut.
#[test]
fn core_later_client_upload_refreshes_earlier_peer_subscription_in_same_tick() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob_author = AuthorSubject::for_test_bytes([0xb1; 16]);
    let core = open_core(0xd1, AuthorSubject::SYSTEM, &schema);
    let peer_edge = open_db(0xd2, AuthorSubject::SYSTEM, &schema);
    let bob = open_db(0xd3, bob_author, &schema);

    // Keep the Core-to-peer queue observable, and accept this peer before
    // Alice so the ordering under test is fixed.
    let (peer_transport, core_transport, core_to_peer) = duplex_with_server_outbound_tap();
    let _peer_upstream = crate::db::block_on(peer_edge.connect_upstream(peer_transport));
    let _core_peer = core.accept_subscriber_with_trust(
        core_transport,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (bob_transport, peer_client_transport) = duplex();
    let _bob_upstream = crate::db::block_on(bob.connect_upstream(bob_transport));
    let _peer_client = peer_edge.accept_subscriber(peer_client_transport, bob_author);

    let query = bob.table("todos");
    let mut subscription = prepared_subscribe(&bob, &query, global_subscribe_opts()).unwrap();
    let opening = (0..32)
        .find_map(|_| {
            bob.tick().unwrap();
            peer_edge.tick().unwrap();
            core.tick().unwrap();
            peer_edge.tick().unwrap();
            bob.tick().unwrap();
            subscription.try_next_event()
        })
        .expect("Bob receives the established empty Global view");
    assert!(event_settled(&opening));
    assert!(opened_rows(opening).is_empty());
    assert!(
        core_to_peer.borrow().is_empty(),
        "the empty opening has been fully consumed before Alice writes"
    );

    let alice_edge = open_db(0xd4, alice, &schema);
    let (alice_transport, core_alice_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_edge.connect_upstream(alice_transport));
    let _core_alice = core.accept_subscriber(core_alice_transport, alice);
    let write = alice_edge
        .insert(
            "todos",
            cells("later row", false, alice),
            crate::db::InsertOptions {
                row_id: Some(row(0xd5)),
                ..Default::default()
            },
        )
        .unwrap();

    // One edge tick uploads Alice's local commit; one Core tick finalizes it
    // and must also serve the earlier peer connection.
    alice_edge.tick().unwrap();
    core.tick().unwrap();
    let later_view_updates = core_to_peer
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    result_member_adds,
                    settled_through,
                    ..
                }) if *settled_through > GlobalTime(0)
                    && result_member_adds.iter().any(|member| {
                        member.as_row().is_some_and(|(table, row_uuid, tx_id)| {
                            table.as_str() == "todos"
                                && row_uuid == row(0xd5)
                                && tx_id == write.tx_id
                        })
                    })
            )
        })
        .count();
    assert_eq!(
        later_view_updates, 1,
        "the first Core service pass after Alice's upload sends the later canonical membership to the already-covered peer"
    );

    // Applying that upstream ViewUpdate must dirty and refresh the existing
    // Bob connection in the same peer-edge service pass.
    peer_edge.tick().unwrap();
    bob.tick().unwrap();
    let delivered = subscription
        .try_next_event()
        .expect("Bob receives the later row without a retry or a new query");
    let (added, updated, removed) = delta_rows(delivered);
    assert_eq!(row_ids(&added), vec![row(0xd5)]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    // The bounded second pass clears its dirty work. A quiet later tick must
    // neither replay the unchanged view nor self-arm another serving loop.
    core.tick().unwrap();
    assert!(
        core_to_peer.borrow().is_empty(),
        "a post-cascade idle tick emits no unchanged peer update"
    );
}

/// An Edge immediately flushes an upload queued by a later client connection
/// through the upstream connection that was already visited in the same pass.
///
/// The upstream connection is deliberately installed first. One client tick
/// places the commit on the Edge subscriber transport; one Edge tick must both
/// ingest it and emit the corresponding Core-bound `CommitUnit`.
#[test]
fn edge_later_client_upload_flushes_earlier_upstream_in_same_tick() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_db(0xd1, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xd2, alice, &schema);

    let (edge_transport, _core_transport, edge_to_core) = duplex_with_client_outbound_tap();
    let _edge_upstream = crate::db::block_on(edge.connect_upstream(edge_transport));

    let (client_transport, edge_client_transport) = duplex();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _edge_client = edge.accept_subscriber(edge_client_transport, alice);

    let write = client
        .insert(
            "todos",
            cells("later upload", false, alice),
            crate::db::InsertOptions {
                row_id: Some(row(0xd3)),
                ..Default::default()
            },
        )
        .unwrap();
    client.tick().unwrap();
    edge.tick().unwrap();

    let uploads = edge_to_core
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id
            )
        })
        .count();
    assert_eq!(
        uploads, 1,
        "one Edge service pass flushes the later client upload through the earlier upstream link"
    );

    edge.tick().unwrap();
    assert_eq!(
        edge_to_core
            .borrow()
            .iter()
            .filter(|message| {
                matches!(
                    message,
                    SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.tx_id
                )
            })
            .count(),
        1,
        "a quiet follow-up tick does not replay the same upload"
    );
}

#[test]
fn write_state_waiter_resolves_on_remote_fate_update() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let write = client
        .insert(
            "todos",
            cells("wait for fate", false, owner),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    assert_eq!(
        client.write_state(tx_id).unwrap().durability,
        DurabilityTier::Local
    );

    let changed = client.next_write_state_change(tx_id);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    block_on(changed);

    let state = client.write_state(tx_id).unwrap();
    assert_eq!(state.fate, Fate::Accepted);
    assert_eq!(state.durability, DurabilityTier::Global);
}

#[test]
fn db_sync_surface_preserves_creator_provenance_across_peer_update() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let receiver = open_db(0xc1, alice, &schema);

    let write = server
        .insert_attributed(alice, "todos", cells("created by alice", false, alice))
        .unwrap();
    let row = write.row_uuid();
    let query = Query::from("todos");
    let create_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap();
    receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(create_unit)
        .unwrap();

    server.next_now_ms.set(2);
    let bob_update = server
        .update_attributed(
            bob,
            "todos",
            row,
            BTreeMap::from([(
                "title".to_owned(),
                Value::String("updated by bob".to_owned()),
            )]),
        )
        .unwrap();
    block_on(bob_update.wait(DurabilityTier::Global)).unwrap();
    let server_rows = server.read(&query).unwrap();
    assert_eq!(server_rows.len(), 1);
    assert_eq!(
        server_rows[0].provenance().unwrap().unwrap().updated_by,
        bob
    );
    let update_unit = server
        .node()
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = update_unit else {
        panic!("expected update commit unit");
    };
    assert_eq!(versions[0].created_by(), alice);
    assert_eq!(versions[0].updated_by(), bob);
    let receiver_updates = receiver
        .node
        .node
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::CommitUnit { tx, versions })
        .unwrap();
    assert!(
        receiver_updates.iter().any(|message| {
            matches!(
                message,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            )
        }),
        "receiver should accept the update, got {receiver_updates:?}"
    );
    let receiver_unit = receiver
        .node
        .node
        .borrow_mut()
        .commit_unit_for(bob_update.mergeable_tx_id())
        .unwrap();
    let SyncMessage::CommitUnit {
        versions: receiver_versions,
        ..
    } = receiver_unit
    else {
        panic!("expected receiver commit unit");
    };
    assert_eq!(receiver_versions[0].created_by(), alice);
    assert_eq!(receiver_versions[0].updated_by(), bob);

    let alice_rows = prepared_read(&receiver, &query);
    assert_eq!(alice_rows.len(), 1);
    assert_eq!(alice_rows[0].row_uuid(), row);
    let provenance = alice_rows[0]
        .provenance()
        .unwrap()
        .expect("current rows should carry provenance");
    assert_eq!(provenance.created_by, alice);
    assert_eq!(provenance.updated_by, bob);
    assert!(
        provenance.created_at < provenance.updated_at,
        "updating a row must preserve creator provenance while advancing updater provenance"
    );
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_private_table_query() {
    let schema = owner_id_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let writer = open_db(0xa1, alice, &schema);
    let reader = open_db(0xb2, bob, &schema);

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                (
                    "owner_id".to_owned(),
                    Value::String(alice.test_uuid().to_string()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = crate::db::block_on(reader.connect_upstream(reader_transport));
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(bob.test_uuid().to_string()),
        )]),
    );
    let query = Query::from("messages");
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
}

/// A real client commonly reads its self-membership grant before querying the
/// resource that grant authorizes. The second subscription must publish a
/// result membership even when the first subscription already delivered the
/// resource as policy support.
fn membership_grant_then_parent_query_keeps_disjunctive_read_proof(indexed: bool) {
    let member_exists = public_exists(
        "members",
        [
            public_outer_eq("workspace_id", "id"),
            public_session_eq("subject", &["claims", "user_id"]),
        ],
    );
    let workspaces = PublicTableSchemaBuilder::new("workspaces")
        .column("owner_subject", PublicColumnType::Text)
        .policies(
            PublicTablePolicies::new()
                .with_select(PublicPolicyExpr::Or(vec![
                    public_session_eq("owner_subject", &["claims", "user_id"]),
                    member_exists,
                ]))
                .with_insert(PublicPolicyExpr::True),
        );
    let members = PublicTableSchemaBuilder::new("members")
        .fk_column("workspace_id", "workspaces")
        .column("subject", PublicColumnType::Text)
        .column("role", PublicColumnType::Text)
        .policies(
            PublicTablePolicies::new()
                .with_select(PublicPolicyExpr::Or(vec![
                    public_session_eq("subject", &["claims", "user_id"]),
                    PublicPolicyExpr::Inherits {
                        operation: PublicOperation::Select,
                        via_column: "workspace_id".to_owned(),
                        max_depth: None,
                    },
                ]))
                .with_insert(PublicPolicyExpr::True),
        );
    let workspaces = if indexed {
        workspaces.index_only(["owner_subject"])
    } else {
        workspaces
    };
    let members = if indexed {
        members.index_only(["workspace_id", "subject", "role"])
    } else {
        members
    };
    let schema =
        build_public_db_test_schema(PublicSchemaBuilder::new().table(workspaces).table(members));
    let manager = AuthorSubject::for_test_bytes([0xa1; 16]);
    let owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, manager, &schema);
    let owner_client = open_db(0xb2, owner, &schema);
    let (owner_transport, server_owner_transport) = duplex();
    let _owner_upstream = crate::db::block_on(owner_client.connect_upstream(owner_transport));
    let _owner_subscriber = server.accept_subscriber_with_claims(
        server_owner_transport,
        owner,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(owner.test_uuid().to_string()),
        )]),
    );
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(
        server_transport,
        manager,
        BTreeMap::from([(
            "user_id".to_owned(),
            Value::String(manager.test_uuid().to_string()),
        )]),
    );
    let workspace = owner_client
        .insert(
            "workspaces",
            BTreeMap::from([(
                "owner_subject".to_owned(),
                Value::String(owner.test_uuid().to_string()),
            )]),
            Default::default(),
        )
        .unwrap();
    let grant = owner_client
        .insert(
            "members",
            BTreeMap::from([
                (
                    "workspace_id".to_owned(),
                    Value::Uuid(workspace.row_uuid().0),
                ),
                (
                    "subject".to_owned(),
                    Value::String(manager.test_uuid().to_string()),
                ),
                ("role".to_owned(), Value::String("member".to_owned())),
            ]),
            Default::default(),
        )
        .unwrap();
    for _ in 0..16 {
        owner_client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        if server.read(&server.table("members")).unwrap().len() == 1 {
            break;
        }
    }
    assert_eq!(server.read(&server.table("workspaces")).unwrap().len(), 1);
    assert_eq!(server.read(&server.table("members")).unwrap().len(), 1);
    let grant_query =
        Query::from("members").filter(eq(col("id"), lit(Value::Uuid(grant.row_uuid().0))));
    let mut grant_subscription =
        prepared_subscribe(&client, &grant_query, edge_subscribe_opts()).unwrap();
    for _ in 0..16 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        if !prepared_all(&client, &grant_query, edge_subscribe_opts()).is_empty() {
            break;
        }
    }
    if indexed {
        assert_eq!(
            server
                .node()
                .borrow()
                .query_engine_read_metrics()
                .source_index_probes,
            0,
            "the disjunctive policy must retain a complete source path",
        );
    }
    assert_eq!(
        prepared_all(&client, &grant_query, edge_subscribe_opts()).len(),
        1
    );
    while grant_subscription.try_next_event().is_some() {}

    let workspace_query =
        Query::from("workspaces").filter(eq(col("id"), lit(Value::Uuid(workspace.row_uuid().0))));
    let mut workspace_subscription =
        prepared_subscribe(&client, &workspace_query, edge_subscribe_opts()).unwrap();
    for _ in 0..16 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        if !prepared_all(&client, &workspace_query, edge_subscribe_opts()).is_empty() {
            break;
        }
    }
    assert_eq!(
        prepared_all(&client, &workspace_query, edge_subscribe_opts())
            .iter()
            .map(CurrentRow::row_uuid)
            .collect::<Vec<_>>(),
        vec![workspace.row_uuid()],
    );
    assert!(workspace_subscription.try_next_event().is_some());
}

/// Covers the normal source layout after the self-membership subscription has
/// already delivered workspace policy support to the client.
#[test]
fn db_sync_surface_membership_grant_then_parent_query_keeps_disjunctive_read_proof() {
    membership_grant_then_parent_query_keeps_disjunctive_read_proof(false);
}

/// Covers the indexed layout, where a disjunctive proof must still retain the
/// complete source path instead of selecting one arm's index for the union.
#[test]
fn db_sync_surface_indexed_membership_grant_then_parent_query_keeps_disjunctive_read_proof() {
    membership_grant_then_parent_query_keeps_disjunctive_read_proof(true);
}

/// A prepared trusted-serving read binds each request session's text `user_id`
/// independently: Alice receives her seeded message while Bob receives none.
///
/// ```text
/// system ──seed owner_id=alice──► server prepared read
///                                      │
///                         Alice session ─┼──► [alice message]
///                           Bob session ─└──► []
/// ```
#[test]
fn prepared_server_read_binds_text_session_user_id_per_session() {
    // Mirror the public test app: a nullable camel-case `ownerId` grants to
    // its matching session or to every session when unowned. In particular,
    // this exercises the disjunctive policy plan rather than only the
    // scalar-equality fast path.
    let read_policy = PublicPolicyExpr::or(vec![
        public_session_eq("ownerId", &["claims", "sub"]),
        PublicPolicyExpr::IsNull {
            column: "ownerId".to_owned(),
        },
    ]);
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .nullable_column("ownerId", PublicColumnType::Text)
                .policies(PublicTablePolicies::new().with_select(read_policy)),
        ),
    );
    let server = open_db(0x5e, AuthorSubject::SYSTEM, &schema);
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let alice_subject = "alice-session-subject";
    let bob_subject = "bob-session-subject";
    server.set_test_provider_claims(
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice_subject.into()),
        )]),
    );
    server.set_test_provider_claims(
        bob,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(bob_subject.into()),
        )]),
    );

    let seeded = server
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("for alice".to_owned())),
                ("done".to_owned(), Value::Bool(false)),
                (
                    "ownerId".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String(alice_subject.into())))),
                ),
            ]),
            Default::default(),
        )
        .expect("system seed must write the protected message");
    block_on(seeded.wait(DurabilityTier::Local)).expect("seed must settle locally");

    // The public `where({ id })` facade contributes an ordinary prepared
    // parameter alongside the hidden policy claim. Keep that mixed binding in
    // this regression so the descriptor cannot accidentally bind Alice's
    // claim into the query-id slot (or vice versa).
    let query = Query::from("todos").filter(eq(col("id"), lit(Value::Uuid(seeded.row_uuid().0))));
    let prepared = prepared(&server, &query);
    let alice_rows = block_on(server.all_for_identity(&prepared, ReadOpts::default(), alice))
        .expect("Alice's prepared read must evaluate against her session claims");
    let bob_rows = block_on(server.all_for_identity(&prepared, ReadOpts::default(), bob))
        .expect("Bob's prepared read must evaluate against his session claims");

    assert_eq!(row_ids(&alice_rows), vec![seeded.row_uuid()]);
    assert!(bob_rows.is_empty());
}

#[test]
fn db_sync_surface_edge_session_read_policy_filters_after_runtime_schema_publish() {
    let public_schema = owner_id_public_schema();
    let permission_schema = owner_id_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &public_schema);
    let writer = open_db(0xa1, alice, &permission_schema);
    let alice_reader = open_db(0xa2, alice, &permission_schema);
    let reader = open_db(0xb2, bob, &permission_schema);

    let schema_version = SchemaVersion::new(permission_schema.clone());
    let schema_id = schema_version.id;
    let acks = server.publish_schema(schema_version).unwrap();
    assert!(acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));
    let current_acks = server
        .server
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema_id,
            },
        })
        .unwrap();
    assert!(current_acks.into_iter().any(|message| matches!(
        message,
        SyncMessage::CatalogueAck(CatalogueAck {
            applied: true,
            schema: Some(applied_schema),
            ..
        }) if applied_schema == schema_id
    )));

    let (writer_transport, server_writer_transport) = duplex();
    let _writer_upstream = crate::db::block_on(writer.connect_upstream(writer_transport));
    let _writer_subscriber = server.accept_subscriber_with_claims(
        server_writer_transport,
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    writer
        .insert(
            "messages",
            BTreeMap::from([
                ("body".to_owned(), Value::String("alice private".to_owned())),
                (
                    "owner_id".to_owned(),
                    Value::String(alice.test_uuid().to_string()),
                ),
            ]),
            Default::default(),
        )
        .unwrap();
    writer.tick().unwrap();
    server.tick().unwrap();

    let (alice_transport, server_alice_transport) = duplex();
    let _alice_upstream = crate::db::block_on(alice_reader.connect_upstream(alice_transport));
    let _alice_subscriber = server.accept_subscriber_with_claims(
        server_alice_transport,
        alice,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(alice.test_uuid().to_string()),
        )]),
    );
    let query = Query::from("messages");
    let mut alice_subscription =
        prepared_subscribe(&alice_reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(alice_subscription.next_raw()).unwrap()).is_empty());
    alice_reader.tick().unwrap();
    server.tick().unwrap();
    alice_reader.tick().unwrap();
    let (added, updated, removed) = delta_rows(block_on(alice_subscription.next_raw()).unwrap());
    assert_eq!(
        added.len(),
        1,
        "Alice's matching text session claim must read the seeded row"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(
        row_ids(&prepared_all(&alice_reader, &query, edge_subscribe_opts())),
        vec![added[0].row_uuid()],
    );

    let (reader_transport, server_reader_transport) = duplex();
    let _reader_upstream = crate::db::block_on(reader.connect_upstream(reader_transport));
    let _reader_subscriber = server.accept_subscriber_with_claims(
        server_reader_transport,
        bob,
        BTreeMap::from([(
            crate::query::provider_claim_key("sub"),
            Value::String(bob.test_uuid().to_string()),
        )]),
    );
    let mut subscription = prepared_subscribe(&reader, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    assert!(prepared_all(&reader, &query, edge_subscribe_opts()).is_empty());
}

#[test]
fn detached_subscriber_is_not_served_on_server_tick() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();

    assert!(server.server.detach_connection(&subscriber));
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(prepared_read(&client, &query).is_empty());
}

#[test]
fn byte_wire_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    let (client_bytes, server_bytes) = byte_duplex_raw();
    let server_inbound = Rc::clone(&server_bytes.inbound);
    let _upstream = crate::db::block_on(
        client.connect_upstream(Box::new(WireTransportAdapter::current(client_bytes))),
    );
    let _subscriber = server.accept_subscriber(
        Box::new(WireTransportAdapter::current(server_bytes)),
        client_author,
    );

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    {
        let queued = server_inbound.borrow();
        let first = queued.front().expect("register shape frame");
        let second = queued.get(1).expect("subscribe frame");
        let mut decoder = WireStreamDecoder::new(current_wire_features()).unwrap();
        let first = match decode_frame(first).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let second = match decode_frame(second).unwrap() {
            WireFrame::Message(envelope) => decode_wire_message_payload(&mut decoder, &envelope),
            other => panic!("expected message frame, got {other:?}"),
        };
        let SyncMessage::RegisterShape { shape_id, .. } = first else {
            panic!("expected RegisterShape, got {first:?}");
        };
        let SyncMessage::Subscribe(subscribe) = second else {
            panic!("expected Subscribe, got {second:?}");
        };
        assert_eq!(subscribe.shape_id, shape_id);
        assert_eq!(subscribe.subscription.shape_id, shape_id);
    }
    server.tick().unwrap();
    client.tick().unwrap();

    let table = &schema.tables[0];
    let rows = prepared_read(&client, &query);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("from server".to_owned()))
    );
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn single_upstream_tick_applies_multiple_subscription_updates() {
    let schema = issue_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    let project = row(1);
    server
        .insert_with_id(
            "projects",
            project,
            BTreeMap::from([("name".to_owned(), Value::String("Platform".to_owned()))]),
        )
        .unwrap();
    seed(
        &server,
        "issues",
        issue_cells("API", "open", owner, project, 5, &["api"], None),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let projects = Query::from("projects");
    let issues = Query::from("issues");
    let mut project_subscription =
        prepared_subscribe(&client, &projects, global_subscribe_opts()).unwrap();
    let mut issue_subscription =
        prepared_subscribe(&client, &issues, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(project_subscription.next_raw()).unwrap()).is_empty());
    assert!(opened_rows(block_on(issue_subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    let stats = client.tick_stats().unwrap();

    assert_eq!(prepared_read(&client, &projects).len(), 1);
    assert_eq!(prepared_read(&client, &issues).len(), 1);
    assert_eq!(stats.subscription_events, 2);
    assert_eq!(
        delta_rows(block_on(project_subscription.next_raw()).unwrap())
            .0
            .len(),
        1
    );
    assert_eq!(
        delta_rows(block_on(issue_subscription.next_raw()).unwrap())
            .0
            .len(),
        1
    );
}

#[test]
fn subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    // The subscriber registers the whole-table query shape; explicit
    // current-row serving then sends the facade-level initial snapshot.
    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = duplex();
    let _resumed_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= full_bytes,
        "resume catch-up should stay bounded by the initial full response: full={full_bytes}, resume={resume_bytes}"
    );
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn byte_wire_subscriber_connection_serves_current_rows_and_resumes_from_cursor() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 1);
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(added.len(), 2);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    let full_bytes = subscriber.borrow().last_resume_bytes().unwrap();
    assert!(full_bytes > 0);

    server.tick().unwrap();
    client.tick().unwrap();

    let third = seed(&server, "todos", cells("third", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 3);

    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    let (client_transport, server_transport) = byte_duplex_with_session(client_author, 2);
    let _resumed_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let resumed = server.accept_subscriber_with_resume(server_transport, client_author, cursor);

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let resume_bytes = resumed.borrow().last_resume_bytes().unwrap();
    assert!(
        resume_bytes > 0,
        "byte-wire resume catch-up should send a bounded non-empty response after cursor resume"
    );
    assert!(
        resume_bytes <= full_bytes,
        "byte-wire resume catch-up should stay bounded by the initial full response: full={full_bytes}, resume={resume_bytes}"
    );
    assert_eq!(prepared_read(&client, &query).len(), 3);
    assert!(
        prepared_read(&client, &query)
            .iter()
            .any(|row| row.row_uuid() == third)
    );
}

#[test]
fn connect_upstream_announces_existing_subscriptions_on_first_tick() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    client.tick().unwrap();
    let first = upstream_transport.try_recv().unwrap();
    let second = upstream_transport.try_recv().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    let SyncMessage::RegisterShape { shape_id, .. } = first else {
        panic!("expected existing subscription shape to be registered upstream first");
    };
    let SyncMessage::Subscribe(subscribe) = second else {
        panic!("expected existing subscription to be announced upstream second");
    };
    assert_eq!(subscribe.shape_id, shape_id);
    assert_eq!(subscribe.subscription.shape_id, shape_id);
}

/// This is intentionally an internal lifecycle test: the public symptom is a
/// binding panic, but reproducing its ordering requires holding the exact node
/// state that an interruptible evaluation or hydration operation owns.
#[test]
fn connect_upstream_waits_for_active_node_state_borrow() {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};

    let schema = schema();
    let client = open_db(0xc1, AuthorSubject::for_test_bytes([0xc1; 16]), &schema);
    let node = client.node.node();
    let held_node = crate::db::block_on(node.lock());
    let (client_transport, _server_transport) = duplex();
    let mut connection = pin!(client.connect_upstream(client_transport));
    let waker = Waker::noop();
    let mut cx = Context::from_waker(waker);

    assert!(matches!(connection.as_mut().poll(&mut cx), Poll::Pending));
    drop(held_node);
    let _connection = crate::db::block_on(connection);
}

// SessionClaims has no distinct public state once the receiving NodeState has
// ignored an identical map, so wire-count coverage must inspect the transport.
// The policy-visible integration coverage lives above this facade; this test
// protects the otherwise unobservable wire-chatter contract.
#[test]
fn repeated_identical_session_claims_emit_once_on_a_live_connection() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    client.set_test_provider_claims(client_author, claims.clone());
    client.set_test_provider_claims(client_author, claims);
    client.tick().unwrap();

    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { .. })
    ));
    assert!(
        upstream_transport.try_recv().is_none(),
        "an unchanged claim map must not produce another wire message"
    );
}

// This is lower-level for the same reason as the wire-count test above. In
// particular, it is the regression that a global deduplication would miss:
// each newly attached transport must receive the current map independently.
#[test]
fn current_session_claims_reach_late_and_reconnected_upstreams() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let claims = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    let admitted_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("reader".to_owned()),
    )]);

    client.set_test_provider_claims(client_author, claims.clone());
    let (first_transport, mut first_upstream_transport) = duplex();
    let first_upstream = crate::db::block_on(client.connect_upstream(first_transport));
    client.tick().unwrap();
    assert!(matches!(
        first_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == admitted_claims
    ));
    assert!(first_upstream_transport.try_recv().is_none());

    client.set_test_provider_claims(client_author, claims.clone());
    assert!(client.detach_connection(&first_upstream));

    let (reconnected_transport, mut reconnected_upstream_transport) = duplex();
    let _reconnected_upstream = crate::db::block_on(client.connect_upstream(reconnected_transport));
    client.tick().unwrap();
    assert!(matches!(
        reconnected_upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims: received })
            if identity == client_author && received == admitted_claims
    ));
    assert!(reconnected_upstream_transport.try_recv().is_none());
}

#[test]
fn changed_session_claims_advance_delivery_after_an_identical_call() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let reader = BTreeMap::from([("role".to_owned(), Value::String("reader".to_owned()))]);
    let writer = BTreeMap::from([("role".to_owned(), Value::String("writer".to_owned()))]);
    let reader_admitted = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("reader".to_owned()),
    )]);
    let writer_admitted = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("writer".to_owned()),
    )]);

    client.set_test_provider_claims(client_author, reader.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { claims, .. }) if claims == reader_admitted
    ));

    client.set_test_provider_claims(client_author, reader);
    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());

    client.set_test_provider_claims(client_author, writer.clone());
    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::SessionClaims { identity, claims })
            if identity == client_author && claims == writer_admitted
    ));
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn global_subscription_registers_array_subquery_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
}

#[test]
fn array_subquery_attachment_registers_upstream_coverage() {
    let schema = relation_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let query = Query::from("users").array_subquery(
        ArraySubquery::new("todos", "todos", "owner_id", "id")
            .nested(ArraySubquery::new("comments", "comments", "todo_id", "id")),
    );
    let prepared = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();

    client.tick().unwrap();
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::RegisterShape { .. })
    ));
    assert!(matches!(
        upstream_transport.try_recv(),
        Some(SyncMessage::Subscribe(_))
    ));
    client.detach_query(attachment);
}

#[test]
fn upload_is_not_marked_sent_after_one_shot_backpressure_and_retries() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let outbound = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let transport = BackpressureOnceTransport {
        outbound: Rc::clone(&outbound),
        failed: false,
    };
    let _upstream = crate::db::block_on(client.connect_upstream(Box::new(transport)));

    let tx_id = client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0xf1), client.next_now_ms())
                .made_by(client_author)
                .permission_subject(client_author)
                .cells(cells("retry", false, client_author)),
        )
        .unwrap();
    assert!(
        client
            .node
            .outbox
            .borrow_mut()
            .push(PendingUpload { tx_id, unit: None }),
        "test setup queues the retry upload once"
    );

    client.tick().unwrap();
    assert!(outbound.borrow().is_empty());
    assert_eq!(
        client
            .node
            .node
            .borrow()
            .sync_metrics()
            .transport_backpressure_retries,
        1
    );

    client.tick().unwrap();
    let sent = outbound.borrow_mut().pop_front().unwrap();
    let SyncMessage::CommitUnit { tx, .. } = sent else {
        panic!("expected retried commit upload");
    };
    assert_eq!(tx.tx_id, tx_id);
    assert!(outbound.borrow_mut().pop_front().is_none());
}

/// A terminal authority rejection releases its upload from the shared outbox,
/// so reconnecting the client cannot replay a transaction whose user-visible
/// outcome is already final.
///
/// writer ──CommitUnit──► authority
/// writer ◄─rejected fate── authority
/// writer ──reconnect──► replacement authority (no replay)
#[test]
fn rejected_upload_is_not_replayed_after_reconnect() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xe1; 16]);
    let client = open_db(0xe1, author, &schema);
    let (client_transport, mut authority_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let write = client
        .insert(
            "todos",
            cells("rejected", false, author),
            Default::default(),
        )
        .expect("stage local upload");
    client.tick().expect("send initial upload");
    let uploaded = std::iter::from_fn(|| authority_transport.try_recv()).find_map(|message| {
        matches!(message, SyncMessage::CommitUnit { ref tx, .. } if tx.tx_id == write.mergeable_tx_id())
            .then_some(message)
    });
    assert!(
        uploaded.is_some(),
        "authority receives the staged upload once"
    );

    authority_transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: None,
        })
        .expect("return terminal rejection");
    client.tick().expect("apply terminal rejection");
    let rejected = crate::db::block_on(write.wait(DurabilityTier::Global))
        .expect_err("rejected upload stays terminal");
    assert_eq!(rejected.code, ErrorCode::WriteRejected);

    assert!(client.detach_connection(&upstream));
    let (reconnected_transport, mut replacement_authority) = duplex();
    let _reconnected = crate::db::block_on(client.connect_upstream(reconnected_transport));
    client.tick().expect("tick replacement connection");
    assert!(
        std::iter::from_fn(|| replacement_authority.try_recv()).all(
            |message| !matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == write.mergeable_tx_id())
        ),
        "replacement authority must not replay a terminally rejected upload"
    );
}

/// Each upstream owns an independent upload cursor.  A fate cleanup can make
/// one cursor non-contiguous relative to the shared oldest-first outbox; that
/// link must fall back to the complete set difference rather than treating its
/// newest uploaded entry as proof that the missing middle entry was sent.
///
/// upstream A: [first, middle, last]
/// upstream B: [first,      -, last] ──tick──► [middle]
#[test]
fn upload_cursor_hole_replays_only_the_missing_entry_on_that_upstream() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xe2; 16]);
    let client = open_db(0xe2, author, &schema);
    let (first_transport, mut first_authority) = duplex();
    let _first = crate::db::block_on(client.connect_upstream(first_transport));
    let (second_transport, mut second_authority) = duplex();
    let _second = crate::db::block_on(client.connect_upstream(second_transport));

    let writes = ["first", "middle", "last"]
        .into_iter()
        .map(|title| {
            client
                .insert("todos", cells(title, false, author), Default::default())
                .expect("stage upload")
        })
        .collect::<Vec<_>>();
    let tx_ids = writes
        .iter()
        .map(|write| write.mergeable_tx_id())
        .collect::<Vec<_>>();

    client
        .tick()
        .expect("send every new entry to both upstreams");
    for authority in [&mut first_authority, &mut second_authority] {
        let sent = std::iter::from_fn(|| authority.try_recv())
            .filter_map(|message| match message {
                SyncMessage::CommitUnit { tx, .. } => Some(tx.tx_id),
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(sent, tx_ids, "initial cursor is contiguous per upstream");
    }

    let connections = client.node.connections.borrow().clone();
    assert_eq!(
        connections.len(),
        2,
        "fixture attached two independent links"
    );
    let mut second = crate::db::block_on(connections[1].lock());
    let crate::db::peer_connection::ConnectionLink::Upstream(state) = &mut second.link else {
        panic!("second fixture link is upstream");
    };
    assert!(
        state.uploaded.remove(&tx_ids[1]),
        "plant one middle cursor hole while retaining both surrounding receipts"
    );
    drop(second);

    client
        .tick()
        .expect("conservatively repair the cursor hole");
    assert!(
        std::iter::from_fn(|| first_authority.try_recv())
            .all(|message| !matches!(message, SyncMessage::CommitUnit { .. })),
        "the independent complete cursor must not duplicate uploads"
    );
    let repaired = std::iter::from_fn(|| second_authority.try_recv())
        .filter_map(|message| match message {
            SyncMessage::CommitUnit { tx, .. } => Some(tx.tx_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(
        repaired,
        vec![tx_ids[1]],
        "the hole fallback sends precisely the missing middle upload once"
    );
}

/// Upload entries remain replayable until an applied terminal fate either
/// rejects them or reaches Global durability.  An Accepted fate at Local or
/// Edge is only progress: reconnect must resend it until a later Global fate
/// releases the shared outbox entry.
#[test]
fn accepted_upload_releases_outbox_only_after_global_durability() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xe3; 16]);
    let client = open_db(0xe3, author, &schema);
    let (client_transport, mut authority) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let write = client
        .insert(
            "todos",
            cells("accepted in stages", false, author),
            Default::default(),
        )
        .expect("stage upload");
    let tx_id = write.mergeable_tx_id();

    assert!(
        client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "the initial Pending/retryable upload remains in the outbox"
    );

    client.tick().expect("send initial upload");
    assert!(
        std::iter::from_fn(|| authority.try_recv()).any(
            |message| matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id)
        )
    );
    for durability in [DurabilityTier::Local, DurabilityTier::Edge] {
        authority
            .send(SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Accepted,
                global_time: None,
                durability: Some(durability),
            })
            .expect("return non-global acceptance");
        client.tick().expect("apply non-global acceptance");
        assert!(
            client
                .node
                .outbox
                .borrow()
                .iter()
                .any(|pending| pending.tx_id == tx_id),
            "{durability:?} acceptance is not terminal for upload replay"
        );
    }

    authority
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .expect("return global acceptance");
    client.tick().expect("apply terminal global acceptance");
    assert!(
        !client
            .node
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "Global acceptance releases the upload from the shared outbox"
    );
}

#[test]
fn local_missing_upload_body_still_kills_sync_driver() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, _server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let missing_tx = TxId::new(
        crate::time::TxTime::from(client.next_now_ms()),
        NodeUuid::from_bytes([0xee; 16]),
    );
    assert!(
        client.node.outbox.borrow_mut().push(PendingUpload {
            tx_id: missing_tx,
            unit: None,
        }),
        "test setup queues the missing upload once"
    );

    let error = client.tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        error.message.contains("missing transaction"),
        "unexpected local-fatal error: {}",
        error.message
    );
}

#[test]
fn detach_connection_removes_connection_from_db_ticks() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, mut upstream_transport) = duplex();

    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let _subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));

    assert!(client.detach_connection(&upstream));
    assert!(!client.detach_connection(&upstream));

    client.tick().unwrap();
    assert!(upstream_transport.try_recv().is_none());
}

#[test]
fn accepted_subscriber_is_served_under_subscriber_author_identity() {
    let schema = owner_read_schema();
    let subscriber_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server_author = AuthorSubject::for_test_bytes([0x5e; 16]);
    let other_author = AuthorSubject::for_test_bytes([0xd1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, subscriber_author, &schema);

    let visible = seed(
        &server,
        "todos",
        cells("for subscriber", false, subscriber_author),
    );
    seed(&server, "todos", cells("for server", false, server_author));
    seed(
        &server,
        "todos",
        cells("for someone else", false, other_author),
    );

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, subscriber_author);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let (rows, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
    assert_eq!(row_ids(&rows), vec![visible]);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("for subscriber".to_owned()))
    );
}

#[test]
fn client_initial_sync_flush_cadence_preserves_public_snapshot_delivery() {
    let schema = schema();
    let server = open_core(0xd4, AuthorSubject::SYSTEM, &schema);
    for ordinal in 0..3_u8 {
        server
            .insert_with_id(
                "todos",
                row(0xd0 + ordinal),
                BTreeMap::from([
                    (
                        "title".to_owned(),
                        Value::String(format!("server {ordinal}")),
                    ),
                    ("done".to_owned(), Value::Bool(false)),
                ]),
            )
            .unwrap();
    }

    let client_author = AuthorSubject::for_test_bytes([0xd5; 16]);
    let client = open_db(0xd5, client_author, &schema);
    client
        .set_initial_sync_flush_cadence(InitialSyncFlushCadence::every(
            NonZeroUsize::new(2).unwrap(),
        ))
        .unwrap();
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = client.table("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();

    for _ in 0..20 {
        client.tick().unwrap();
        server.server.tick().unwrap();
        client.tick().unwrap();
        if let Some(event) = subscription.try_next_event()
            && opened_rows(event).len() == 3
        {
            return;
        }
    }
    panic!("client configured with a cadence must receive the initial snapshot");
}
