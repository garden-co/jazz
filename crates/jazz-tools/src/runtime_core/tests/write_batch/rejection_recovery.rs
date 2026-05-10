use super::*;

fn users_delete_denied_authorization_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                        .with_delete(PolicyExpr::False),
                ),
        )
        .build()
}

#[test]
fn rc_direct_insert_persisted_reconnect_reconciles_rejected_batch_from_server() {
    let mut core = create_runtime_with_boxed_storage(
        test_schema(),
        "direct-reject-replay-test",
        Box::new(RowRegionReadFailingStorage::with_row_locator_scan_failure()),
    );

    let ((row_id, _row_values), mut receiver) = core
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = core.schema_manager().branch_name();
    let batch_id = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("persisted direct insert should materialize a visible row")
        .batch_id;

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Rejected {
                batch_id,
                code: "permission_denied".to_string(),
                reason: "writer lacks publish rights".to_string(),
            },
        },
    });
    core.immediate_tick();

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Err(crate::runtime_core::PersistedWriteRejection {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        }))),
        "replayed direct-batch rejections should resolve persisted waits"
    );
    assert_eq!(
        core.drain_rejected_batch_ids(),
        vec![batch_id],
        "rejected batch ids should be surfaced once for bindings"
    );
    assert!(
        core.drain_rejected_batch_ids().is_empty(),
        "draining rejected batch ids should clear the queue"
    );
    assert_eq!(
        core.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        })
    );
    assert_eq!(
        core.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap(),
        None,
        "replayed direct-batch rejection should retract the optimistic visible row"
    );
    assert_eq!(
        core.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()[0]
            .state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn rc_direct_update_rejection_restores_previous_visible_row() {
    let mut core = create_runtime_with_schema(test_schema(), "direct-update-reject-restore-test");

    let ((row_id, _row_values), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.update(
        row_id,
        vec![("name".into(), Value::Text("Bob".into()))],
        None,
    )
    .unwrap();

    let branch_name = core.schema_manager().branch_name();
    let update_batch_id = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("update should be visible optimistically")
        .batch_id;

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Rejected {
                batch_id: update_batch_id,
                code: "permission_denied".to_string(),
                reason: "writer lost update rights".to_string(),
            },
        },
    });
    core.immediate_tick();

    let visible = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("rejecting an update should restore the previous visible row");
    assert_ne!(visible.batch_id, update_batch_id);

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Alice".into()));

    core.update(
        row_id,
        vec![("name".into(), Value::Text("Carol".into()))],
        None,
    )
    .expect("restored rows should remain writable after a rejected update");
}

#[test]
fn rc_direct_delete_rejection_restores_previous_visible_row() {
    let mut core = create_runtime_with_schema(test_schema(), "direct-delete-reject-restore-test");

    let ((row_id, _row_values), _) = core
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();
    core.delete(row_id, None).unwrap();

    let branch_name = core.schema_manager().branch_name();
    let delete_batch_id = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("delete should be visible optimistically")
        .batch_id;

    core.push_sync_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Rejected {
                batch_id: delete_batch_id,
                code: "permission_denied".to_string(),
                reason: "writer lost delete rights".to_string(),
            },
        },
    });
    core.immediate_tick();

    let visible = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("rejecting a delete should restore the previous visible row");
    assert_ne!(visible.batch_id, delete_batch_id);

    let query = Query::new("users");
    let results = execute_query(&mut core, query);
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[1], Value::Text("Alice".into()));

    core.update(
        row_id,
        vec![("name".into(), Value::Text("Carol".into()))],
        None,
    )
    .expect("restored rows should remain writable after a rejected delete");
}

#[test]
fn rc_worker_peer_relays_rejected_batch_fate_to_downstream_peer() {
    let mut s = create_3tier_rc();

    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();

    let batch_id =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()[0]
            .batch_id;
    let branch_name = s.a.schema_manager().branch_name();

    pump_a_to_b(&mut s);

    s.b.push_sync_inbox(InboxEntry {
        source: Source::Server(s.c_server_for_b),
        payload: SyncPayload::BatchFate {
            fate: crate::batch_fate::BatchFate::Rejected {
                batch_id,
                code: "permission_denied".to_string(),
                reason: "writer lacks publish rights".to_string(),
            },
        },
    });
    s.b.immediate_tick();
    pump_b_to_a(&mut s);

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Err(crate::runtime_core::PersistedWriteRejection {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        }))),
        "worker peers should relay rejected settlements back to downstream persisted waits"
    );
    assert_eq!(
        s.a.storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap(),
        None,
        "downstream peer should retract the optimistic visible row after rejection"
    );
}

#[test]
fn rc_worker_peer_retains_replayable_batch_record_for_downstream_direct_write() {
    let mut s = create_3tier_rc();

    let ((row_id, _row_values), _receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();

    let batch_id =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()[0]
            .batch_id;

    pump_a_to_b(&mut s);

    assert!(
        s.b.storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap()
            .is_some()
            || s.b
                .storage()
                .load_authoritative_batch_fate(batch_id)
                .unwrap()
                .is_some(),
        "worker peers should retain sealed/fate state for downstream direct writes"
    );
}

#[test]
fn rc_transactional_insert_persisted_reconnect_reconciles_rejected_batch_from_server() {
    // alice -> worker
    //   alice stages one transactional batch locally
    //   worker has a durable rejection record for that batch
    //   reconnect must reconcile the rejection without any visible row replay
    let mut s = create_3tier_rc();
    let write_context = WriteContext {
        session: None,
        attribution: None,
        updated_at: None,
        batch_mode: Some(crate::batch_fate::BatchMode::Transactional),
        batch_id: None,
        target_branch_name: None,
    };

    s.a.remove_server(s.b_server_for_a);

    let ((row_id, _row_values), mut receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;
    s.a.seal_batch(batch_id).unwrap();

    s.b.storage_mut()
        .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        })
        .unwrap();

    s.a.add_server(s.b_server_for_a);
    pump_a_to_b(&mut s);
    pump_b_to_a(&mut s);

    assert_eq!(
        s.a.storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        })
    );

    assert_eq!(
        receiver.try_recv(),
        Ok(Some(Err(crate::runtime_core::PersistedWriteRejection {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        }))),
        "rejections should resolve durability waiters with a terminal rejection"
    );
}

#[test]
fn rc_direct_insert_persisted_is_rejected_by_authority_permission_check() {
    let schema = test_schema();
    let mut alice = create_runtime_with_schema(schema.clone(), "direct-reject-test");
    let mut worker = create_runtime_with_schema_and_sync_manager(
        schema,
        "direct-reject-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(users_insert_denied_authorization_schema());

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    worker.add_client(client_id, Some(alice_session.clone()));
    alice.add_server(server_id);
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::User);

    alice.batched_tick();
    worker.batched_tick();
    alice.sync_sender().take();
    worker.sync_sender().take();

    let write_context = WriteContext::from_session(alice_session);
    let ((row_id, _row_values), mut receiver) = alice
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let batch_id = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap()[0]
        .batch_id;
    let branch_name = alice.schema_manager().branch_name();

    pump_client_messages_to_server(&mut alice, &mut worker, server_id, client_id);

    let worker_outbox = worker.sync_sender().take();
    assert!(
        worker_outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate: crate::batch_fate::BatchFate::Rejected { batch_id: settled_batch_id, .. },
                },
            } if *id == client_id && *settled_batch_id == batch_id
        )),
        "direct permission denials should be replayed as rejected batch settlements"
    );
    assert!(
        !worker_outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::Error(SyncError::PermissionDenied { .. }),
            } if *id == client_id
        )),
        "direct permission denials should not fall back to the non-replayable error path"
    );

    for entry in worker_outbox {
        if entry.destination == Destination::Client(client_id) {
            alice.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }
    alice.batched_tick();

    match receiver.try_recv() {
        Ok(Some(Err(rejection))) => {
            assert_eq!(rejection.batch_id, batch_id);
            assert_eq!(rejection.code, "permission_denied");
            assert!(
                rejection.reason.contains("denied"),
                "unexpected direct rejection reason: {}",
                rejection.reason
            );
        }
        other => panic!(
            "live direct permission denials should resolve persisted waits with a replayable rejection, got {other:?}"
        ),
    }
    assert!(matches!(
        alice
            .storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected {
            batch_id: settled_batch_id,
            code,
            reason,
        }) if settled_batch_id == batch_id
            && code == "permission_denied"
            && reason.contains("denied")
    ));
    assert_eq!(
        alice
            .storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap(),
        None,
        "live direct permission denials should retract the optimistic visible row"
    );
    assert_eq!(
        alice
            .storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()[0]
            .state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn rc_direct_insert_persisted_is_rejected_without_permissions_head() {
    let schema = test_schema();
    let mut alice = create_runtime_with_schema(schema.clone(), "direct-missing-permissions-head");
    let mut worker = create_runtime_with_schema_and_sync_manager(
        schema,
        "direct-missing-permissions-head",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .require_authorization_schema();

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    worker.add_client(client_id, Some(alice_session.clone()));
    alice.add_server(server_id);
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::User);

    alice.batched_tick();
    worker.batched_tick();
    alice.sync_sender().take();
    worker.sync_sender().take();

    let write_context = WriteContext::from_session(alice_session);
    let ((row_id, _row_values), mut receiver) = alice
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let batch_id = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap()[0]
        .batch_id;
    let branch_name = alice.schema_manager().branch_name();

    pump_client_messages_to_server(&mut alice, &mut worker, server_id, client_id);

    let worker_outbox = worker.sync_sender().take();
    assert!(
        worker_outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::BatchFate { fate: crate::batch_fate::BatchFate::Rejected { batch_id: settled_batch_id, .. },
                },
            } if *id == client_id && *settled_batch_id == batch_id
        )),
        "missing permissions head should reject persisted writes as replayable batch settlements"
    );

    for entry in worker_outbox {
        if entry.destination == Destination::Client(client_id) {
            alice.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }
    alice.batched_tick();

    match receiver.try_recv() {
        Ok(Some(Err(rejection))) => {
            assert_eq!(rejection.batch_id, batch_id);
            assert_eq!(rejection.code, "permissions_head_missing");
            assert!(
                rejection.reason.contains("no published permissions head"),
                "unexpected rejection reason: {}",
                rejection.reason
            );
        }
        other => panic!(
            "missing permissions head should resolve persisted waits with a rejection, got {other:?}"
        ),
    }
    assert!(matches!(
        alice
            .storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected {
            batch_id: settled_batch_id,
            code,
            reason,
        }) if settled_batch_id == batch_id
            && code == "permissions_head_missing"
            && reason.contains("no published permissions head")
    ));
    assert_eq!(
        alice
            .storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap(),
        None,
        "missing permissions head should retract the optimistic visible row"
    );
}

#[test]
fn rc_transactional_insert_is_rejected_by_authority_permission_check() {
    // alice -> worker
    //   alice stages one transactional batch locally
    //   worker denies it during authoritative permission evaluation
    //   rejection is persisted and relayed back as replayable batch fate
    let schema = test_schema();
    let mut alice = create_runtime_with_schema(schema.clone(), "transactional-reject-test");
    let mut worker = create_runtime_with_schema_and_sync_manager(
        schema,
        "transactional-reject-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(users_insert_denied_authorization_schema());

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    worker.add_client(client_id, Some(alice_session.clone()));
    alice.add_server(server_id);
    assert_eq!(
        worker
            .schema_manager()
            .query_manager()
            .sync_manager()
            .get_client(client_id)
            .expect("alice should be registered on worker")
            .role,
        ClientRole::User,
        "test must exercise user permission evaluation rather than peer bypass"
    );

    alice.batched_tick();
    worker.batched_tick();
    alice.sync_sender().take();
    worker.sync_sender().take();

    let write_context = WriteContext::from_session(alice_session)
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional);
    let ((row_id, _row_values), _receiver) = alice
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    alice.seal_batch(batch_id).unwrap();
    pump_client_messages_to_server(&mut alice, &mut worker, server_id, client_id);

    let worker_outbox = worker.sync_sender().take();
    assert!(worker_outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchFate { fate: crate::batch_fate::BatchFate::Rejected { batch_id: settled_batch_id, .. },
            },
        } if *id == client_id && *settled_batch_id == batch_id
    )));

    for entry in worker_outbox {
        if entry.destination == Destination::Client(client_id) {
            alice.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }
    alice.batched_tick();

    let worker_settlement = worker
        .storage()
        .load_authoritative_batch_fate(batch_id)
        .unwrap()
        .expect("worker should persist the rejected settlement");
    assert!(matches!(
        &worker_settlement,
        crate::batch_fate::BatchFate::Rejected { batch_id: settled_batch_id, code, reason }
            if *settled_batch_id == batch_id
                && code == "permission_denied"
                && reason.contains("denied")
    ));

    assert!(matches!(
        alice.storage().load_authoritative_batch_fate(batch_id).unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected { batch_id: settled_batch_id, code, reason })
            if settled_batch_id == batch_id
                && code == "permission_denied"
                && reason.contains("denied")
    ));

    let alice_history_rows = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(alice_history_rows.len(), 1);
    assert_eq!(alice_history_rows[0].batch_id(), batch_id);
    assert_eq!(
        alice_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn rc_worker_path_overlapping_insert_and_delete_rejects_delete() {
    let mut s = create_3tier_rc();

    s.c.schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(users_delete_denied_authorization_schema());
    s.c.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_session(s.b_client_of_c, Session::new("alice"));
    s.c.schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(s.b_client_of_c, ClientRole::User);

    let ((row_id, _row_values), mut insert_receiver) =
        s.a.insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::EdgeServer,
        )
        .unwrap();
    let insert_batch_id =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()[0]
            .batch_id;
    let mut delete_receiver =
        s.a.delete_persisted(row_id, None, DurabilityTier::EdgeServer)
            .unwrap();
    let delete_batch_id =
        s.a.storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()
            .iter()
            .find(|row| row.batch_id != insert_batch_id)
            .expect("delete should append a second history row")
            .batch_id;

    pump_a_to_b(&mut s);

    let b_out = s.b.sync_sender().take();
    assert!(
        b_out.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(server_id),
                payload: SyncPayload::RowBatchCreated { row, .. },
            } if *server_id == s.c_server_for_b && row.batch_id == delete_batch_id && row.is_deleted
        )),
        "worker should forward the delete tombstone upstream when insert and delete overlap",
    );
    for entry in b_out {
        match &entry.destination {
            Destination::Client(cid) if *cid == s.a_client_of_b => {
                s.a.park_sync_message(InboxEntry {
                    source: Source::Server(s.b_server_for_a),
                    payload: entry.payload,
                });
            }
            Destination::Server(server_id) if *server_id == s.c_server_for_b => {
                s.c.park_sync_message(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
    s.c.batched_tick();
    s.c.immediate_tick();

    let c_out = s.c.sync_sender().take();
    assert!(
        c_out.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(client_id),
                payload: SyncPayload::BatchFate { fate: crate::batch_fate::BatchFate::Rejected { batch_id, .. },
                },
            } if *client_id == s.b_client_of_c && *batch_id == delete_batch_id
        )),
        "edge should reject the overlapping delete batch, got {c_out:?}",
    );
    for entry in c_out {
        if entry.destination == Destination::Client(s.b_client_of_c) {
            s.b.park_sync_message(InboxEntry {
                source: Source::Server(s.c_server_for_b),
                payload: entry.payload,
            });
        }
    }
    s.b.batched_tick();
    s.b.immediate_tick();
    pump_b_to_a(&mut s);

    assert_eq!(
        insert_receiver.try_recv(),
        Ok(Some(Ok(()))),
        "insert waiter should resolve once the edge accepts the base row"
    );
    let rejection = delete_receiver
        .try_recv()
        .expect("delete wait should settle after edge permission evaluation")
        .expect("delete wait should produce a settlement result")
        .expect_err("delete wait should reject under edge delete-denied policy");
    assert_eq!(rejection.code, "permission_denied");
    assert!(
        rejection.reason.contains("Delete denied"),
        "expected delete rejection reason, got {:?}",
        rejection.reason
    );
}

#[test]
fn rc_acknowledge_rejected_batch_prunes_local_batch_record() {
    // alice -> worker
    //   alice stages one transactional batch locally
    //   worker rejects it authoritatively
    //   alice acknowledges the replayable rejection
    //   the local batch record is pruned while rejected row history stays intact
    let schema = test_schema();
    let mut alice = create_runtime_with_schema(schema.clone(), "transactional-ack-reject-test");
    let mut worker = create_runtime_with_schema_and_sync_manager(
        schema,
        "transactional-ack-reject-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(users_insert_denied_authorization_schema());

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    worker.add_client(client_id, Some(alice_session.clone()));
    alice.add_server(server_id);
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::User);

    alice.batched_tick();
    worker.batched_tick();
    alice.sync_sender().take();
    worker.sync_sender().take();

    let write_context = WriteContext::from_session(alice_session)
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional);
    let ((row_id, _row_values), _receiver) = alice
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    alice.seal_batch(batch_id).unwrap();
    pump_client_messages_to_server(&mut alice, &mut worker, server_id, client_id);

    for entry in worker.sync_sender().take() {
        if entry.destination == Destination::Client(client_id) {
            alice.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }
    alice.batched_tick();

    assert!(
        matches!(
            alice
                .storage()
                .load_authoritative_batch_fate(batch_id)
                .unwrap(),
            Some(crate::batch_fate::BatchFate::Rejected { .. })
        ),
        "rejected batch fate should be persisted before acknowledgement"
    );

    assert!(
        alice.acknowledge_rejected_batch(batch_id).unwrap(),
        "first acknowledgement should prune the rejected batch record"
    );
    assert_eq!(
        alice.storage().load_local_batch_record(batch_id).unwrap(),
        None
    );
    assert!(
        matches!(
            alice
                .storage()
                .load_authoritative_batch_fate(batch_id)
                .unwrap(),
            Some(crate::batch_fate::BatchFate::Rejected { .. })
        ),
        "acknowledgement should leave the authoritative rejected fate intact"
    );
    assert!(
        alice
            .storage()
            .is_rejected_batch_fate_acknowledged(batch_id)
            .unwrap(),
        "acknowledgement should persist a replay tombstone"
    );
    assert!(
        !alice.acknowledge_rejected_batch(batch_id).unwrap(),
        "acknowledging an already-pruned batch should be a no-op"
    );

    let alice_history_rows = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(alice_history_rows.len(), 1);
    assert_eq!(
        alice_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn rc_rejected_batch_survives_restart_until_acknowledged() {
    // alice -> worker
    //   alice receives a replayable transactional rejection
    //   restart preserves that rejected batch record
    //   acknowledgement after restart prunes the replayable records
    let schema = test_schema();
    let mut alice = create_runtime_with_schema(schema.clone(), "transactional-restart-reject-test");
    let mut worker = create_runtime_with_schema_and_sync_manager(
        schema.clone(),
        "transactional-restart-reject-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .set_authorization_schema(users_insert_denied_authorization_schema());

    let alice_session = Session::new("alice");
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    worker.add_client(client_id, Some(alice_session.clone()));
    alice.add_server(server_id);
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::User);

    alice.batched_tick();
    worker.batched_tick();
    alice.sync_sender().take();
    worker.sync_sender().take();

    let write_context = WriteContext::from_session(alice_session)
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional);
    let ((row_id, _row_values), _receiver) = alice
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows = alice
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;

    alice.seal_batch(batch_id).unwrap();
    pump_client_messages_to_server(&mut alice, &mut worker, server_id, client_id);

    for entry in worker.sync_sender().take() {
        if entry.destination == Destination::Client(client_id) {
            alice.park_sync_message(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
    }
    alice.batched_tick();

    let storage = alice.into_storage();
    let mut restarted =
        create_runtime_with_storage(schema.clone(), "transactional-restart-reject-test", storage);

    assert!(matches!(
        restarted
            .storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected { batch_id: settled_batch_id, .. })
            if settled_batch_id == batch_id
    ));
    assert_eq!(
        restarted.drain_rejected_batch_ids(),
        vec![batch_id],
        "restart should seed rejected batch ids from persisted rejected batch records"
    );
    assert!(
        restarted.drain_rejected_batch_ids().is_empty(),
        "draining rejected batch ids after restart should clear the seeded queue"
    );

    assert!(
        restarted.acknowledge_rejected_batch(batch_id).unwrap(),
        "restart should preserve a rejection record that can still be acknowledged"
    );
    assert_eq!(
        restarted
            .storage()
            .load_local_batch_record(batch_id)
            .unwrap(),
        None
    );
    assert!(
        matches!(
            restarted
                .storage()
                .load_authoritative_batch_fate(batch_id)
                .unwrap(),
            Some(crate::batch_fate::BatchFate::Rejected {
                batch_id: settled_batch_id,
                ..
            }) if settled_batch_id == batch_id
        ),
        "acknowledgement should preserve the authoritative rejected fate"
    );
    assert!(
        restarted
            .storage()
            .is_rejected_batch_fate_acknowledged(batch_id)
            .unwrap(),
        "acknowledgement should persist a replay tombstone"
    );

    let storage = restarted.into_storage();
    let mut restarted_again =
        create_runtime_with_storage(schema, "transactional-restart-reject-test", storage);
    assert!(
        restarted_again.drain_rejected_batch_ids().is_empty(),
        "acknowledged rejected batches must not replay after another restart"
    );

    let restarted_history_rows = restarted_again
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(restarted_history_rows.len(), 1);
    assert_eq!(
        restarted_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn rc_rejected_replay_record_can_be_synthesized_from_sealed_submission() {
    let mut core = create_runtime_with_schema(
        users_delete_denied_authorization_schema(),
        "direct-reject-replay-record-test",
    );

    let ((row_id, _row_values), _receiver) = core
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    let branch_name = core.schema_manager().branch_name();
    let batch_id = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("persisted direct insert should materialize a visible row")
        .batch_id;

    core.storage_mut()
        .delete_local_batch_record(batch_id)
        .unwrap();
    core.storage_mut()
        .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        })
        .unwrap();
    core.replay_batch_rejection(batch_id, "permission_denied", "writer lacks publish rights")
        .unwrap();

    assert_eq!(core.local_batch_record(batch_id).unwrap(), None);
    let record = core
        .local_batch_record_for_rejection_replay(batch_id)
        .unwrap()
        .expect("sealed rejected batches should remain replayable");
    assert_eq!(record.batch_id, batch_id);
    assert_eq!(record.mode, crate::batch_fate::BatchMode::Direct);
    assert!(record.sealed);
    assert!(matches!(
        record.latest_fate,
        Some(crate::batch_fate::BatchFate::Rejected { .. })
    ));
    assert_eq!(record.members.len(), 1);
    assert_eq!(record.members[0].object_id, row_id);
    assert!(record.sealed_submission.is_some());
}

#[test]
fn rc_transactional_rejected_replay_record_keeps_sealed_submission_mode() {
    let mut core = create_runtime_with_schema(
        test_schema(),
        "transactional-reject-replay-record-mode-test",
    );

    let write_context = WriteContext::from_session(Session::new("alice"))
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional);
    let ((row_id, _row_values), _receiver) = core
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            Some(&write_context),
            DurabilityTier::Local,
        )
        .unwrap();

    let history_rows = core
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(history_rows.len(), 1);
    let batch_id = history_rows[0].batch_id;
    core.seal_batch(batch_id).unwrap();

    core.storage_mut()
        .delete_local_batch_record(batch_id)
        .unwrap();
    core.storage_mut()
        .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "writer lacks publish rights".to_string(),
        })
        .unwrap();

    let record = core
        .local_batch_record_for_rejection_replay(batch_id)
        .unwrap()
        .expect("sealed rejected transactional batches should remain replayable");
    assert_eq!(record.batch_id, batch_id);
    assert_eq!(record.mode, crate::batch_fate::BatchMode::Transactional);
    assert!(record.sealed);
    assert_eq!(record.members.len(), 1);
    assert_eq!(record.members[0].object_id, row_id);
    assert_eq!(
        record
            .sealed_submission
            .as_ref()
            .expect("replay should retain the sealed submission")
            .mode,
        crate::batch_fate::BatchMode::Transactional
    );
}

#[test]
fn rc_worker_sync_records_include_sealed_batches_pending_edge_reconciliation() {
    let mut core = create_runtime_with_schema(
        users_delete_denied_authorization_schema(),
        "direct-pending-worker-sync-record-test",
    );

    let ((row_id, _row_values), _receiver) = core
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();
    let branch_name = core.schema_manager().branch_name();
    let batch_id = core
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("persisted direct insert should materialize a visible row")
        .batch_id;

    core.storage_mut()
        .delete_local_batch_record(batch_id)
        .unwrap();

    assert_eq!(core.local_batch_record(batch_id).unwrap(), None);
    let records = core.local_batch_records_for_worker_sync().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].batch_id, batch_id);
    assert!(matches!(
        records[0].latest_fate,
        Some(crate::batch_fate::BatchFate::DurableDirect {
            confirmed_tier: DurabilityTier::Local,
            ..
        })
    ));
    assert_eq!(records[0].members.len(), 1);
    assert_eq!(records[0].members[0].object_id, row_id);
}

#[test]
fn rc_worker_sync_records_include_local_only_fates_as_pending_markers() {
    let mut core = create_runtime_with_schema(
        users_delete_denied_authorization_schema(),
        "direct-pending-fate-worker-sync-record-test",
    );
    let batch_id = BatchId::new();

    core.storage_mut()
        .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::DurableDirect {
            batch_id,
            confirmed_tier: DurabilityTier::Local,
        })
        .unwrap();

    let records = core.local_batch_records_for_worker_sync().unwrap();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].batch_id, batch_id);
    assert!(records[0].members.is_empty());
    assert!(matches!(
        records[0].latest_fate,
        Some(crate::batch_fate::BatchFate::DurableDirect {
            confirmed_tier: DurabilityTier::Local,
            ..
        })
    ));
}

#[test]
fn rc_worker_accepts_local_batch_replay_payloads_from_peer() {
    let schema = test_schema();
    let mut main = create_runtime_with_schema(schema.clone(), "worker-local-replay-test");
    let mut worker = create_runtime_with_schema_and_sync_manager(
        schema,
        "worker-local-replay-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    let client_id = ClientId::new();
    worker.add_client(client_id, None);
    worker
        .schema_manager_mut()
        .query_manager_mut()
        .sync_manager_mut()
        .set_client_role(client_id, ClientRole::Peer);

    let ((row_id, _row_values), batch_id) = main
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    let payloads = main.local_batch_replay_payloads(batch_id);
    assert_eq!(payloads.len(), 2);

    for payload in payloads {
        worker.park_sync_message(InboxEntry {
            source: Source::Client(client_id),
            payload,
        });
    }
    worker.batched_tick();
    worker.immediate_tick();

    let records = worker.local_batch_records_for_worker_sync().unwrap();
    assert!(
        records
            .iter()
            .any(|record| record.batch_id == batch_id && record.members.len() == 1),
        "worker should retain memberful batch record after local replay; records={records:?}"
    );
    assert!(
        worker
            .storage()
            .scan_history_row_batches("users", row_id)
            .unwrap()
            .iter()
            .any(|row| row.batch_id == batch_id),
        "worker should persist replayed row history"
    );
}

#[test]
fn rc_restart_retracts_visible_rows_with_stored_rejected_settlement() {
    // Simulates a crash between "rejection settlement persisted" and
    // "visible row deleted + query retracted": the local batch record has a
    // Rejected settlement but its visible row is still in the visible
    // region. On restart the runtime must retract the lingering visible row
    // so queries never emit it — otherwise the row would render on reload
    // and then be retracted by the next network-delivered re-rejection,
    // causing a visible flash.
    let schema = test_schema();
    let mut alice =
        create_runtime_with_schema(schema.clone(), "rc-restart-apply-stored-rejected-test");

    let ((row_id, _row_values), _receiver) = alice
        .insert_persisted(
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
            DurabilityTier::Local,
        )
        .unwrap();

    let branch_name = alice.schema_manager().branch_name();
    let visible_row_before = alice
        .storage()
        .load_visible_region_row("users", branch_name.as_str(), row_id)
        .unwrap()
        .expect("insert_persisted should create one visible row");
    let batch_id = visible_row_before.batch_id;

    alice
        .storage_mut()
        .upsert_authoritative_batch_fate(&crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "permission_denied".to_string(),
            reason: "simulated post-insert rejection".to_string(),
        })
        .unwrap();

    let storage = alice.into_storage();
    let restarted =
        create_runtime_with_storage(schema, "rc-restart-apply-stored-rejected-test", storage);

    assert_eq!(
        restarted
            .storage()
            .load_visible_region_row("users", branch_name.as_str(), row_id)
            .unwrap(),
        None,
        "restart must apply stored Rejected settlement and retract the lingering visible row"
    );

    let history_rows = restarted
        .storage()
        .scan_history_row_batches("users", row_id)
        .unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::Rejected,
        "row history should reflect the stored rejection"
    );
}

#[test]
fn rc_persisting_invalid_multibranch_sealed_batch_submission_fails() {
    let schema = test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let batch_id = BatchId::new();
    let main_row_id = ObjectId::new();
    let draft_row_id = ObjectId::new();
    let main_row = staged_user_row(main_row_id, batch_id, 1_000, "Alice");
    let draft_row = crate::row_histories::StoredRowBatch::new_with_batch_id(
        batch_id,
        draft_row_id,
        "draft",
        Vec::<BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(draft_row_id, "Bob"),
        )
        .expect("user test row should encode"),
        crate::metadata::RowProvenance::for_insert(draft_row_id.to_string(), 1_100),
        HashMap::new(),
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let mut old_runtime = create_runtime_with_schema_and_sync_manager(
        schema.clone(),
        "transactional-restart-invalid-seal-recovery-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    for row_id in [main_row_id, draft_row_id] {
        old_runtime
            .storage_mut()
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();
    }
    old_runtime
        .storage_mut()
        .append_history_region_rows("users", &[main_row.clone(), draft_row.clone()])
        .unwrap();
    old_runtime
        .storage_mut()
        .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
            batch_id,
            crate::batch_fate::BatchMode::Direct,
            crate::object::BranchName::new("main"),
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
        ))
        .unwrap();

    let storage = old_runtime.into_storage();
    let restarted = create_runtime_with_storage_and_sync_manager(
        schema,
        "transactional-restart-invalid-seal-recovery-test",
        storage,
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    assert_eq!(
        restarted
            .storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "invalid_batch_submission".to_string(),
            reason: "sealed batch rows must belong to the declared target branch".to_string(),
        })
    );
    assert_eq!(
        restarted
            .storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None
    );
}

#[test]
fn rc_restart_rejects_stale_family_frontier_sealed_batch_from_storage() {
    let schema = test_schema();
    let schema_hash = SchemaHash::compute(&schema);
    let batch_id = BatchId::new();
    let existing_row_id = ObjectId::new();
    let conflicting_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();
    let target_branch = crate::object::BranchName::new("dev-aaaaaaaaaaaa-main");
    let sibling_branch = crate::object::BranchName::new("dev-bbbbbbbbbbbb-main");
    let existing_row = crate::row_histories::StoredRowBatch::new(
        existing_row_id,
        target_branch.as_str(),
        Vec::<BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(existing_row_id, "Seen"),
        )
        .expect("user test row should encode"),
        crate::metadata::RowProvenance::for_insert(existing_row_id.to_string(), 900),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    let conflicting_row = crate::row_histories::StoredRowBatch::new(
        conflicting_row_id,
        sibling_branch.as_str(),
        Vec::<BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(conflicting_row_id, "Bob"),
        )
        .expect("user test row should encode"),
        crate::metadata::RowProvenance::for_insert(conflicting_row_id.to_string(), 950),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    );
    let staged_row = crate::row_histories::StoredRowBatch::new_with_batch_id(
        batch_id,
        staged_row_id,
        target_branch.as_str(),
        Vec::<BatchId>::new(),
        encode_row(
            &test_schema()[&TableName::new("users")].columns,
            &user_row_values(staged_row_id, "Alice"),
        )
        .expect("user test row should encode"),
        crate::metadata::RowProvenance::for_insert(staged_row_id.to_string(), 1_000),
        HashMap::new(),
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let mut old_runtime = create_runtime_with_schema_and_sync_manager(
        schema.clone(),
        "transactional-restart-frontier-conflict-test",
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );
    for row_id in [existing_row_id, conflicting_row_id, staged_row_id] {
        old_runtime
            .storage_mut()
            .put_row_locator(
                row_id,
                Some(&RowLocator {
                    table: "users".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .unwrap();
    }
    old_runtime
        .storage_mut()
        .append_history_region_rows(
            "users",
            &[
                existing_row.clone(),
                conflicting_row.clone(),
                staged_row.clone(),
            ],
        )
        .unwrap();
    old_runtime
        .storage_mut()
        .upsert_visible_region_rows(
            "users",
            &[
                crate::row_histories::VisibleRowEntry::rebuild(
                    existing_row.clone(),
                    std::slice::from_ref(&existing_row),
                ),
                crate::row_histories::VisibleRowEntry::rebuild(
                    conflicting_row.clone(),
                    std::slice::from_ref(&conflicting_row),
                ),
            ],
        )
        .unwrap();
    old_runtime
        .storage_mut()
        .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
            batch_id,
            crate::batch_fate::BatchMode::Transactional,
            target_branch,
            vec![SealedBatchMember {
                object_id: staged_row_id,
                row_digest: staged_row.content_digest(),
            }],
            vec![CapturedFrontierMember {
                object_id: existing_row_id,
                branch_name: target_branch,
                batch_id: existing_row.batch_id(),
            }],
        ))
        .unwrap();

    let storage = old_runtime.into_storage();
    let restarted = create_runtime_with_storage_and_sync_manager(
        schema,
        "transactional-restart-frontier-conflict-test",
        storage,
        SyncManager::new().with_durability_tier(DurabilityTier::Local),
    );

    assert_eq!(
        restarted
            .storage()
            .load_authoritative_batch_fate(batch_id)
            .unwrap(),
        Some(crate::batch_fate::BatchFate::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "family-visible frontier changed since batch was sealed".to_string(),
        })
    );
    assert_eq!(
        restarted
            .storage()
            .load_visible_region_row("users", target_branch.as_str(), staged_row_id)
            .unwrap(),
        None
    );
    assert_eq!(
        restarted
            .storage()
            .scan_history_row_batches("users", staged_row_id)
            .unwrap()[0]
            .state,
        crate::row_histories::RowState::Rejected
    );
    assert_eq!(
        restarted
            .storage()
            .load_sealed_batch_submission(batch_id)
            .unwrap(),
        None
    );
}
