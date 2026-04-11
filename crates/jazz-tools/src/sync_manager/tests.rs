use super::*;
use crate::metadata::{MetadataKey, RowProvenance};
use crate::query_manager::encoding::encode_row;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema, Value};
use crate::row_histories::{StoredRowVersion, VisibleRowEntry};
use crate::storage::{MemoryStorage, Storage};
use crate::test_row_history::{create_test_row_with_id, persist_test_schema};
use std::collections::{HashMap, HashSet};

fn users_test_schema() -> crate::query_manager::types::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("value", ColumnType::Text))
        .build()
}

fn users_schema_hash() -> SchemaHash {
    SchemaHash::compute(&users_test_schema())
}

fn seed_users_schema(storage: &mut MemoryStorage) {
    persist_test_schema(storage, &users_test_schema());
}

fn row_metadata(table: &str) -> HashMap<String, String> {
    HashMap::from([
        (MetadataKey::Table.to_string(), table.to_string()),
        (
            MetadataKey::OriginSchemaHash.to_string(),
            users_schema_hash().to_string(),
        ),
    ])
}

fn visible_row(
    row_id: ObjectId,
    branch: &str,
    parents: Vec<crate::commit::CommitId>,
    updated_at: u64,
    data: &[u8],
) -> crate::row_histories::StoredRowVersion {
    let payload = std::str::from_utf8(data).expect("sync-manager test row payload should be utf8");
    crate::row_histories::StoredRowVersion::new(
        row_id,
        branch,
        parents,
        encode_row(
            &users_test_schema()[&"users".into()].columns,
            &[Value::Text(payload.to_string())],
        )
        .expect("sync-manager test row should encode"),
        RowProvenance::for_insert(row_id.to_string(), updated_at),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    )
}

fn seed_visible_row(
    _sm: &mut SyncManager,
    io: &mut MemoryStorage,
    table: &str,
    row: crate::row_histories::StoredRowVersion,
) {
    seed_users_schema(io);
    create_test_row_with_id(io, row.row_id, Some(row_metadata(table)));
    io.append_history_region_rows(table, std::slice::from_ref(&row))
        .unwrap();
    io.upsert_visible_region_rows(
        table,
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            row.clone(),
            std::slice::from_ref(&row),
        )),
    )
    .unwrap();
}

fn add_client(sm: &mut SyncManager, io: &MemoryStorage, client_id: ClientId) {
    sm.add_client_with_storage(io, client_id);
}

fn add_server(sm: &mut SyncManager, io: &MemoryStorage, server_id: ServerId) {
    sm.add_server_with_storage(server_id, false, io);
}

fn set_client_query_scope(
    sm: &mut SyncManager,
    io: &MemoryStorage,
    client_id: ClientId,
    query_id: QueryId,
    scope: HashSet<(ObjectId, BranchName)>,
    session: Option<crate::query_manager::session::Session>,
) {
    sm.set_client_query_scope_with_storage(io, client_id, query_id, scope, session);
}

fn load_visible_row(
    storage: &MemoryStorage,
    table: &str,
    row_id: ObjectId,
    branch: &str,
) -> StoredRowVersion {
    storage
        .load_visible_region_row(table, branch, row_id)
        .unwrap()
        .expect("visible row should exist")
}

#[test]
fn can_create_sync_manager() {
    let sm = SyncManager::new();
    assert!(sm.servers.is_empty());
    assert!(sm.clients.is_empty());
}

#[test]
fn set_query_scope_stores_session() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();

    add_client(&mut sm, &io, client_id);
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        Some(crate::query_manager::session::Session::new("alice")),
    );

    let query = sm
        .get_client(client_id)
        .expect("client should exist")
        .queries
        .get(&QueryId(1))
        .expect("query should exist");
    assert_eq!(query.scope.len(), 1);
    assert_eq!(
        query
            .session
            .as_ref()
            .map(|session| session.user_id.as_str()),
        Some("alice")
    );
}

#[test]
fn send_query_subscription_includes_session() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let server_id = ServerId::new();
    add_server(&mut sm, &io, server_id);
    sm.take_outbox();

    let query = QueryBuilder::new("users").branch("main").build();
    let session = crate::query_manager::session::Session::new("alice");
    sm.send_query_subscription_to_servers(
        QueryId(7),
        query.clone(),
        Some(session.clone()),
        QueryPropagation::Full,
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::QuerySubscription {
                    query_id,
                    query: sent_query,
                    session: sent_session,
                    propagation,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*query_id, QueryId(7));
            assert_eq!(sent_query.table, query.table);
            assert_eq!(*propagation, QueryPropagation::Full);
            assert_eq!(
                sent_session
                    .as_ref()
                    .map(|session| session.user_id.as_str()),
                Some("alice")
            );
        }
        other => panic!("expected QuerySubscription to server, got {other:?}"),
    }
}

#[test]
fn schema_warning_from_server_relays_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let query_id = QueryId(42);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    sm.query_origin
        .entry(query_id)
        .or_default()
        .insert(client_id);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::SchemaWarning(SchemaWarning {
            query_id,
            table_name: "users".to_string(),
            row_count: 3,
            from_hash: crate::query_manager::types::SchemaHash([0xAA; 32]),
            to_hash: crate::query_manager::types::SchemaHash([0xBB; 32]),
        }),
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::SchemaWarning(warning),
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(warning.query_id, query_id);
            assert_eq!(warning.table_name, "users");
        }
        other => panic!("expected relayed schema warning, got {other:?}"),
    }
}

#[test]
fn row_version_created_emits_row_version_state_changed_to_source() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowVersionCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::RowVersionStateChanged {
                    row_id: ack_row_id,
                    branch_name,
                    version_id,
                    state,
                    confirmed_tier,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*ack_row_id, row_id);
            assert_eq!(*branch_name, BranchName::new("main"));
            assert_eq!(*version_id, row.version_id());
            assert_eq!(*state, None);
            assert_eq!(*confirmed_tier, Some(DurabilityTier::Worker));
        }
        other => panic!("expected RowVersionStateChanged to server, got {other:?}"),
    }
}

#[test]
fn row_version_created_stamps_local_durability_into_storage() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    seed_users_schema(&mut io);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowVersionCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row,
        },
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("visible row");
    let history = io
        .scan_history_region(
            "users",
            "main",
            crate::row_histories::HistoryScan::Row { row_id },
        )
        .unwrap();

    assert_eq!(visible.confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").confirmed_tier,
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_version_state_changed_updates_row_region_confirmed_tier_monotonically() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let version_id = row.version_id();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowVersionStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            version_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );
    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowVersionStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            version_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    let visible = io.scan_visible_region("users", "main").unwrap();
    let history = io
        .scan_history_region(
            "users",
            "main",
            crate::row_histories::HistoryScan::Row { row_id },
        )
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(history.len(), 1);
    assert_eq!(visible[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(history[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
}

#[test]
fn row_version_state_changed_enqueues_pending_row_update_for_visible_row() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let version_id = row.version_id();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowVersionStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            version_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    let updates = sm.take_pending_row_visibility_changes();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].object_id, row_id);
    assert_eq!(
        updates[0].row.confirmed_tier,
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_version_state_changed_relays_to_clients_that_received_row_version_needed() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let version_id = row.version_id();

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let initial = sm.take_outbox();
    assert!(initial.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowVersionNeeded { row: needed, .. },
        } if *id == client_id && needed.row_id == row_id
    )));

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowVersionStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            version_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::RowVersionStateChanged {
                    row_id: changed_row_id,
                    version_id: changed_version_id,
                    confirmed_tier: Some(DurabilityTier::Worker),
                    ..
                },
        } if id == client_id && changed_row_id == row_id && changed_version_id == version_id
    )));
}

#[test]
fn row_version_state_changed_stops_relaying_after_scope_removal() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    set_client_query_scope(&mut sm, &io, client_id, QueryId(1), HashSet::new(), None);
    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowVersionStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            version_id: row.version_id(),
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    assert!(sm.take_outbox().into_iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowVersionStateChanged { row_id: changed_row_id, .. },
        } if id == client_id && changed_row_id == row_id
    )));
}

#[test]
fn stale_row_version_from_client_replays_upstream_without_regressing_visible_row() {
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
        SyncPayload::RowVersionCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: older.clone(),
        },
    );

    let visible = load_visible_row(&io, "users", row_id, "main");
    assert_eq!(visible.version_id(), newer.version_id());

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowVersionCreated { row, .. },
        } if id == server_id && row.version_id() == older.version_id()
    )));
}

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
            payload: SyncPayload::RowVersionCreated { row: created, metadata, .. },
        } if id == server_id && created.version_id() == row.version_id() && metadata.is_some()
    )));
}

#[test]
fn add_server_with_storage_syncs_full_row_history_to_server() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    let newer = visible_row(row_id, "main", vec![older.version_id()], 2_000, b"newer");

    seed_users_schema(&mut io);
    io.put_metadata(row_id, row_metadata("users")).unwrap();
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
        .filter(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::CatalogueEntryUpdated { .. },
            } if *id == server_id
        ))
        .count();
    assert_eq!(schema_syncs, 1);

    let row_syncs = outbox
        .iter()
        .filter(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::RowVersionCreated { .. },
            } if *id == server_id
        ))
        .collect::<Vec<_>>();
    assert_eq!(row_syncs.len(), 2);
    assert!(matches!(
        row_syncs[0],
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowVersionCreated { row, metadata, .. },
        } if *id == server_id && row.version_id() == older.version_id() && metadata.is_some()
    ));
    assert!(matches!(
        row_syncs[1],
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowVersionCreated { row, metadata, .. },
        } if *id == server_id && row.version_id() == newer.version_id() && metadata.is_none()
    ));
}

fn push_query_subscription(
    sm: &mut SyncManager,
    client_id: ClientId,
    payload_session: Option<crate::query_manager::session::Session>,
) -> Vec<PendingQuerySubscription> {
    let query = QueryBuilder::new("messages").branch("main").build();
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: payload_session,
            propagation: QueryPropagation::Full,
        },
    });
    sm.process_inbox(&mut MemoryStorage::new());
    sm.take_pending_query_subscriptions()
}

#[test]
fn query_subscription_falls_back_to_client_session_when_payload_omits_it() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
    );

    let pending = push_query_subscription(&mut sm, client_id, None);
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0]
            .session
            .as_ref()
            .map(|session| session.user_id.as_str()),
        Some("alice")
    );
}

#[test]
fn remove_client_cleans_pending_query_subscriptions() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);

    let query = QueryBuilder::new("users").build();
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: alice,
            query_id: QueryId(1),
            query: query.clone(),
            session: None,
            propagation: QueryPropagation::Full,
        });
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: bob,
            query_id: QueryId(2),
            query,
            session: None,
            propagation: QueryPropagation::Full,
        });

    sm.remove_client(alice);

    assert_eq!(sm.pending_query_subscriptions.len(), 1);
    assert_eq!(sm.pending_query_subscriptions[0].client_id, bob);
}

#[test]
fn remove_client_cleans_outbox_entries() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);

    let row = visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice");
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(alice),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: row.clone(),
        },
    });
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(bob),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: row.clone(),
        },
    });
    let server_id = ServerId::new();
    sm.outbox.push(OutboxEntry {
        destination: Destination::Server(server_id),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row,
        },
    });

    sm.remove_client(alice);

    assert_eq!(sm.outbox.len(), 2);
    assert!(sm.outbox.iter().all(|entry| match entry.destination {
        Destination::Client(id) => id != alice,
        Destination::Server(_) => true,
    }));
}

#[test]
fn remove_client_skips_when_inbox_entries_exist() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    add_client(&mut sm, &io, alice);

    sm.push_inbox(InboxEntry {
        source: Source::Client(alice),
        payload: SyncPayload::RowVersionCreated {
            metadata: None,
            row: visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice"),
        },
    });

    assert!(!sm.remove_client(alice));
    assert!(sm.get_client(alice).is_some());
}
