use super::*;
use crate::commit::{Commit, StoredState};
use crate::query_manager::policy::Operation;
use crate::storage::MemoryStorage;
use smallvec::smallvec;

// ========================================================================
// Phase 1: Foundation Tests
// ========================================================================

#[test]
fn can_create_sync_manager() {
    let sm = SyncManager::new();
    assert!(sm.servers.is_empty());
    assert!(sm.clients.is_empty());
}

// ========================================================================
// Phase 2: Server Sync Tests
// ========================================================================

#[test]
fn add_server_receives_existing_objects() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create an object with a commit
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj_id,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    // Add server
    let server_id = ServerId::new();
    sm.add_server(server_id);

    // Check outbox has the object update
    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    metadata,
                    branch_name,
                    commits,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*object_id, obj_id);
            let metadata = metadata
                .as_ref()
                .expect("First sync should include object metadata");
            assert_eq!(metadata.id, obj_id);
            assert!(
                metadata.metadata.is_empty(),
                "Object created without metadata should sync an empty metadata map"
            );
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
        }
        _ => panic!("Expected ObjectUpdated to the newly added server"),
    }
}

#[test]
fn local_commit_syncs_to_server() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    sm.add_server(server_id);

    // Clear initial outbox
    sm.take_outbox();

    // Create object and commit
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let commit_id = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Manually trigger sync (in real usage, this would be called after local changes)
    sm.forward_update_to_servers(obj_id, "main".into());

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    ..
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
            assert_eq!(commits[0].id(), commit_id);
        }
        _ => panic!("Expected ObjectUpdated to server"),
    }
}

#[test]
fn remove_server_stops_sync() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    sm.add_server(server_id);
    sm.take_outbox();

    sm.remove_server(server_id);

    // Create new object
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj_id,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    sm.forward_update_to_servers(obj_id, "main".into());

    let outbox = sm.take_outbox();
    assert!(outbox.is_empty()); // No server to send to
}

#[test]
fn commits_sent_in_causal_order() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();

    // Create chain: c1 <- c2 <- c3
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let c2 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![c1],
            b"c2".to_vec(),
            author,
            None,
        )
        .unwrap();
    let c3 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![c2],
            b"c3".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add server - should receive all commits in order
    let server_id = ServerId::new();
    sm.add_server(server_id);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    ..
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 3);
            // Parents should come before children
            assert_eq!(commits[0].id(), c1);
            assert_eq!(commits[1].id(), c2);
            assert_eq!(commits[2].id(), c3);
        }
        _ => panic!("Expected ObjectUpdated with causal commit ordering"),
    }
}

// ========================================================================
// Phase 3: Client Query Tests
// ========================================================================

#[test]
fn client_with_query_receives_matching_objects() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create object
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj_id,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    // Add client with query
    let client_id = ClientId::new();
    sm.add_client(client_id);

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    ..
                },
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
        }
        _ => panic!("Expected ObjectUpdated to client"),
    }
}

#[test]
fn client_without_query_receives_nothing() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create object
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj_id,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    // Add client without query
    let client_id = ClientId::new();
    sm.add_client(client_id);

    let outbox = sm.take_outbox();
    assert!(outbox.is_empty());
}

#[test]
fn client_receives_existing_catalogue_on_connect() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let object_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(
        crate::metadata::MetadataKey::Type.to_string(),
        crate::metadata::ObjectType::CatalogueSchema.to_string(),
    );

    sm.create_object_with_content(&mut io, object_id, metadata, b"schema".to_vec());

    let client_id = ClientId::new();
    sm.add_client(client_id);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id: synced_object_id,
                    branch_name,
                    metadata,
                    commits,
                },
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*synced_object_id, object_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
            assert!(
                metadata.is_some(),
                "catalogue replay should include metadata"
            );
        }
        _ => panic!("Expected catalogue ObjectUpdated to client"),
    }
}

#[test]
fn live_catalogue_updates_broadcast_without_query_scope() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let client_a = ClientId::new();
    let client_b = ClientId::new();
    sm.add_client(client_a);
    sm.add_client(client_b);
    sm.take_outbox();

    let object_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(
        crate::metadata::MetadataKey::Type.to_string(),
        crate::metadata::ObjectType::CatalogueLens.to_string(),
    );
    sm.create_object_with_content(&mut io, object_id, metadata, b"lens".to_vec());

    sm.forward_update_to_clients(object_id, "main".into());

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 2);
    assert!(outbox.iter().all(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(_),
            payload: SyncPayload::ObjectUpdated { object_id: synced_object_id, .. },
        } if *synced_object_id == object_id
    )));
}

#[test]
fn remotely_received_catalogue_replays_to_later_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let object_id = ObjectId::new();
    let mut metadata = HashMap::new();
    metadata.insert(
        crate::metadata::MetadataKey::Type.to_string(),
        crate::metadata::ObjectType::CatalogueSchema.to_string(),
    );

    sm.push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::ObjectUpdated {
            object_id,
            metadata: Some(ObjectMetadata {
                id: object_id,
                metadata,
            }),
            branch_name: "main".into(),
            commits: vec![Commit {
                parents: smallvec![],
                content: b"schema".to_vec(),
                timestamp: 1000,
                author: ObjectId::new(),
                metadata: None,
                stored_state: StoredState::Stored,
                ack_state: Default::default(),
            }],
        },
    });
    sm.process_inbox(&mut io);
    sm.take_outbox();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    assert!(matches!(
        &outbox[0],
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id: synced_object_id,
                    metadata: Some(_),
                    ..
                },
        } if *id == client_id && *synced_object_id == object_id
    ));
}

#[test]
fn local_commit_in_scope_syncs_to_client() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Setup client with query
    let client_id = ClientId::new();
    sm.add_client(client_id);

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox(); // Clear initial sync

    // Add commit
    let commit_id = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"content".to_vec(),
            author,
            None,
        )
        .unwrap();

    sm.forward_update_to_clients(obj_id, "main".into());

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    ..
                },
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert!(commits.iter().any(|c| c.id() == commit_id));
        }
        _ => panic!("Expected ObjectUpdated to matching client scope"),
    }
}

#[test]
fn local_commit_out_of_scope_not_sent_to_client() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    // Client has query for obj1/main
    let obj1 = sm.object_manager.create(&mut io, None);
    let mut scope = HashSet::new();
    scope.insert((obj1, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox();

    // Create commit on different object
    let obj2 = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj2,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    sm.forward_update_to_clients(obj2, "main".into());

    let outbox = sm.take_outbox();
    assert!(outbox.is_empty()); // obj2 not in client's scope
}

#[test]
fn query_update_adds_scope_triggers_initial_sync() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create two objects
    let obj1 = sm.object_manager.create(&mut io, None);
    let obj2 = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let _ =
        sm.object_manager
            .add_commit(&mut io, obj1, "main", vec![], b"c1".to_vec(), author, None);
    let _ =
        sm.object_manager
            .add_commit(&mut io, obj2, "main", vec![], b"c2".to_vec(), author, None);

    // Client initially only has obj1
    let client_id = ClientId::new();
    sm.add_client(client_id);

    let mut scope = HashSet::new();
    scope.insert((obj1, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox(); // Clear obj1 sync

    // Update query to also include obj2
    let mut new_scope = HashSet::new();
    new_scope.insert((obj1, "main".into()));
    new_scope.insert((obj2, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), new_scope, None);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1); // Only obj2 (newly visible)

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    ..
                },
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj2);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
        }
        _ => panic!("Expected ObjectUpdated for newly visible object"),
    }
}

#[test]
fn query_removal_stops_future_updates() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox();

    // Remove query by directly manipulating client state
    sm.clients
        .get_mut(&client_id)
        .unwrap()
        .queries
        .remove(&QueryId(1));

    // Add commit
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj_id,
        "main",
        vec![],
        b"content".to_vec(),
        author,
        None,
    );

    sm.forward_update_to_clients(obj_id, "main".into());

    let outbox = sm.take_outbox();
    assert!(outbox.is_empty()); // Client no longer in scope
}

// ========================================================================
// ReBAC Permission Enforcement Tests
// ========================================================================

#[test]
fn peer_writes_applied_directly() {
    // Peer role writes are applied directly without permission checks
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_role(client_id, ClientRole::Peer);

    sm.take_outbox();

    // Client pushes update - Peer role bypasses all checks
    let commit = Commit {
        parents: smallvec![c1],
        content: b"update".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // No pending permission checks — Peer bypasses
    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 0);

    // Verify commit was applied
    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(tips.contains(&commit.id()));
}

#[test]
fn admin_writes_catalogue_directly() {
    // Admin role can write catalogue objects directly
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_role(client_id, ClientRole::Admin);

    let obj_id = ObjectId::new();
    let author = ObjectId::new();
    let commit = Commit {
        parents: smallvec![],
        content: b"schema data".to_vec(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    let mut cat_metadata = HashMap::new();
    cat_metadata.insert(
        crate::metadata::MetadataKey::Type.to_string(),
        crate::metadata::ObjectType::CatalogueSchema.to_string(),
    );

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: cat_metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // No pending permission checks — Admin bypasses
    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 0);

    // Commit should be applied directly
    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(tips.contains(&commit.id()));
}

#[test]
fn admin_writes_row_directly() {
    // Admin role can write row objects directly without ReBAC
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_role(client_id, ClientRole::Admin);
    sm.take_outbox();

    let commit = Commit {
        parents: smallvec![c1],
        content: b"updated".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 0);

    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(tips.contains(&commit.id()));
}

#[test]
fn backend_writes_row_directly() {
    // Backend role can write row objects directly without ReBAC.
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_role(client_id, ClientRole::Backend);
    sm.take_outbox();

    let commit = Commit {
        parents: smallvec![c1],
        content: b"updated".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 0);

    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(tips.contains(&commit.id()));
}

#[test]
fn backend_catalogue_writes_are_denied() {
    // Backend role should not be able to write catalogue objects.
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_role(client_id, ClientRole::Backend);

    let obj_id = ObjectId::new();
    let author = ObjectId::new();
    let commit = Commit {
        parents: smallvec![],
        content: b"schema data".to_vec(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    let mut cat_metadata = HashMap::new();
    cat_metadata.insert(
        crate::metadata::MetadataKey::Type.to_string(),
        crate::metadata::ObjectType::CatalogueSchema.to_string(),
    );

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: cat_metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit],
        },
    });

    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| {
        matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::Error(SyncError::CatalogueWriteDenied { object_id, .. }),
            } if *id == client_id && *object_id == obj_id
        )
    }));
    assert!(sm.object_manager.get(obj_id).is_none());
}

#[test]
fn user_with_session_goes_to_permission_check() {
    // User with session sends row data → queued for ReBAC
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_session(client_id, Session::new("alice"));
    sm.take_outbox();

    let commit = Commit {
        parents: smallvec![c1],
        content: b"update".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // Should be queued for permission check
    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].client_id, client_id);
    assert_eq!(pending[0].session.user_id, "alice");

    // Should NOT be applied yet
    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(!tips.contains(&commit.id()));
}

#[test]
fn user_without_session_rejected() {
    // User without session → SessionRequired error
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    // No session set — default User role

    let obj_id = ObjectId::new();
    let author = ObjectId::new();
    let commit = Commit {
        parents: smallvec![],
        content: b"data".to_vec(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit],
        },
    });

    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::Error(SyncError::SessionRequired {
                    object_id,
                    branch_name,
                }),
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
        }
        other => panic!(
            "Expected SessionRequired error to source client, got {:?}",
            other
        ),
    }

    // Object should not exist
    assert!(sm.object_manager.get(obj_id).is_none());
}

#[test]
fn user_catalogue_write_rejected() {
    // User with session tries to write catalogue → CatalogueWriteDenied
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_session(client_id, Session::new("alice"));

    let obj_id = ObjectId::new();
    let author = ObjectId::new();
    let commit = Commit {
        parents: smallvec![],
        content: b"schema data".to_vec(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    let mut cat_metadata = HashMap::new();
    cat_metadata.insert(
        crate::metadata::MetadataKey::Type.to_string(),
        crate::metadata::ObjectType::CatalogueSchema.to_string(),
    );

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: cat_metadata,
            }),
            branch_name: "main".into(),
            commits: vec![commit],
        },
    });

    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::Error(SyncError::CatalogueWriteDenied {
                    object_id,
                    branch_name,
                }),
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
        }
        other => panic!(
            "Expected CatalogueWriteDenied error to source client, got {:?}",
            other
        ),
    }

    // Object should not exist
    assert!(sm.object_manager.get(obj_id).is_none());
}

#[test]
fn add_client_then_set_peer_role() {
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    let client = sm.get_client(client_id).unwrap();
    assert_eq!(client.role, ClientRole::Peer);
}

#[test]
fn write_with_session_goes_to_pending_permission_checks() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    // Set session on client
    if let Some(client) = sm.clients.get_mut(&client_id) {
        client.session = Some(Session::new("user123"));
    }

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox();

    // Client tries to push update
    let commit = Commit {
        parents: smallvec![c1],
        content: b"new_content".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // Should be in pending permission checks
    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].session.user_id, "user123");
    assert_eq!(pending[0].operation, Operation::Update);
    assert_eq!(pending[0].old_content, Some(b"original".to_vec()));
    assert_eq!(pending[0].new_content, Some(b"new_content".to_vec()));

    // Commit should NOT be applied yet
    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(!tips.contains(&commit.id()));
}

#[test]
fn soft_delete_object_updated_is_queued_as_delete_permission_check() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_session(client_id, Session::new("user123"));
    sm.take_outbox();

    let delete_commit = Commit {
        parents: smallvec![c1],
        content: b"original".to_vec(),
        timestamp: 2000,
        author,
        metadata: Some(crate::metadata::soft_delete_metadata()),
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![delete_commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    let pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 1);
    assert_eq!(pending[0].operation, Operation::Delete);
    assert_eq!(pending[0].old_content, Some(b"original".to_vec()));
    assert_eq!(pending[0].new_content, None);

    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(!tips.contains(&delete_commit.id()));
}

#[test]
fn approve_permission_check_applies_write() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    // Set session on client
    if let Some(client) = sm.clients.get_mut(&client_id) {
        client.session = Some(Session::new("user123"));
    }

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox();

    // Client pushes update
    let commit = Commit {
        parents: smallvec![c1],
        content: b"allowed".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // Get pending check and approve it
    let mut pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 1);
    let check = pending.remove(0);

    sm.approve_permission_check(&mut io, check);

    // Commit should now be applied
    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(tips.contains(&commit.id()));
}

#[test]
fn reject_permission_check_sends_error() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"original".to_vec(),
            author,
            None,
        )
        .unwrap();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    // Set session on client
    if let Some(client) = sm.clients.get_mut(&client_id) {
        client.session = Some(Session::new("user123"));
    }

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox();

    // Client tries to push update
    let commit = Commit {
        parents: smallvec![c1],
        content: b"denied".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // Get pending check and reject it
    let mut pending = sm.take_pending_permission_checks();
    assert_eq!(pending.len(), 1);
    let check = pending.remove(0);

    sm.reject_permission_check(check, "access denied by policy".to_string());

    // Should get permission denied error
    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::Error(SyncError::PermissionDenied {
                    object_id,
                    branch_name,
                    reason,
                }),
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(reason, "access denied by policy");
        }
        _ => panic!("Expected PermissionDenied error for source client"),
    }

    // Commit should NOT be applied
    let tips = sm.object_manager.get_tip_ids(obj_id, "main").unwrap();
    assert!(!tips.contains(&commit.id()));
}

#[test]
fn server_update_forwarded_to_matching_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Setup server
    let server_id = ServerId::new();
    sm.add_server(server_id);
    sm.take_outbox();

    // Setup client with query
    let client_id = ClientId::new();
    sm.add_client(client_id);

    let obj_id = ObjectId::new();
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox();

    // Server sends update
    let author = ObjectId::new();
    let commit = Commit {
        parents: smallvec![],
        content: b"from server".to_vec(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };
    let commit_id = commit.id();

    sm.push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: Some(ObjectMetadata {
                id: obj_id,
                metadata: HashMap::new(),
            }),
            branch_name: "main".into(),
            commits: vec![commit.clone()],
        },
    });

    sm.process_inbox(&mut io);

    // Client should receive forwarded update
    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);

    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    ..
                },
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
            assert_eq!(commits[0].id(), commit_id);
        }
        _ => panic!("Expected ObjectUpdated to client"),
    }
}

// ========================================================================
// Integration Tests
// ========================================================================

#[test]
fn client_update_forwarded_to_server_and_other_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Setup server
    let server_id = ServerId::new();
    sm.add_server(server_id);

    // Create object
    let obj_id = sm.object_manager.create(&mut io, None);
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"initial".to_vec(),
            author,
            None,
        )
        .unwrap();

    sm.take_outbox();

    // Setup two clients — client1 is Peer so writes go through directly
    let client1 = ClientId::new();
    let client2 = ClientId::new();
    sm.add_client(client1);
    sm.set_client_role(client1, ClientRole::Peer);
    sm.add_client(client2);

    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client1, QueryId(1), scope.clone(), None);
    sm.set_client_query_scope(client2, QueryId(1), scope, None);
    sm.take_outbox();

    // Client1 sends update
    let commit = Commit {
        parents: smallvec![c1],
        content: b"from client1".to_vec(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };
    let commit_id = commit.id();

    sm.push_inbox(InboxEntry {
        source: Source::Client(client1),
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: "main".into(),
            commits: vec![commit],
        },
    });

    sm.process_inbox(&mut io);

    let outbox = sm.take_outbox();

    // Should have updates for: server + client2 (not client1)
    assert_eq!(outbox.len(), 2);

    let destinations: HashSet<_> = outbox.iter().map(|e| &e.destination).collect();
    assert!(destinations.contains(&Destination::Server(server_id)));
    assert!(destinations.contains(&Destination::Client(client2)));
    assert!(!destinations.contains(&Destination::Client(client1)));

    let server_update = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .expect("expected forwarded update to server");
    match &server_update.payload {
        SyncPayload::ObjectUpdated {
            object_id,
            branch_name,
            commits,
            ..
        } => {
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert!(
                commits.iter().any(|c| c.id() == commit_id),
                "server payload must include the client's new commit"
            );
            let parent_pos = commits.iter().position(|c| c.id() == c1);
            let child_pos = commits.iter().position(|c| c.id() == commit_id);
            if let (Some(parent_pos), Some(child_pos)) = (parent_pos, child_pos) {
                assert!(
                    parent_pos < child_pos,
                    "if parent commit is forwarded, it must come before child"
                );
            }
        }
        other => panic!("Expected ObjectUpdated payload to server, got {:?}", other),
    }

    let client_update = outbox
        .iter()
        .find(|e| matches!(e.destination, Destination::Client(id) if id == client2))
        .expect("expected forwarded update to matching client");
    match &client_update.payload {
        SyncPayload::ObjectUpdated {
            object_id,
            branch_name,
            commits,
            ..
        } => {
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert!(
                commits.iter().any(|c| c.id() == commit_id),
                "client payload must include the forwarded commit"
            );
        }
        other => panic!("Expected ObjectUpdated payload to client2, got {:?}", other),
    }
}

#[test]
fn metadata_sent_only_once_per_destination() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create object BEFORE adding server
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [("key".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        ),
    );
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Now add server - should receive existing object with metadata
    let server_id = ServerId::new();
    sm.add_server(server_id);

    let outbox = sm.take_outbox();

    // First message should have metadata
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    metadata,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
            assert_eq!(commits[0].id(), c1);
            let metadata = metadata
                .as_ref()
                .expect("Existing object sync should include metadata on first send");
            assert_eq!(metadata.id, obj_id);
            assert_eq!(
                metadata.metadata.get("key"),
                Some(&"value".to_string()),
                "Expected key=value metadata to be included in first sync"
            );
        }
        _ => panic!("Expected ObjectUpdated to server with first-send metadata"),
    }

    // Add another commit (as child of c1)
    let _ = sm.object_manager.add_commit(
        &mut io,
        obj_id,
        "main",
        vec![c1],
        b"c2".to_vec(),
        author,
        None,
    );

    sm.forward_update_to_servers(obj_id, "main".into());

    let outbox = sm.take_outbox();

    // Second message should NOT have metadata
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    branch_name,
                    commits,
                    metadata,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*object_id, obj_id);
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
            assert!(metadata.is_none());
        }
        _ => panic!("Expected ObjectUpdated to server without metadata on repeat send"),
    }
}

// ========================================================================
// nosync Filtering Tests
// ========================================================================

#[test]
fn nosync_object_not_synced_to_server() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create object with nosync: "true" metadata
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [(
                crate::metadata::MetadataKey::NoSync.to_string(),
                "true".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
    );
    let author = ObjectId::new();
    sm.object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add server - should NOT receive the nosync object
    let server_id = ServerId::new();
    sm.add_server(server_id);

    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "nosync object should not be synced to server"
    );
}

#[test]
fn nosync_object_not_synced_to_client() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create object with nosync: "true" metadata
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [(
                crate::metadata::MetadataKey::NoSync.to_string(),
                "true".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
    );
    let author = ObjectId::new();
    sm.object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add client with scope including the object
    let client_id = ClientId::new();
    sm.add_client(client_id);
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);

    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "nosync object should not be synced to client"
    );
}

#[test]
fn nosync_object_update_not_forwarded_to_server() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create nosync object
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [(
                crate::metadata::MetadataKey::NoSync.to_string(),
                "true".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
    );
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add server
    let server_id = ServerId::new();
    sm.add_server(server_id);
    sm.take_outbox(); // Clear any initial sync messages

    // Add another commit
    sm.object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![c1],
            b"c2".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Forward update to servers
    sm.forward_update_to_servers(obj_id, "main".into());

    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "nosync object update should not be forwarded to server"
    );
}

#[test]
fn nosync_object_truncation_not_forwarded_to_server() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create nosync object with some history
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [(
                crate::metadata::MetadataKey::NoSync.to_string(),
                "true".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
    );
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let c2 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![c1],
            b"c2".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add server
    let server_id = ServerId::new();
    sm.add_server(server_id);
    sm.take_outbox(); // Clear any initial sync messages

    // Forward truncation to servers (simulating what would happen after truncation)
    // The nosync check should prevent any message from being sent
    sm.forward_truncation_to_servers(obj_id, "main".into(), [c2].into_iter().collect());

    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "nosync object truncation should not be forwarded to server"
    );
}

#[test]
fn nosync_object_truncation_not_forwarded_to_client() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create nosync object with some history
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [(
                crate::metadata::MetadataKey::NoSync.to_string(),
                "true".to_string(),
            )]
            .into_iter()
            .collect(),
        ),
    );
    let author = ObjectId::new();
    let c1 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();
    let c2 = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![c1],
            b"c2".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add client with scope including the object
    let client_id = ClientId::new();
    sm.add_client(client_id);
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    sm.set_client_query_scope(client_id, QueryId(1), scope, None);
    sm.take_outbox(); // Clear any initial sync messages

    // Forward truncation to clients (simulating what would happen after truncation)
    // The nosync check should prevent any message from being sent
    sm.forward_truncation_to_clients(obj_id, "main".into(), [c2].into_iter().collect());

    let outbox = sm.take_outbox();
    assert!(
        outbox.is_empty(),
        "nosync object truncation should not be forwarded to client"
    );
}

#[test]
fn regular_object_still_syncs_to_server() {
    // Ensure regular objects without nosync still sync properly
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();

    // Create object WITHOUT nosync metadata
    let obj_id = sm.object_manager.create(
        &mut io,
        Some(
            [("key".to_string(), "value".to_string())]
                .into_iter()
                .collect(),
        ),
    );
    let author = ObjectId::new();
    let commit_id = sm
        .object_manager
        .add_commit(
            &mut io,
            obj_id,
            "main",
            vec![],
            b"c1".to_vec(),
            author,
            None,
        )
        .unwrap();

    // Add server - should receive the object
    let server_id = ServerId::new();
    sm.add_server(server_id);

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1, "regular object should sync to server");
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::ObjectUpdated {
                    object_id,
                    metadata,
                    branch_name,
                    commits,
                },
        } => {
            assert_eq!(
                *id, server_id,
                "message should target the newly added server"
            );
            assert_eq!(
                *object_id, obj_id,
                "synced object id should match created object"
            );
            assert_eq!(branch_name.as_str(), "main");
            assert_eq!(commits.len(), 1);
            assert_eq!(commits[0].id(), commit_id);

            let metadata = metadata
                .as_ref()
                .expect("first sync for regular object should include metadata");
            assert_eq!(metadata.id, obj_id);
            assert_eq!(
                metadata.metadata.get("key").map(String::as_str),
                Some("value")
            );
        }
        other => panic!(
            "Expected ObjectUpdated payload to server after add_server, got {:?}",
            other
        ),
    }
}

// ========================================================================
// Session Propagation Tests
// ========================================================================

#[test]
fn set_query_scope_stores_session() {
    let mut sm = SyncManager::new();

    let client_id = ClientId::new();
    sm.add_client(client_id);

    let obj_id = ObjectId::new();
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));

    let session = Session::new("alice");
    sm.set_client_query_scope(client_id, QueryId(1), scope.clone(), Some(session));

    let client = sm.get_client(client_id).expect("client should exist");
    let query = client.queries.get(&QueryId(1)).expect("query should exist");
    assert_eq!(query.scope, scope);
    let session = query
        .session
        .as_ref()
        .expect("query scope should store provided session");
    assert_eq!(session.user_id, "alice");
}

#[test]
fn send_query_subscription_includes_session() {
    // Test that send_query_subscription_to_servers includes the session
    use crate::query_manager::query::QueryBuilder;

    let mut sm = SyncManager::new();

    let server_id = ServerId::new();
    sm.add_server(server_id);
    sm.take_outbox();

    let query = QueryBuilder::new("users").branch("main").build();
    let session = Session::new("alice");

    sm.send_query_subscription_to_servers(
        QueryId(1),
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
            assert_eq!(*query_id, QueryId(1));
            assert_eq!(sent_query.table, query.table);
            assert_eq!(*propagation, QueryPropagation::Full);
            let sent_session = sent_session
                .as_ref()
                .expect("QuerySubscription payload should include session");
            assert_eq!(sent_session.user_id, "alice");
        }
        _ => panic!("Expected QuerySubscription to connected server"),
    }
}

// ========================================================================
// Phase 6a: Persistence Ack E2E Tests
// ========================================================================

/// Route messages between three tiers: A ↔ B ↔ C.
///
/// A is a client of B, B is a client of C.
/// Pumps until no messages remain or 10 rounds (whichever comes first).
/// Auto-approves pending updates on B and C (simulates permissive server).
#[allow(clippy::too_many_arguments)]
fn pump_messages_3tier(
    a: &mut SyncManager,
    b: &mut SyncManager,
    c: &mut SyncManager,
    a_io: &mut MemoryStorage,
    b_io: &mut MemoryStorage,
    c_io: &mut MemoryStorage,
    a_client_of_b: ClientId,
    b_server_for_a: ServerId,
    b_client_of_c: ClientId,
    c_server_for_b: ServerId,
) {
    for _ in 0..10 {
        let mut any_messages = false;

        // A outbox → B inbox (A sends to server b_server_for_a → B receives from client a_client_of_b)
        for entry in a.take_outbox() {
            if entry.destination == Destination::Server(b_server_for_a) {
                any_messages = true;
                b.push_inbox(InboxEntry {
                    source: Source::Client(a_client_of_b),
                    payload: entry.payload,
                });
            }
        }

        // B outbox → route to A or C
        for entry in b.take_outbox() {
            match &entry.destination {
                Destination::Client(cid) if *cid == a_client_of_b => {
                    any_messages = true;
                    a.push_inbox(InboxEntry {
                        source: Source::Server(b_server_for_a),
                        payload: entry.payload,
                    });
                }
                Destination::Server(sid) if *sid == c_server_for_b => {
                    any_messages = true;
                    c.push_inbox(InboxEntry {
                        source: Source::Client(b_client_of_c),
                        payload: entry.payload,
                    });
                }
                _ => {}
            }
        }

        // C outbox → B inbox
        for entry in c.take_outbox() {
            if entry.destination == Destination::Client(b_client_of_c) {
                any_messages = true;
                b.push_inbox(InboxEntry {
                    source: Source::Server(c_server_for_b),
                    payload: entry.payload,
                });
            }
        }

        if !any_messages && a.inbox.is_empty() && b.inbox.is_empty() && c.inbox.is_empty() {
            break;
        }

        a.process_inbox(a_io);
        b.process_inbox(b_io);
        c.process_inbox(c_io);
    }
}

/// Setup helper: creates A ↔ B ↔ C topology.
/// Returns (a, b, c, a_io, b_io, c_io, ids...).
struct ThreeTierSetup {
    a: SyncManager,
    b: SyncManager,
    c: SyncManager,
    a_io: MemoryStorage,
    b_io: MemoryStorage,
    c_io: MemoryStorage,
    a_client_of_b: ClientId,
    b_server_for_a: ServerId,
    b_client_of_c: ClientId,
    c_server_for_b: ServerId,
}

fn setup_3tier() -> ThreeTierSetup {
    let a_client_of_b = ClientId::new();
    let b_server_for_a = ServerId::new();
    let b_client_of_c = ClientId::new();
    let c_server_for_b = ServerId::new();

    let a = SyncManager::new();
    let mut b = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut c = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);

    // A connects to B as server
    b.add_client(a_client_of_b);
    b.set_client_role(a_client_of_b, ClientRole::Peer);

    // B connects to C as server
    c.add_client(b_client_of_c);
    c.set_client_role(b_client_of_c, ClientRole::Peer);
    b.add_server(c_server_for_b);

    ThreeTierSetup {
        a,
        b,
        c,
        a_io: MemoryStorage::new(),
        b_io: MemoryStorage::new(),
        c_io: MemoryStorage::new(),
        a_client_of_b,
        b_server_for_a,
        b_client_of_c,
        c_server_for_b,
    }
}

fn make_test_commit(content: &[u8], parents: Vec<CommitId>) -> Commit {
    Commit {
        parents: parents.into(),
        content: content.to_vec(),
        timestamp: 1000,
        author: ObjectId::from_uuid(uuid::Uuid::nil()),
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    }
}

#[test]
fn persistence_ack_direct() {
    let mut s = setup_3tier();

    // Create object on A and add commit
    let obj_id = s.a.object_manager.create(&mut s.a_io, None);
    let commit = make_test_commit(b"hello", vec![]);
    let commit_id = commit.id();
    let _ =
        s.a.object_manager
            .receive_commit(&mut s.a_io, obj_id, "main", commit);
    s.a.add_server(s.b_server_for_a);
    s.a.forward_update_to_servers(obj_id, "main".into());

    pump_messages_3tier(
        &mut s.a,
        &mut s.b,
        &mut s.c,
        &mut s.a_io,
        &mut s.b_io,
        &mut s.c_io,
        s.a_client_of_b,
        s.b_server_for_a,
        s.b_client_of_c,
        s.c_server_for_b,
    );

    // A should have received a PersistenceAck from B (tier=Worker)
    // Check A's processed state — the ack was processed by A's process_inbox
    // Since A has no tier, it doesn't re-emit, but it should have received the ack
    // Let's check: the ack was delivered to A's inbox and processed.
    // Since A processes PersistenceAck from server, it stores it in io and updates in-memory.
    let a_commit =
        s.a.object_manager
            .get_commit_mut(obj_id, &"main".into(), commit_id);
    let a_commit = a_commit.expect("Commit should exist on A");
    assert!(
        a_commit
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::Worker),
        "A should have received Worker ack from B"
    );
}

#[test]
fn persistence_ack_relay() {
    let mut s = setup_3tier();

    // Create object on A
    let obj_id = s.a.object_manager.create(&mut s.a_io, None);
    let commit = make_test_commit(b"hello-relay", vec![]);
    let commit_id = commit.id();
    let _ =
        s.a.object_manager
            .receive_commit(&mut s.a_io, obj_id, "main", commit);
    s.a.add_server(s.b_server_for_a);
    s.a.forward_update_to_servers(obj_id, "main".into());

    pump_messages_3tier(
        &mut s.a,
        &mut s.b,
        &mut s.c,
        &mut s.a_io,
        &mut s.b_io,
        &mut s.c_io,
        s.a_client_of_b,
        s.b_server_for_a,
        s.b_client_of_c,
        s.c_server_for_b,
    );

    // A should have received EdgeServer ack (relayed through B from C)
    let a_commit =
        s.a.object_manager
            .get_commit_mut(obj_id, &"main".into(), commit_id)
            .expect("Commit should exist on A");
    assert!(
        a_commit
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::EdgeServer),
        "A should have received EdgeServer ack relayed through B"
    );
}

#[test]
fn persistence_ack_both_tiers() {
    let mut s = setup_3tier();

    let obj_id = s.a.object_manager.create(&mut s.a_io, None);
    let commit = make_test_commit(b"hello-both", vec![]);
    let commit_id = commit.id();
    let _ =
        s.a.object_manager
            .receive_commit(&mut s.a_io, obj_id, "main", commit);
    s.a.add_server(s.b_server_for_a);
    s.a.forward_update_to_servers(obj_id, "main".into());

    pump_messages_3tier(
        &mut s.a,
        &mut s.b,
        &mut s.c,
        &mut s.a_io,
        &mut s.b_io,
        &mut s.c_io,
        s.a_client_of_b,
        s.b_server_for_a,
        s.b_client_of_c,
        s.c_server_for_b,
    );

    let a_commit =
        s.a.object_manager
            .get_commit_mut(obj_id, &"main".into(), commit_id)
            .expect("Commit should exist on A");
    assert!(
        a_commit
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::Worker),
        "Should have Worker ack from B"
    );
    assert!(
        a_commit
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::EdgeServer),
        "Should have EdgeServer ack from C"
    );
}

#[test]
fn persistence_ack_idempotent() {
    let mut s = setup_3tier();

    let obj_id = s.a.object_manager.create(&mut s.a_io, None);
    let commit = make_test_commit(b"idempotent", vec![]);
    let commit_id = commit.id();
    let _ =
        s.a.object_manager
            .receive_commit(&mut s.a_io, obj_id, "main", commit.clone());
    s.a.add_server(s.b_server_for_a);
    s.a.forward_update_to_servers(obj_id, "main".into());

    // Pump once
    pump_messages_3tier(
        &mut s.a,
        &mut s.b,
        &mut s.c,
        &mut s.a_io,
        &mut s.b_io,
        &mut s.c_io,
        s.a_client_of_b,
        s.b_server_for_a,
        s.b_client_of_c,
        s.c_server_for_b,
    );

    // Send the same commit again — should not panic
    s.a.forward_update_to_servers(obj_id, "main".into());

    pump_messages_3tier(
        &mut s.a,
        &mut s.b,
        &mut s.c,
        &mut s.a_io,
        &mut s.b_io,
        &mut s.c_io,
        s.a_client_of_b,
        s.b_server_for_a,
        s.b_client_of_c,
        s.c_server_for_b,
    );

    // Still has acks
    let a_commit =
        s.a.object_manager
            .get_commit_mut(obj_id, &"main".into(), commit_id)
            .expect("Commit should exist on A");
    assert!(
        a_commit
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::Worker)
    );
}

#[test]
fn persistence_ack_cleanup_on_disconnect() {
    let mut s = setup_3tier();

    // A creates and sends a commit to B
    let obj_id = s.a.object_manager.create(&mut s.a_io, None);
    let commit = make_test_commit(b"disconnect-test", vec![]);
    let _ =
        s.a.object_manager
            .receive_commit(&mut s.a_io, obj_id, "main", commit);
    s.a.add_server(s.b_server_for_a);
    s.a.forward_update_to_servers(obj_id, "main".into());

    // Pump A→B only (one round)
    for entry in s.a.take_outbox() {
        if entry.destination == Destination::Server(s.b_server_for_a) {
            s.b.push_inbox(InboxEntry {
                source: Source::Client(s.a_client_of_b),
                payload: entry.payload,
            });
        }
    }
    s.b.process_inbox(&mut s.b_io);
    // B should now have interest for A's commits

    // Disconnect A from B
    s.b.remove_client(s.a_client_of_b);

    // C acks arrive at B — should not crash when trying to relay to disconnected A
    // Forward B→C and let C ack back
    for entry in s.b.take_outbox() {
        match &entry.destination {
            Destination::Server(sid) if *sid == s.c_server_for_b => {
                s.c.push_inbox(InboxEntry {
                    source: Source::Client(s.b_client_of_c),
                    payload: entry.payload,
                });
            }
            _ => {}
        }
    }
    s.c.process_inbox(&mut s.c_io);

    // C sends ack back to B
    for entry in s.c.take_outbox() {
        if entry.destination == Destination::Client(s.b_client_of_c) {
            s.b.push_inbox(InboxEntry {
                source: Source::Server(s.c_server_for_b),
                payload: entry.payload,
            });
        }
    }
    // Should not panic — A's interest was cleaned up
    s.b.process_inbox(&mut s.b_io);

    // B should not have any outbox entries for the disconnected client
    let outbox = s.b.take_outbox();
    for entry in &outbox {
        if let Destination::Client(cid) = &entry.destination {
            assert_ne!(
                *cid, s.a_client_of_b,
                "Should not relay to disconnected client"
            );
        }
    }
}

#[test]
fn persistence_ack_survives_reload() {
    let mut io = MemoryStorage::new();

    let obj_id = ObjectId::new();
    io.create_object(obj_id, HashMap::new()).unwrap();

    let commit = make_test_commit(b"persist-test", vec![]);
    let commit_id = commit.id();
    io.append_commit(obj_id, &"main".into(), commit).unwrap();

    // Store ack tier
    io.store_ack_tier(commit_id, DurabilityTier::EdgeServer)
        .unwrap();

    // Load branch and verify ack_state is populated
    let loaded = io
        .load_branch(obj_id, &"main".into())
        .unwrap()
        .expect("Branch should exist");

    assert_eq!(loaded.commits.len(), 1);
    assert!(
        loaded.commits[0]
            .ack_state
            .confirmed_tiers
            .contains(&DurabilityTier::EdgeServer),
        "Loaded commit should have EdgeServer ack"
    );
}

#[test]
fn ack_state_does_not_affect_commit_id_sync() {
    // Verify that commits with different ack_state have the same ID
    // (complementary to the unit test in commit.rs)
    let mut ack_state = crate::commit::CommitAckState::default();
    ack_state
        .confirmed_tiers
        .insert(DurabilityTier::GlobalServer);

    let commit1 = make_test_commit(b"same-content", vec![]);
    let mut commit2 = make_test_commit(b"same-content", vec![]);
    commit2.ack_state = ack_state;

    assert_eq!(commit1.id(), commit2.id());
}

// ========================================================================
// QuerySubscription session fallback (inbox.rs fix)
// ========================================================================

/// Helper: push a QuerySubscription from a client and drain pending subs.
fn push_query_subscription(
    sm: &mut SyncManager,
    client_id: ClientId,
    payload_session: Option<Session>,
) -> Vec<PendingQuerySubscription> {
    use crate::query_manager::query::QueryBuilder;
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
    // Demo/anonymous clients send session: None in the payload.
    // The server established a session during the SSE handshake; that should be used.
    //
    //   client.session = Some("alice")   (server-established)
    //   payload session = None           (client sent nothing)
    //   → effective session = Some("alice")
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_session(client_id, Session::new("alice"));

    let pending = push_query_subscription(&mut sm, client_id, None);

    assert_eq!(pending.len(), 1);
    let session = pending[0]
        .session
        .as_ref()
        .expect("should fall back to server-established session");
    assert_eq!(session.user_id, "alice");
}

#[test]
fn query_subscription_uses_client_session_when_payload_supplies_one() {
    // Authenticated client sends a matching session in the payload.
    // server session wins regardless (same value in the honest case).
    //
    //   client.session = Some("alice")
    //   payload session = Some("alice")
    //   → effective session = Some("alice")
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_session(client_id, Session::new("alice"));

    let pending = push_query_subscription(&mut sm, client_id, Some(Session::new("alice")));

    assert_eq!(pending.len(), 1);
    let session = pending[0]
        .session
        .as_ref()
        .expect("session should be present");
    assert_eq!(session.user_id, "alice");
}

#[test]
fn query_subscription_ignores_spoofed_payload_session() {
    // A client with an established server session sends a different session
    // in the payload — the server-established one must win.
    //
    //   client.session = Some("alice")
    //   payload session = Some("mallory")   ← spoofed
    //   → effective session = Some("alice")
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    sm.add_client(client_id);
    sm.set_client_session(client_id, Session::new("alice"));

    let pending = push_query_subscription(&mut sm, client_id, Some(Session::new("mallory")));

    assert_eq!(pending.len(), 1);
    let session = pending[0]
        .session
        .as_ref()
        .expect("session should be present");
    assert_eq!(
        session.user_id, "alice",
        "spoofed payload session must be ignored"
    );
}

#[test]
fn query_subscription_demo_client_no_server_session_no_payload_session() {
    // Fully anonymous/demo client: no server session, no payload session.
    // Queries should proceed with session: None (the query layer handles
    // the open-access policy for demo mode).
    //
    //   client.session = None
    //   payload session = None
    //   → effective session = None
    let mut sm = SyncManager::new();
    let client_id = ClientId::new();
    sm.add_client(client_id);
    // No set_client_session call — client is fully anonymous.

    let pending = push_query_subscription(&mut sm, client_id, None);

    assert_eq!(pending.len(), 1);
    assert!(
        pending[0].session.is_none(),
        "anonymous client should produce session: None"
    );
}

// ========================================================================
// Client disconnect cleanup tests
// ========================================================================

#[test]
fn remove_client_cleans_pending_permission_checks() {
    //
    // alice ──write──▶ server (pending policy check)
    // bob   ──write──▶ server (pending policy check)
    //
    // alice disconnects → only bob's check remains.
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    let bob = ClientId::new();
    sm.add_client(alice);
    sm.add_client(bob);

    let obj_id = ObjectId::new();
    // Manually push pending permission checks
    sm.pending_permission_checks.push(PendingPermissionCheck {
        id: PendingUpdateId(1),
        client_id: alice,
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: BranchName::new("main"),
            commits: vec![],
        },
        session: crate::query_manager::session::Session {
            user_id: "alice".into(),
            claims: serde_json::Value::Null,
        },
        schema_wait_started_at: None,
        metadata: Default::default(),
        old_content: None,
        new_content: None,
        operation: Operation::Insert,
    });
    sm.pending_permission_checks.push(PendingPermissionCheck {
        id: PendingUpdateId(2),
        client_id: bob,
        payload: SyncPayload::ObjectUpdated {
            object_id: obj_id,
            metadata: None,
            branch_name: BranchName::new("main"),
            commits: vec![],
        },
        session: crate::query_manager::session::Session {
            user_id: "bob".into(),
            claims: serde_json::Value::Null,
        },
        schema_wait_started_at: None,
        metadata: Default::default(),
        old_content: None,
        new_content: None,
        operation: Operation::Insert,
    });

    sm.remove_client(alice);

    assert_eq!(sm.pending_permission_checks.len(), 1);
    assert_eq!(sm.pending_permission_checks[0].client_id, bob);
}

#[test]
fn remove_client_cleans_pending_query_subscriptions() {
    //
    // alice ──subscribe──▶ server (pending, not yet built)
    // bob   ──subscribe──▶ server (pending, not yet built)
    //
    // alice disconnects → only bob's pending sub remains.
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    let bob = ClientId::new();
    sm.add_client(alice);
    sm.add_client(bob);

    let query = crate::query_manager::query::QueryBuilder::new("users").build();
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
            query_id: QueryId(1),
            query,
            session: None,
            propagation: QueryPropagation::Full,
        });

    sm.remove_client(alice);

    assert_eq!(sm.pending_query_subscriptions.len(), 1);
    assert_eq!(sm.pending_query_subscriptions[0].client_id, bob);
}

#[test]
fn remove_client_cleans_pending_query_unsubscriptions() {
    //
    // alice ──unsubscribe──▶ server (pending cleanup)
    // bob   ──unsubscribe──▶ server (pending cleanup)
    //
    // alice disconnects → only bob's pending unsub remains.
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    let bob = ClientId::new();
    sm.add_client(alice);
    sm.add_client(bob);

    sm.pending_query_unsubscriptions
        .push(PendingQueryUnsubscription {
            client_id: alice,
            query_id: QueryId(1),
        });
    sm.pending_query_unsubscriptions
        .push(PendingQueryUnsubscription {
            client_id: bob,
            query_id: QueryId(2),
        });

    sm.remove_client(alice);

    assert_eq!(sm.pending_query_unsubscriptions.len(), 1);
    assert_eq!(sm.pending_query_unsubscriptions[0].client_id, bob);
}

#[test]
fn remove_client_cleans_outbox_entries() {
    //
    // server has queued messages for alice and bob.
    // alice disconnects → only bob's messages remain.
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    let bob = ClientId::new();
    sm.add_client(alice);
    sm.add_client(bob);

    let obj_id = ObjectId::new();
    let payload = SyncPayload::ObjectUpdated {
        object_id: obj_id,
        metadata: None,
        branch_name: BranchName::new("main"),
        commits: vec![],
    };

    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(alice),
        payload: payload.clone(),
    });
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(bob),
        payload: payload.clone(),
    });
    // Server-destined messages should not be affected
    let server_id = ServerId::new();
    sm.outbox.push(OutboxEntry {
        destination: Destination::Server(server_id),
        payload,
    });

    sm.remove_client(alice);

    assert_eq!(sm.outbox.len(), 2);
    assert!(sm.outbox.iter().all(|e| match &e.destination {
        Destination::Client(id) => *id != alice,
        Destination::Server(_) => true,
    }));
}

#[test]
fn remove_client_cleans_inbox_entries() {
    //
    // alice ──msg──▶ server inbox (not yet processed)
    // bob   ──msg──▶ server inbox (not yet processed)
    //
    // alice disconnects → only bob's inbox entry and server-sourced entries remain.
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    let bob = ClientId::new();
    let server_id = ServerId::new();
    sm.add_client(alice);
    sm.add_client(bob);

    let obj_id = ObjectId::new();
    let payload = SyncPayload::ObjectUpdated {
        object_id: obj_id,
        metadata: None,
        branch_name: BranchName::new("main"),
        commits: vec![],
    };

    sm.push_inbox(InboxEntry {
        source: Source::Client(alice),
        payload: payload.clone(),
    });
    sm.push_inbox(InboxEntry {
        source: Source::Client(bob),
        payload: payload.clone(),
    });
    sm.push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload,
    });

    sm.remove_client(alice);

    assert_eq!(sm.inbox.len(), 2);
    assert!(sm.inbox.iter().all(|e| e.source != Source::Client(alice)));
}

#[test]
fn remove_client_cleans_query_origin() {
    //
    // alice ──subscribe(q1)──▶ server   (query_origin: q1→{alice, bob})
    // bob   ──subscribe(q1)──▶ server
    //
    // alice disconnects → query_origin: q1→{bob}
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    let bob = ClientId::new();
    sm.add_client(alice);
    sm.add_client(bob);

    let q1 = QueryId(42);
    sm.query_origin.entry(q1).or_default().insert(alice);
    sm.query_origin.entry(q1).or_default().insert(bob);

    sm.remove_client(alice);

    assert!(
        sm.query_origin.contains_key(&q1),
        "q1 should still exist (bob is still interested)"
    );
    let clients = &sm.query_origin[&q1];
    assert!(!clients.contains(&alice), "alice should be removed");
    assert!(clients.contains(&bob), "bob should remain");
}

#[test]
fn remove_client_removes_query_origin_entry_when_last_client() {
    //
    // alice ──subscribe(q1)──▶ server   (query_origin: q1→{alice})
    //
    // alice disconnects → query_origin: empty
    //
    let mut sm = SyncManager::new();

    let alice = ClientId::new();
    sm.add_client(alice);

    let q1 = QueryId(42);
    sm.query_origin.entry(q1).or_default().insert(alice);

    sm.remove_client(alice);

    assert!(
        !sm.query_origin.contains_key(&q1),
        "q1 entry should be removed when last client disconnects"
    );
}
