//! Catalogue, snapshot, branch metadata, parking, replay, and resume.

use super::*;

#[test]
fn db_sync_surface_round_trips_subscription_to_client() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);

    seed(&server, "todos", cells("from server", false, owner));

    // Wire the two Dbs together and subscribe on the client.
    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    // Drive: client announces the shape -> server serves -> client applies.
    client.tick().unwrap(); // RegisterShape + Subscribe upstream
    server.tick().unwrap(); // ViewUpdate downstream
    client.tick().unwrap(); // apply, push the subscription event

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

    // A later server write propagates incrementally on the next round trip.
    seed(&server, "todos", cells("second", true, owner));
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(prepared_read(&client, &query).len(), 2);
}

#[test]
fn large_logical_snapshot_crosses_byte_peer_transport_and_settles() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0x71; 16]);
    let client_author = AuthorId::from_bytes([0x72; 16]);
    let server = open_core(0x73, AuthorId::SYSTEM, &schema);
    let client = open_db(0x74, client_author, &schema);
    let expected = 900;

    for idx in 0..expected {
        seed(
            &server,
            "todos",
            cells(&format!("row-{idx}-{}", "x".repeat(4096)), false, owner),
        );
    }

    let (client_transport, server_transport) = byte_duplex_uncompressed();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, global_subscribe_opts()).unwrap();
    let opened = block_on(subscription.next_raw()).unwrap();
    assert!(!event_settled(&opened));
    assert!(opened_rows(opened).is_empty());

    for _ in 0..200 {
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();

        while let Some(event) = subscription.try_next_event() {
            let settled = event_settled(&event);
            let snapshot = snapshot_from_event(event);
            if settled {
                assert_eq!(snapshot.rows.len(), expected);
                return;
            }
        }
    }

    let rows = prepared_read(&client, &query);
    panic!(
        "large logical snapshot subscription did not settle; currently visible rows={}",
        rows.len()
    );
}

#[test]
fn offline_branch_creation_and_commit_sync_metadata_before_data() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc1, identity, &schema);
    let branch = BranchId::from_bytes([0x42; 16]);
    client.create_branch_with_id(branch).unwrap();
    let write = client
        .insert_on_branch(branch, "todos", cells("offline branch", false, identity))
        .unwrap();
    let branch_row = write.row_uuid();
    assert!(server.node().borrow().branch_record(branch).is_none());

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, identity);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    let record = client
        .node
        .node
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    assert_eq!(record.created_by, identity);
    let received = server
        .node()
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    assert_eq!(received.branch_id, record.branch_id);
    assert_eq!(received.created_by, record.created_by);
    assert_eq!(received.parent, record.parent);
    assert_eq!(
        received.base.as_ref().map(|base| base.global_base),
        record.base.as_ref().map(|base| base.global_base)
    );
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_record(write.mergeable_tx_id())
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch)
    );
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = server
        .node()
        .borrow_mut()
        .query_rows_on_branch(branch, &shape, &binding)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), branch_row);
}

#[test]
fn fixed_schema_db_branch_and_bootstrap_writes_retain_authored_schema() {
    let base = schema();
    let evolved_schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]);
    let evolved = SchemaVersion::new(evolved_schema.clone());
    let identity = AuthorId::from_bytes([0xc2; 16]);
    let writer = open_db(0xc2, identity, &base);
    let publication = SchemaLineagePublication::new(
        evolved.clone(),
        MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "todos".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "body".to_owned(),
                    default: Value::String("default-body".to_owned()),
                }],
            }],
        ),
        Vec::<String>::new(),
        Vec::<String>::new(),
    );
    writer
        .node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        })
        .unwrap();
    writer
        .node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorId::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            },
        })
        .unwrap();

    let branch = BranchId::from_bytes([0x52; 16]);
    writer.create_branch_with_id(branch).unwrap();
    let write = writer
        .insert_on_branch(branch, "todos", cells("authored-base", false, identity))
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap()
    else {
        panic!("commit unit expected");
    };
    assert_eq!(versions[0].schema_version(), base.version_id());

    let receiver = open_db(0xc3, identity, &evolved_schema);
    receiver
        .node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_snapshot(writer.node.node.borrow().catalogue_snapshot().unwrap())
        .unwrap();
    receiver.create_branch_with_id(branch).unwrap();
    receiver
        .node
        .node
        .borrow_mut()
        .ingest_relay_commit_unit(tx, versions)
        .unwrap();
    let shape = Query::from("todos").validate(&evolved_schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let rows = receiver
        .node
        .node
        .borrow_mut()
        .query_rows_on_branch(branch, &shape, &binding)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell_at(3),
        Some(Value::String("default-body".to_owned()))
    );

    let seeded_row = RowUuid::from_bytes([0x53; 16]);
    let seeded_tx = writer
        .seed_settled_mergeable_for_bootstrap(
            "todos",
            seeded_row,
            identity,
            cells("seeded-base", true, identity),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = writer
        .node
        .node
        .borrow_mut()
        .commit_unit_for(seeded_tx)
        .unwrap()
    else {
        panic!("commit unit expected");
    };
    assert_eq!(versions[0].schema_version(), base.version_id());
    receiver
        .node
        .node
        .borrow_mut()
        .ingest_relay_commit_unit(tx, versions)
        .unwrap();
    let rows = receiver
        .node
        .node
        .borrow_mut()
        .query_rows(&shape, &binding, DurabilityTier::Local)
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), seeded_row);
    assert_eq!(
        rows[0].cell_at(3),
        Some(Value::String("default-body".to_owned()))
    );
}

#[test]
fn session_branch_metadata_rejects_creator_mismatch() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId::from_bytes([0x42; 16]);
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport
        .send(SyncMessage::BranchMetadata(BranchMetadata {
            branch_id: branch,
            created_by: AuthorId::from_bytes([0xee; 16]),
            parent: Some(BranchId::from_bytes([0xdd; 16])),
            base: None,
            open: false,
        }))
        .unwrap();
    assert!(subscriber.borrow_mut().tick().is_err());
    assert!(server.node().borrow().branch_record(branch).is_none());
}

#[test]
fn session_branch_metadata_rejects_malformed_initial_shapes() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let source = open_core(0xc1, identity, &schema);
    let branch = BranchId::from_bytes([0x49; 16]);
    let record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, identity)
        .unwrap();
    let canonical = BranchMetadata::from(&record);
    let mut discarded = canonical.clone();
    discarded.open = false;
    let mut parented = canonical.clone();
    parented.parent = Some(BranchId::from_bytes([0xdd; 16]));
    let mut arbitrary_owner = canonical.clone();
    arbitrary_owner.base.as_mut().unwrap().owner = NodeUuid::from_bytes([0xee; 16]);
    let mut local_tail = canonical.clone();
    local_tail.base.as_mut().unwrap().local_base = TxTime(1);
    let mut dotted = canonical;
    dotted
        .base
        .as_mut()
        .unwrap()
        .dots
        .push(TxId::new(TxTime(1), NodeUuid(uuid::Uuid::nil())));

    for metadata in [discarded, parented, arbitrary_owner, local_tail, dotted] {
        let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
        let (mut client_transport, server_transport) = duplex();
        let subscriber = server.accept_subscriber(server_transport, identity);
        client_transport
            .send(SyncMessage::BranchMetadata(metadata))
            .unwrap();
        assert!(subscriber.borrow_mut().tick().is_err());
        assert!(server.node().borrow().branch_record(branch).is_none());
    }
}

#[test]
fn empty_branch_metadata_retries_after_unacked_reopen() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let node_uuid = NodeUuid::from_bytes([0xc1; 16]);
    let branch = BranchId::from_bytes([0x4a; 16]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
    }))
    .unwrap();
    client.create_branch_with_id(branch).unwrap();
    let first_server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let (client_transport, server_transport) = duplex();
    let upstream = client.connect_upstream(client_transport);
    let _subscriber = first_server.accept_subscriber(server_transport, identity);
    upstream.borrow_mut().tick().unwrap();
    first_server.tick().unwrap();
    assert!(first_server.node().borrow().branch_record(branch).is_some());
    drop(upstream);
    client.close().unwrap();
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
    }))
    .unwrap();
    assert_eq!(
        reopened
            .node
            .node
            .borrow()
            .pending_branch_metadata_uploads()
            .len(),
        1
    );
    let replay_server = open_core(0x6e, AuthorId::SYSTEM, &reopened.schema);
    let (client_transport, server_transport) = duplex();
    let upstream = reopened.connect_upstream(client_transport);
    let _subscriber = replay_server.accept_subscriber(server_transport, identity);
    upstream.borrow_mut().tick().unwrap();
    replay_server.tick().unwrap();
    upstream.borrow_mut().tick().unwrap();
    assert!(
        replay_server
            .node()
            .borrow()
            .branch_record(branch)
            .is_some()
    );
    assert!(
        reopened
            .node
            .node
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
}

#[test]
fn acknowledged_open_accepts_remote_discard_and_recovers_it() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let node_uuid = NodeUuid::from_bytes([0xc2; 16]);
    let branch = BranchId::from_bytes([0x4d; 16]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
    }))
    .unwrap();
    let authority = open_core(0x5e, AuthorId::SYSTEM, &schema);
    client.create_branch_with_id(branch).unwrap();
    let (client_transport, authority_transport) = duplex();
    let upstream = client.connect_upstream(client_transport);
    let subscriber = authority.accept_subscriber(authority_transport, identity);
    client.tick().unwrap();
    authority.tick().unwrap();
    client.tick().unwrap();
    assert!(
        client
            .node
            .node
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
    drop(upstream);
    drop(subscriber);

    authority
        .node()
        .borrow_mut()
        .discard_branch(branch)
        .unwrap();
    let discarded = BranchMetadata::from(authority.node().borrow().branch_record(branch).unwrap());
    assert!(!discarded.open);
    let (client_transport, mut trusted_remote) = duplex();
    let upstream = client.connect_upstream(client_transport);
    trusted_remote
        .send(SyncMessage::BranchMetadata(discarded.clone()))
        .unwrap();
    upstream.borrow_mut().tick().unwrap();
    assert_eq!(
        BranchMetadata::from(client.node.node.borrow().branch_record(branch).unwrap()),
        discarded
    );
    drop(upstream);
    client.close().unwrap();
    drop(client);

    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: None,
    }))
    .unwrap();
    assert_eq!(
        BranchMetadata::from(reopened.node.node.borrow().branch_record(branch).unwrap()),
        discarded
    );
}

#[test]
fn edge_durably_relays_empty_branch_creation_and_discard_after_reopen() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let edge_uuid = NodeUuid::from_bytes([0xe1; 16]);
    let branch = BranchId::from_bytes([0x4c; 16]);
    let client = open_db(0xc1, identity, &schema);
    let authority = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let edge_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();

    let edge_storage = RocksDbStorage::open(edge_dir.path(), &refs).unwrap();
    let edge = Node::new(
        NodeState::new_history_complete(edge_uuid, schema.clone(), edge_storage).unwrap(),
    );
    client.create_branch_with_id(branch).unwrap();
    let (client_transport, edge_transport) = duplex();
    let client_link = client.connect_upstream(client_transport);
    let edge_downstream = edge.accept_subscriber(edge_transport, identity);
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    assert!(authority.node().borrow().branch_record(branch).is_none());
    drop(client_link);
    drop(edge_downstream);
    drop(edge);

    // The edge acknowledged the client hop, but its independent authority hop
    // remains durable across restart.
    let edge_storage = RocksDbStorage::open(edge_dir.path(), &refs).unwrap();
    let edge = Node::new(
        NodeState::new_history_complete(edge_uuid, schema.clone(), edge_storage).unwrap(),
    );
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    let (edge_transport, authority_transport) = duplex();
    let edge_upstream = edge.connect_upstream(edge_transport);
    let authority_downstream = authority.accept_subscriber_with_trust(
        authority_transport,
        identity,
        CommitUnitTrust::TrustedBackend,
    );
    edge.tick().unwrap();
    authority.tick().unwrap();
    edge.tick().unwrap();
    assert!(authority.node().borrow().branch_record(branch).is_some());
    assert!(
        edge.node()
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
    drop(edge_upstream);
    drop(authority_downstream);

    // A delayed exact retry from the downstream author is acknowledged but
    // does not reopen an already-acknowledged upstream relay.
    let open_metadata =
        BranchMetadata::from(client.node.node.borrow().branch_record(branch).unwrap());
    let (mut retry_transport, edge_transport) = duplex();
    let retry_downstream = edge.accept_subscriber(edge_transport, identity);
    retry_transport
        .send(SyncMessage::BranchMetadata(open_metadata))
        .unwrap();
    retry_downstream.borrow_mut().tick().unwrap();
    assert!(
        edge.node()
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
    drop(retry_downstream);

    client
        .node
        .node
        .borrow_mut()
        .discard_branch(branch)
        .unwrap();
    let (client_transport, edge_transport) = duplex();
    let client_link = client.connect_upstream(client_transport);
    let edge_downstream = edge.accept_subscriber(edge_transport, identity);
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    assert!(BranchMetadata::from(authority.node().borrow().branch_record(branch).unwrap()).open);
    drop(client_link);
    drop(edge_downstream);
    drop(edge);

    let edge_storage = RocksDbStorage::open(edge_dir.path(), &refs).unwrap();
    let edge = Node::new(
        NodeState::new_history_complete(edge_uuid, schema.clone(), edge_storage).unwrap(),
    );
    assert_eq!(
        edge.node().borrow().pending_branch_metadata_uploads().len(),
        1
    );
    let (edge_transport, authority_transport) = duplex();
    let _edge_upstream = edge.connect_upstream(edge_transport);
    let _authority_downstream = authority.accept_subscriber_with_trust(
        authority_transport,
        identity,
        CommitUnitTrust::TrustedBackend,
    );
    edge.tick().unwrap();
    authority.tick().unwrap();
    edge.tick().unwrap();
    assert!(!BranchMetadata::from(authority.node().borrow().branch_record(branch).unwrap()).open);
    assert!(
        edge.node()
            .borrow()
            .pending_branch_metadata_uploads()
            .is_empty()
    );
}

#[test]
fn session_branch_data_parks_until_authenticated_metadata_arrives() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let writer = open_core(0xc1, identity, &schema);
    let branch = BranchId::from_bytes([0x47; 16]);
    let record = writer
        .node()
        .borrow_mut()
        .create_branch_as(branch, identity)
        .unwrap();
    let tx_id = writer
        .node()
        .borrow_mut()
        .commit_mergeable_on_branch_settled(
            branch,
            MergeableCommit::new("todos", row(0x47), 1)
                .made_by(identity)
                .cells(cells("data first", false, identity)),
        )
        .unwrap();
    let unit = writer.node().borrow_mut().commit_unit_for(tx_id).unwrap();
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport.send(unit).unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        server
            .node()
            .borrow_mut()
            .transaction_record(tx_id)
            .is_none()
    );
    assert!(matches!(
        try_recv_subscriber_payload(client_transport.as_mut()),
        Some(SyncMessage::FetchBranchMetadata { branches }) if branches == vec![branch]
    ));

    client_transport
        .send(SyncMessage::BranchMetadata((&record).into()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_record(tx_id)
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch)
    );
}

#[test]
fn session_branch_metadata_parks_until_snapshot_base_arrives() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let source = open_core(0xc1, identity, &schema);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let base_write = source
        .insert("todos", cells("base first", false, identity))
        .unwrap();
    let base_unit = source
        .node()
        .borrow_mut()
        .commit_unit_for(base_write.mergeable_tx_id())
        .unwrap();
    let branch = BranchId::from_bytes([0x48; 16]);
    let record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, identity)
        .unwrap();
    assert_eq!(record.base.as_ref().unwrap().global_base, GlobalSeq(1));
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport
        .send(SyncMessage::BranchMetadata((&record).into()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(server.node().borrow().branch_record(branch).is_none());

    client_transport.send(base_unit).unwrap();
    subscriber.borrow_mut().tick().unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(server.node().borrow().branch_record(branch).is_some());
}

#[test]
fn locally_created_branch_and_commit_survive_rocks_reopen() {
    let schema = schema();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let node_uuid = NodeUuid::from_bytes([0xc1; 16]);
    let branch = BranchId::from_bytes([0x43; 16]);
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let client = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xc1))),
    }))
    .unwrap();
    client.create_branch_with_id(branch).unwrap();
    let write = client
        .insert_on_branch(branch, "todos", cells("durable offline", false, identity))
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    let expected = client
        .node
        .node
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    client.close().unwrap();
    drop(client);
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema,
        storage,
        identity: DbIdentity {
            node: node_uuid,
            author: identity,
        },
        id_source: Some(Box::new(SeededRowIdSource::new(0xc2))),
    }))
    .unwrap();
    assert_eq!(
        reopened.node.node.borrow().branch_record(branch),
        Some(&expected)
    );
    assert!(reopened.write_state(tx_id).is_ok());

    // Recovery restores both independent durable outboxes: metadata must be
    // replayed and admitted before the branch-target transaction can land.
    let server = open_core(0x5e, AuthorId::SYSTEM, &reopened.schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = reopened.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber(server_transport, identity);
    reopened.tick().unwrap();
    server.tick().unwrap();
    reopened.tick().unwrap();
    server.tick().unwrap();
    assert_eq!(
        server.node().borrow().branch_record(branch),
        Some(&expected)
    );
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_record(tx_id)
            .unwrap()
            .target_lineage,
        crate::tx::BranchLineage::Branch(branch)
    );
}

#[test]
fn trusted_branch_snapshot_round_trips_without_receiver_reauthoring() {
    let schema = schema();
    let backend_identity = AuthorId::from_bytes([0xb0; 16]);
    let receiver_uuid = NodeUuid::from_bytes([0x5e; 16]);
    let snapshot_owner = NodeUuid::from_bytes([0xa7; 16]);
    let branch = BranchId::from_bytes([0x4b; 16]);
    let snapshot = crate::tx::Snapshot::exclusive_base(
        snapshot_owner,
        GlobalSeq(0),
        TxTime(7),
        vec![TxId::new(TxTime(8), snapshot_owner)],
    )
    .unwrap();
    let metadata = BranchMetadata {
        branch_id: branch,
        created_by: backend_identity,
        parent: None,
        base: Some(snapshot.clone()),
        open: true,
    };
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let target =
        Node::new(NodeState::new_history_complete(receiver_uuid, schema.clone(), storage).unwrap());
    let (mut backend_transport, server_transport) = duplex();
    let subscriber = target.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );
    backend_transport
        .send(SyncMessage::BranchMetadata(metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        target.node().borrow().branch_record(branch).unwrap().base,
        Some(snapshot.clone())
    );

    drop(subscriber);
    drop(target);
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = NodeState::new_history_complete(receiver_uuid, schema, storage).unwrap();
    assert_eq!(
        BranchMetadata::from(reopened.branch_record(branch).unwrap()),
        metadata
    );
}

#[test]
fn trusted_backend_replays_branch_metadata_over_transport() {
    // Internal trust-boundary test: raw routing metadata is intentionally only
    // accepted on a trusted backend transport and has no public client facade.
    let schema = schema();
    let backend_identity = AuthorId::from_bytes([0xb0; 16]);
    let source = open_core(0xb0, AuthorId::SYSTEM, &schema);
    let target = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId::from_bytes([0x44; 16]);
    let record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, backend_identity)
        .unwrap();
    let metadata = BranchMetadata::from(&record);
    let (mut backend_transport, server_transport) = duplex();
    let subscriber = target.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );

    backend_transport
        .send(SyncMessage::BranchMetadata(metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(target.node().borrow().branch_record(branch), Some(&record));

    backend_transport
        .send(SyncMessage::BranchMetadata(metadata))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(target.node().borrow().branch_record(branch), Some(&record));
}

#[test]
fn trusted_backend_discards_branch_metadata_once_and_recovers_it() {
    // Internal trust/storage boundary test: lifecycle metadata is carried only
    // by trusted backend links and must be durable before branch data is routed.
    let schema = schema();
    let backend_identity = AuthorId::from_bytes([0xb0; 16]);
    let node_uuid = NodeUuid::from_bytes([0x5e; 16]);
    let branch = BranchId::from_bytes([0x46; 16]);
    let source = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let open_record = source
        .node()
        .borrow_mut()
        .create_branch_as(branch, backend_identity)
        .unwrap();
    let open_metadata = BranchMetadata::from(&open_record);
    let mut discarded_metadata = open_metadata.clone();
    discarded_metadata.open = false;
    let dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let target =
        Node::new(NodeState::new_history_complete(node_uuid, schema.clone(), storage).unwrap());
    let (mut backend_transport, server_transport) = duplex();
    let subscriber = target.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );

    backend_transport
        .send(SyncMessage::BranchMetadata(open_metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    backend_transport
        .send(SyncMessage::BranchMetadata(discarded_metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    backend_transport
        .send(SyncMessage::BranchMetadata(discarded_metadata.clone()))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    let discarded_record = target
        .node()
        .borrow()
        .branch_record(branch)
        .cloned()
        .unwrap();
    assert_eq!(discarded_record.created_by, open_record.created_by);
    assert_eq!(discarded_record.parent, open_record.parent);
    assert_eq!(discarded_record.base, open_record.base);
    assert!(!BranchMetadata::from(&discarded_record).open);

    drop(subscriber);
    drop(target);
    let storage = RocksDbStorage::open(dir.path(), &refs).unwrap();
    let reopened = Node::new(NodeState::new_history_complete(node_uuid, schema, storage).unwrap());
    assert_eq!(
        reopened.node().borrow().branch_record(branch),
        Some(&discarded_record)
    );

    let (mut reverse_transport, server_transport) = duplex();
    let reverse = reopened.accept_subscriber_with_trust(
        server_transport,
        backend_identity,
        CommitUnitTrust::TrustedBackend,
    );
    reverse_transport
        .send(SyncMessage::BranchMetadata(open_metadata))
        .unwrap();
    assert!(reverse.borrow_mut().tick().is_err());

    let mut changed_creator = discarded_metadata.clone();
    changed_creator.created_by = AuthorId::from_bytes([0xee; 16]);
    let mut changed_parent = discarded_metadata.clone();
    changed_parent.parent = Some(BranchId::from_bytes([0xdd; 16]));
    let mut changed_base = discarded_metadata;
    changed_base.base = None;
    for mutation in [changed_creator, changed_parent, changed_base] {
        let (mut mutation_transport, server_transport) = duplex();
        let mutation_connection = reopened.accept_subscriber_with_trust(
            server_transport,
            backend_identity,
            CommitUnitTrust::TrustedBackend,
        );
        mutation_transport
            .send(SyncMessage::BranchMetadata(mutation))
            .unwrap();
        assert!(mutation_connection.borrow_mut().tick().is_err());
    }
    assert_eq!(
        reopened.node().borrow().branch_record(branch),
        Some(&discarded_record)
    );
}

#[test]
fn subscriber_connection_serves_branch_subscription_with_known_state_and_unsubscribe() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    server
        .node()
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    server
        .node()
        .borrow_mut()
        .commit_mergeable_on_branch_settled(
            branch,
            MergeableCommit::new("todos", row(0x42), 10).cells(cells(
                "branch-only",
                false,
                client_author,
            )),
        )
        .expect("commit branch row");

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let read_opts = branch_read_opts();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: read_opts.read_view,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: opts.clone(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let initial = loop {
        match try_recv_subscriber_payload(client_transport.as_mut()) {
            Some(SyncMessage::BranchMetadata(_)) => continue,
            Some(message) => break message,
            None => panic!("expected initial branch view update"),
        }
    };
    assert_view_update_for_subscription(initial, subscription);
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: GlobalSeq::default(),
            }),
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    let known_state_update = loop {
        match try_recv_subscriber_payload(client_transport.as_mut()) {
            Some(SyncMessage::BranchMetadata(_)) => continue,
            Some(message) => break message,
            None => panic!("known-state branch resubscribe must remain served"),
        }
    };
    assert_view_update_for_subscription(known_state_update, subscription);
    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber { served, .. } = &subscriber_ref.link else {
        unreachable!("expected subscriber link")
    };
    assert!(
        !served.contains_key(&subscription),
        "unsubscribing must retire the branch maintained view"
    );
}

#[test]
fn subscriber_connection_serves_branch_subscription_alongside_root_subscription() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0xa1; 16]);
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    server
        .node()
        .borrow_mut()
        .create_branch(branch)
        .expect("create branch");
    seed(&server, "todos", cells("first", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let supported_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let branch_opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: branch_read_opts().read_view,
        ..RegisterShapeOptions::default()
    };
    let branch_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: branch_opts.read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: supported_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected initial supported view update"),
        supported_subscription,
    );

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: branch_opts,
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: branch_subscription,
            values: Vec::new(),
            known_state: None,
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    let branch_update = loop {
        match try_recv_subscriber_payload(client_transport.as_mut()) {
            Some(SyncMessage::BranchMetadata(_)) => continue,
            Some(message) => break message,
            None => panic!("expected initial branch view update"),
        }
    };
    assert_view_update_for_subscription(branch_update, branch_subscription);

    seed(&server, "todos", cells("second", false, owner));
    subscriber.borrow_mut().tick().unwrap();
    assert_view_update_for_subscription(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected supported update after branch admission"),
        supported_subscription,
    );
}
