//! Schema publication, registry growth, and live-runtime compatibility tests.

use super::*;

// Keep large async constructor/read futures in helper frames so this multi-open
// lifecycle fixture runs on an ordinary Rust test thread's default stack.
fn open_offline_catalogue_replica(
    schema: JazzSchema,
    storage: groove::storage::TestStorage,
    identity: DbIdentity,
    backend: bool,
) -> Db<groove::storage::TestStorage> {
    let config = DbConfig::new(schema, storage, identity);
    if backend {
        // SAFETY: this fixture explicitly admits Bob's synthetic backend identity.
        block_on(Box::pin(unsafe {
            Db::open_history_complete_with_backend_attribution(config)
        }))
        .expect("open backend replica sync owner")
    } else {
        block_on(Box::pin(Db::open(config))).expect("open replica sync owner")
    }
}

fn offline_catalogue_remote_read<'a>(
    db: &'a Db<groove::storage::TestStorage>,
    query: &'a PreparedQuery,
) -> Pin<Box<dyn Future<Output = Result<Vec<CurrentRow>, Error>> + 'a>> {
    Box::pin(db.all(
        query,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
    ))
}

fn offline_catalogue_serialized_read(
    db: &Db<groove::storage::TestStorage>,
) -> Pin<Box<dyn Future<Output = Result<SerializedReadResult, Error>> + '_>> {
    offline_catalogue_serialized_read_with_deadline(db, None)
}

fn offline_catalogue_serialized_read_with_deadline(
    db: &Db<groove::storage::TestStorage>,
    deadline: Option<std::time::Instant>,
) -> Pin<Box<dyn Future<Output = Result<SerializedReadResult, Error>> + '_>> {
    Box::pin(async move {
        let query = postcard::to_allocvec(&Query::from("items")).unwrap();
        db.all_serialized_query(
            &query,
            ReadOpts {
                tier: DurabilityTier::Global,
                ..ReadOpts::default()
            },
            None,
            None,
            None,
            false,
            || deadline.is_some_and(|deadline| std::time::Instant::now() >= deadline),
            |_| {},
        )
        .await
    })
}

fn tick_offline_catalogue_connection(
    connection: &Rc<LocalMutex<PeerConnection<groove::storage::TestStorage>>>,
) {
    block_on(Box::pin(connection.borrow_mut().tick())).unwrap();
}

fn assert_offline_catalogue_all_denied(
    db: &Db<groove::storage::TestStorage>,
    query: &PreparedQuery,
) {
    assert!(block_on(Box::pin(db.all(query, ReadOpts::default()))).is_err());
}

fn assert_offline_catalogue_relation_denied(
    db: &Db<groove::storage::TestStorage>,
    query: &PreparedQuery,
) {
    assert!(
        block_on(Box::pin(
            db.all_relation_snapshot(query, ReadOpts::default())
        ))
        .is_err()
    );
    assert!(
        block_on(Box::pin(db.all_relation_snapshot_for_identity(
            query,
            ReadOpts::default(),
            AuthorSubject::SYSTEM
        )))
        .is_err()
    );
}

fn assert_offline_catalogue_subscription_denied(
    db: &Db<groove::storage::TestStorage>,
    query: &PreparedQuery,
) {
    assert!(block_on(Box::pin(db.subscribe(query, ReadOpts::default()))).is_err());
}

/// Alice reopens an offline replica directly at Bob's published next schema.
/// Bob A -> Alice A -> offline -> Bob A→B -> Alice opens B -> catalogue -> rows.
/// Uses the binding-facing Db boundary to verify durable recovery independently
/// of environment startup; full transport authentication belongs to shell tests.
#[test]
fn offline_replica_opens_requested_schema_only_after_published_lineage() {
    assert_offline_replica_schema_bootstrap(false);
}

/// Alice's backend replica uses complete-history attribution without inventing B.
/// Bob A -> offline Alice A -> Bob A→B -> Alice opens B -> catalogue -> rows.
#[test]
fn offline_backend_opens_requested_schema_only_after_published_lineage() {
    assert_offline_replica_schema_bootstrap(true);
}

fn assert_offline_replica_schema_bootstrap(backend: bool) {
    let make_schema = |extra: bool| {
        let builder = PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("label", PublicColumnType::Text)
                .policies(
                    public_legacy_write_policy(PublicPolicyExpr::True)
                        .with_select(PublicPolicyExpr::True),
                ),
        );
        build_public_db_test_schema(if extra {
            builder.table(
                PublicTableSchemaBuilder::new("controls")
                    .column("value", PublicColumnType::Text)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
            )
        } else {
            builder
        })
    };
    let base = make_schema(false);
    let target = make_schema(true);
    let families = target.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, _) = groove::storage::TestStorage::controlled(&refs);
    let reopen = storage.clone();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xe1; 16]),
        author: if backend {
            AuthorSubject::SYSTEM
        } else {
            AuthorSubject::for_test_bytes([0xe1; 16])
        },
    };
    let trust = if backend {
        CommitUnitTrust::TrustedBackend
    } else {
        CommitUnitTrust::Session
    };
    let alice = open_offline_catalogue_replica(base.clone(), storage, identity, backend);
    let bob = open_core(0xe2, AuthorSubject::SYSTEM, &base);
    let (upstream, downstream) = duplex();
    let accepted = bob.accept_subscriber_with_trust(downstream, identity.author, trust);
    let connection = block_on(alice.connect_upstream(upstream));
    for _ in 0..20 {
        bob.tick().unwrap();
        alice.tick().unwrap();
    }
    block_on(alice.detach_connection_async(&connection)).unwrap();
    drop(connection);
    drop(accepted);
    // A local pending edit must survive both recovery and later catalogue sync.
    let write = alice
        .insert(
            "items",
            BTreeMap::from([(
                "label".to_owned(),
                Value::String("retained pending edit".to_owned()),
            )]),
            Default::default(),
        )
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    let retained_row = write.row_uuid();
    let retained_query = alice.prepare_query(&Query::from("items")).unwrap();
    drop(write);
    block_on(alice.close()).unwrap();
    drop(alice);

    let storage = block_on(reopen.clone().reopen(families.clone())).unwrap();
    let alice = open_offline_catalogue_replica(target.clone(), storage, identity, backend);
    assert!(alice.read(&retained_query).is_err());
    assert!(alice.read_profiled(&retained_query).is_err());
    assert_offline_catalogue_all_denied(&alice, &retained_query);
    assert_offline_catalogue_relation_denied(&alice, &retained_query);
    assert!(block_on(alice.local_current_row("items", retained_row)).is_err());
    assert_offline_catalogue_subscription_denied(&alice, &retained_query);
    assert!(alice.attach_query(&retained_query).is_err());
    assert!(block_on(alice.begin_mergeable(OpenTransactionId::new())).is_err());
    assert!(block_on(alice.begin_exclusive(OpenTransactionId::new())).is_err());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    // Cancellation before the first catalogue must not affect a later waiter.
    let mut expired = offline_catalogue_serialized_read_with_deadline(
        &alice,
        Some(std::time::Instant::now() + std::time::Duration::from_millis(10)),
    );
    assert!(expired.as_mut().poll(&mut context).is_pending());
    std::thread::sleep(std::time::Duration::from_millis(20));
    match expired.as_mut().poll(&mut context) {
        std::task::Poll::Ready(Err(error)) => assert_eq!(error.code, ErrorCode::NotObserved),
        _ => panic!("schema admission must preserve the serialized read deadline"),
    }
    drop(expired);
    let mut cancelled = offline_catalogue_remote_read(&alice, &retained_query);
    assert!(cancelled.as_mut().poll(&mut context).is_pending());
    drop(cancelled);
    let mut closing = offline_catalogue_remote_read(&alice, &retained_query);
    assert!(closing.as_mut().poll(&mut context).is_pending());
    block_on(alice.close()).unwrap();
    assert!(matches!(
        closing.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(_))
    ));
    drop(closing);
    drop(alice);
    let storage = block_on(reopen.clone().reopen(families.clone())).unwrap();
    let alice = open_offline_catalogue_replica(target.clone(), storage, identity, backend);
    let mut unpublished = offline_catalogue_remote_read(&alice, &retained_query);
    assert!(unpublished.as_mut().poll(&mut context).is_pending());
    let unavailable = alice.prepare_query(&Query::from("items")).unwrap_err();
    assert!(
        unavailable
            .to_string()
            .contains("awaiting published catalogue admission")
    );
    assert!(block_on(alice.register_schema_view(target.clone())).is_err());
    assert!(
        alice
            .insert(
                "items",
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String("must not be written under A".to_owned())
                ),]),
                Default::default()
            )
            .is_err()
    );
    let (upstream, downstream) = duplex();
    let detached = bob.accept_subscriber_with_trust(downstream, identity.author, trust);
    let connection = block_on(alice.connect_upstream(upstream));
    block_on(alice.detach_connection_async(&connection)).unwrap();
    assert!(matches!(
        unpublished.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(_))
    ));
    drop(unpublished);
    drop(connection);
    drop(detached);
    // A live authority that has not published B cannot authorize the request.
    let (upstream, downstream) = duplex();
    let accepted = bob.accept_subscriber_with_trust(downstream, identity.author, trust);
    let connection = block_on(alice.connect_upstream(upstream));
    let (mut absent_foreground, absent_transport) = duplex();
    let absent_connection = alice.accept_subscriber(absent_transport, identity.author);
    let mut unpublished = offline_catalogue_remote_read(&alice, &retained_query);
    assert!(unpublished.as_mut().poll(&mut context).is_pending());
    for _ in 0..20 {
        bob.tick().unwrap();
        alice.tick().unwrap();
    }
    assert!(matches!(
        unpublished.as_mut().poll(&mut context),
        std::task::Poll::Ready(Err(_))
    ));
    assert!(
        matches!(
            absent_foreground.try_recv(),
            Some(SyncMessage::CatalogueSnapshot(_))
        ),
        "validated absence must reach the foreground instead of hanging"
    );
    block_on(alice.detach_connection_async(&absent_connection)).unwrap();
    drop(absent_connection);
    drop(unpublished);
    assert!(alice.prepare_query(&Query::from("items")).is_err());
    assert!(block_on(alice.register_schema_view(target.clone())).is_err());
    block_on(alice.detach_connection_async(&connection)).unwrap();
    drop(connection);
    drop(accepted);
    // Closing while offline neither admits B nor destroys A's data.
    block_on(alice.close()).unwrap();
    drop(alice);
    let storage = block_on(reopen.clone().reopen(families.clone())).unwrap();
    let alice = open_offline_catalogue_replica(base.clone(), storage, identity, backend);
    assert_eq!(
        prepared_all(&alice, &Query::from("items"), ReadOpts::default()).len(),
        1
    );
    block_on(alice.close()).unwrap();
    drop(alice);

    let lens = MigrationLens::new(
        base.version_id(),
        target.version_id(),
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![],
        }],
    )
    .unwrap();
    let publication = bob
        .author_schema_lineage_publication(
            SchemaVersion::new(target.clone()),
            lens,
            vec!["controls".to_owned()],
            Vec::<String>::new(),
        )
        .unwrap();
    bob.publish_schema_with_lens(1, publication).unwrap();
    bob.activate_catalogue_schema_for_test(CurrentWriteSchema {
        revision: 1,
        schema: target.version_id(),
    })
    .unwrap();
    let storage = block_on(reopen.clone().reopen(families.clone())).unwrap();
    let alice = open_offline_catalogue_replica(target.clone(), storage, identity, backend);
    let (mut foreground, foreground_transport) = duplex();
    let foreground_connection = alice.accept_subscriber(foreground_transport, identity.author);
    alice.tick().unwrap();
    assert!(
        foreground.try_recv().is_none(),
        "pending owner must not announce its recovered old catalogue"
    );
    let mut waiting = offline_catalogue_serialized_read(&alice);
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    let (upstream, downstream) = duplex();
    let accepted = bob.accept_subscriber_with_trust(downstream, identity.author, trust);
    let old_connection = block_on(alice.connect_upstream(upstream));
    let (replacement_upstream, replacement_downstream) = duplex();
    let replacement_accepted =
        bob.accept_subscriber_with_trust(replacement_downstream, identity.author, trust);
    let connection = block_on(alice.connect_upstream(replacement_upstream));
    assert!(
        matches!(waiting.as_mut().poll(&mut context), Poll::Ready(Err(_))),
        "replacement must reject old waiter"
    );
    drop(waiting);
    let mut waiting = offline_catalogue_serialized_read(&alice);
    assert!(waiting.as_mut().poll(&mut context).is_pending());
    for _ in 0..30 {
        bob.tick().unwrap();
        tick_offline_catalogue_connection(&old_connection);
    }
    assert!(
        waiting.as_mut().poll(&mut context).is_pending(),
        "stale catalogue must not resolve new waiter"
    );
    tick_offline_catalogue_connection(&foreground_connection);
    assert!(
        foreground.try_recv().is_none(),
        "stale upstream must not announce a downstream catalogue"
    );
    for _ in 0..30 {
        bob.tick().unwrap();
        tick_offline_catalogue_connection(&connection);
    }
    assert!(
        foreground.try_recv().is_none(),
        "stale upstream must not admit the owner's downstream catalogue"
    );
    alice.tick().unwrap();
    assert!(matches!(
        foreground.try_recv(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    block_on(alice.detach_connection_async(&foreground_connection)).unwrap();
    drop(foreground_connection);
    block_on(alice.detach_connection_async(&old_connection)).unwrap();
    drop(old_connection);
    drop(replacement_accepted);
    assert!(matches!(
        waiting.as_mut().poll(&mut context),
        std::task::Poll::Ready(Ok(_))
    ));
    drop(waiting);
    let rows = prepared_all(&alice, &Query::from("items"), ReadOpts::default());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0]
            .cell(
                target
                    .tables
                    .iter()
                    .find(|table| table.name == "items")
                    .unwrap(),
                "label"
            )
            .unwrap(),
        Value::String("retained pending edit".to_owned())
    );
    assert!(prepared_all(&alice, &Query::from("controls"), ReadOpts::default()).is_empty());
    block_on(alice.detach_connection_async(&connection)).unwrap();
    drop(connection);
    drop(accepted);
    block_on(alice.close()).unwrap();
    drop(alice);
    let storage = block_on(reopen.reopen(families)).unwrap();
    let alice = open_offline_catalogue_replica(target, storage, identity, backend);
    assert_eq!(
        prepared_all(&alice, &Query::from("items"), ReadOpts::default()).len(),
        1
    );
}

#[test]
fn trusted_snapshot_preserves_offline_enum_rows_and_reopens() {
    assert_snapshot_preserves_offline_enum_rows(false);
}

#[test]
fn trusted_snapshot_preserves_descendant_anchor_enum_rows_and_reopens() {
    assert_snapshot_preserves_offline_enum_rows(true);
}

fn assert_snapshot_preserves_offline_enum_rows(descendant: bool) {
    let scalar = PublicColumnType::ScalarEnum {
        name: "Status".to_owned(),
        variants: vec!["pending".to_owned(), "done".to_owned()],
    };
    let payload_type = PublicColumnType::EnumPayload {
        cases: vec![PublicEnumCaseDescriptor {
            name: "message".to_owned(),
            fields: vec![PublicColumnDescriptor::new(
                "level",
                PublicColumnType::Integer,
            )],
        }],
    };
    let make_schema = |extra: bool| {
        let table = PublicTableSchemaBuilder::new("items")
            .column("status", scalar.clone())
            .column("event", payload_type.clone())
            .column(
                "statuses",
                PublicColumnType::Array {
                    element: Box::new(scalar.clone()),
                },
            )
            .column(
                "events",
                PublicColumnType::Array {
                    element: Box::new(payload_type.clone()),
                },
            );
        let table = if extra {
            table.column("extra", PublicColumnType::Text)
        } else {
            table
        };
        build_public_db_test_schema(PublicSchemaBuilder::new().table(table))
    };
    let base = make_schema(false);
    let schema = make_schema(descendant);
    let table = &schema.tables[0];
    let event_column = table
        .columns
        .iter()
        .find(|column| column.name == "event")
        .unwrap();
    let ValueType::Enum(event_schema) = &event_column.column_type else {
        panic!("payload enum")
    };
    let event = Value::Enum(
        EnumValue::create(0, event_schema.cases[0].payload.clone(), &[Value::I32(7)]).unwrap(),
    );
    // Binding-facing Db accepts core cells; row_input! targets JazzClient values.
    let mut expected = BTreeMap::from([
        ("status".to_owned(), Value::EnumTag(1)),
        ("event".to_owned(), event.clone()),
        (
            "statuses".to_owned(),
            Value::Array(vec![Value::EnumTag(1), Value::EnumTag(0)]),
        ),
        ("events".to_owned(), Value::Array(vec![event])),
    ]);
    if descendant {
        expected.insert("extra".to_owned(), Value::String("offline".to_owned()));
    }
    let families = schema.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, _) = groove::storage::TestStorage::controlled(&refs);
    let reopen = storage.clone();
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([0xdd; 16]),
        author: AuthorSubject::SYSTEM,
    };
    let db = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xdd))),
    }))
    .unwrap();
    let write = db
        .insert("items", expected.clone(), Default::default())
        .unwrap();
    block_on(write.wait(DurabilityTier::Local)).unwrap();
    drop(write);
    let before = prepared_all(
        &db,
        &Query::from("items"),
        ReadOpts {
            tier: DurabilityTier::Local,
            ..ReadOpts::default()
        },
    );
    assert_eq!(before.len(), 1);
    let row = before[0].row_uuid();

    // Host-admitted backend trust is fixture plumbing; all row and reopen
    // assertions use Db. The two opens mint independent provisional manifests.
    let authority = open_core(0xde, AuthorSubject::SYSTEM, &base);
    if descendant {
        let evolved = SchemaVersion::new(schema.clone());
        let lens = MigrationLens::new(
            base.version_id(),
            evolved.id,
            vec![TableLens {
                source_table: "items".to_owned(),
                target_table: "items".to_owned(),
                ops: vec![LensOp::AddColumn {
                    column: "extra".to_owned(),
                    default: Value::String(String::new()),
                }],
            }],
        )
        .unwrap();
        let publication = authority
            .author_schema_lineage_publication(
                evolved.clone(),
                lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )
            .unwrap();
        authority.publish_schema_with_lens(1, publication).unwrap();
        authority
            .activate_catalogue_schema_for_test(CurrentWriteSchema {
                revision: 1,
                schema: evolved.id,
            })
            .unwrap();
    }
    let (upstream, downstream) = duplex();
    let accepted = authority.accept_subscriber_with_trust(
        downstream,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let connection = block_on(db.connect_upstream(upstream));
    for _ in 0..20 {
        authority.tick().unwrap();
        db.tick().unwrap();
    }
    let after = prepared_all(
        &db,
        &Query::from("items"),
        ReadOpts {
            tier: DurabilityTier::Local,
            ..ReadOpts::default()
        },
    );
    assert_eq!(after.len(), 1);
    assert_eq!(after[0].row_uuid(), row);
    for (column, value) in &expected {
        assert_eq!(after[0].cell(table, column).unwrap(), *value);
    }
    block_on(db.detach_connection_async(&connection)).unwrap();
    drop(connection);
    drop(accepted);
    block_on(db.close()).unwrap();
    drop(db);
    let storage = block_on(reopen.reopen(families)).unwrap();
    let reopened = block_on(Db::open(DbConfig {
        schema: schema.clone(),
        storage,
        identity,
        id_source: Some(Box::new(SeededRowIdSource::new(0xdf))),
    }))
    .expect("authority UUIDs and persisted enum registries remain coherent on reopen");
    let rows = prepared_all(
        &reopened,
        &Query::from("items"),
        ReadOpts {
            tier: DurabilityTier::Local,
            ..ReadOpts::default()
        },
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);
    for (column, value) in &expected {
        assert_eq!(rows[0].cell(table, column).unwrap(), *value);
    }
}

pub(super) fn assert_authority_rejects_staged_write(
    client: &Db<RocksDbStorage>,
    server: &CoreDb,
    write: &WriteHandle<RocksDbStorage>,
) {
    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Pending,
            global_time: None,
            durability: DurabilityTier::Local,
        },
        "the client must stage the write locally until the authority assigns its fate"
    );
    assert_eq!(
        block_on(write.wait(DurabilityTier::Local)).unwrap(),
        write.mergeable_tx_id()
    );

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        write.write_state().unwrap(),
        WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            global_time: None,
            durability: DurabilityTier::Local,
        },
        "only the authority may reject a staged write for policy authorization"
    );
    let error = block_on(write.wait(DurabilityTier::Global)).unwrap_err();
    assert_eq!(error.code, ErrorCode::WriteRejected);
}

#[test]
fn live_subscription_rebuilds_after_shared_current_descriptor_widens() {
    let base = owner_write_schema();
    let evolved = evolved_owner_write_schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0x5d, author, &base);
    db.insert(
        "todos",
        cells("before evolution", false, author),
        Default::default(),
    )
    .unwrap();

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(
        &db,
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    )
    .unwrap();
    assert_eq!(
        opened_rows(block_on(subscription.next_raw()).unwrap()).len(),
        1
    );

    let schema_version = SchemaVersion::new(evolved);
    let lens = MigrationLens::new(
        base.version_id(),
        schema_version.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .expect("valid migration lens");
    let publication = db
        .author_schema_lineage_publication(
            schema_version.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
        })
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: schema_version.id,
        })
        .unwrap();

    db.refresh_subscriptions().unwrap();
    let reset = subscription
        .try_next_event()
        .expect("descriptor widening must rebuild the live subscription");
    assert!(matches!(
        reset,
        SubscriptionEvent::Delta { reset: true, .. }
    ));

    db.insert(
        "todos",
        cells("after evolution", true, author),
        Default::default(),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(
        subscription
            .try_next_event()
            .expect("the rebuilt subscription must receive the next delta"),
    );
    assert_eq!(
        added.len(),
        1,
        "the rebuilt graph must accept the next delta"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn old_enum_subscription_rebuilds_across_registry_and_layout_growth() {
    let schema = |statuses: &[&str], with_body: bool| {
        let table = PublicTableSchemaBuilder::new("items")
            .column("title", PublicColumnType::Text)
            .column(
                "status",
                PublicColumnType::EnumPayload {
                    cases: statuses
                        .iter()
                        .map(|status| PublicEnumCaseDescriptor {
                            name: (*status).to_owned(),
                            fields: Vec::new(),
                        })
                        .collect(),
                },
            );
        let table = if with_body {
            table.column("body", PublicColumnType::Text)
        } else {
            table
        };
        build_public_db_test_schema(PublicSchemaBuilder::new().table(table))
    };
    let base = schema(&["open"], false);
    let middle = SchemaVersion::new(schema(&["open", "archived"], false));
    let latest = SchemaVersion::new(schema(&["open", "archived"], true));
    let author = AuthorSubject::for_test_bytes([0xa2; 16]);
    let db = open_db(0x5c, author, &base);
    let _before = db
        .insert(
            "items",
            BTreeMap::from([
                ("title".to_owned(), Value::String("before".to_owned())),
                ("status".to_owned(), empty_payload_case(0)),
            ]),
            Default::default(),
        )
        .unwrap();
    let query = Query::from("items");
    let mut subscription = prepared_subscribe(
        &db,
        &query,
        ReadOpts {
            tier: DurabilityTier::Local,
            local_updates: LocalUpdates::Deferred,
            propagation: Propagation::LocalOnly,
            include_deleted: false,
            ..ReadOpts::default()
        },
    )
    .unwrap();
    assert_eq!(
        opened_rows(block_on(subscription.next_raw()).unwrap()).len(),
        1
    );

    let enum_lens = MigrationLens::new(
        base.version_id(),
        middle.id,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![LensOp::TransformColumn {
                column: "status".to_owned(),
                transform: "jazz.identity".to_owned(),
            }],
        }],
    )
    .expect("valid migration lens");
    let enum_publication = db
        .author_schema_lineage_publication(
            middle.clone(),
            enum_lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(enum_publication),
        })
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 1,
            schema: middle.id,
        })
        .unwrap();
    assert_eq!(
        db.refresh_subscriptions().unwrap(),
        0,
        "enum registry growth alone refreshes the raw target in place"
    );

    let column_lens = MigrationLens::new(
        middle.id,
        latest.id,
        vec![TableLens {
            source_table: "items".to_owned(),
            target_table: "items".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .expect("valid migration lens");
    let column_publication = db
        .author_schema_lineage_publication(
            latest.clone(),
            column_lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(column_publication),
        })
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .activate_catalogue_schema_settled(CurrentWriteSchema {
            revision: 2,
            schema: latest.id,
        })
        .unwrap();
    db.refresh_subscriptions().unwrap();
    assert!(matches!(
        subscription.try_next_event(),
        Some(SubscriptionEvent::Delta { reset: true, .. })
    ));

    db.node
        .node
        .borrow_mut()
        .commit_mergeable_settled(MergeableCommit::new("items", row(0x5c), 10).cells(
            BTreeMap::from([
                ("title".to_owned(), Value::String("after".to_owned())),
                ("status".to_owned(), empty_payload_case(0)),
                ("body".to_owned(), Value::String("new body".to_owned())),
            ]),
        ))
        .unwrap();
    db.refresh_subscriptions().unwrap();
    let (added, updated, removed) = delta_rows(
        subscription
            .try_next_event()
            .expect("rebuilt old-enum subscription receives the next compatible delta"),
    );
    assert_eq!(added.len(), 1);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn live_subscription_rebuilds_when_non_genesis_permissions_head_changes() {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let table = |with_body: bool, read_column: Option<&str>| {
        let table = PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .column("editor", PublicColumnType::Uuid);
        let table = if with_body {
            table.column("body", PublicColumnType::Text)
        } else {
            table
        };
        let table = if let Some(column) = read_column {
            table.policies(
                PublicTablePolicies::new()
                    .with_select(public_session_eq(column, &["claims", "sub"])),
            )
        } else {
            table
        };
        build_public_db_test_schema(PublicSchemaBuilder::new().table(table))
    };
    let structural = table(false, None);
    let owner_head = table(true, Some("owner"));
    let editor_head = table(true, Some("editor"));
    let owner_payload = SchemaVersion::new(owner_head.clone());
    assert_eq!(owner_payload.id, editor_head.version_id());

    let db = open_db(0xa0, AuthorSubject::SYSTEM, &structural);
    db.set_test_provider_claims(alice, test_provider_claims(alice));
    db.set_test_provider_claims(bob, test_provider_claims(bob));
    let owner_lens = MigrationLens::new(
        structural.version_id(),
        owner_payload.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .expect("valid migration lens");
    let owner_publication = db
        .author_schema_lineage_publication(
            owner_payload.clone(),
            owner_lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.publish_schema_with_lens(1, owner_publication).unwrap();
    db.activate_catalogue_schema_for_test(CurrentWriteSchema {
        revision: 1,
        schema: owner_payload.id,
    })
    .unwrap();
    let first = row(0xa1);
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        first,
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
            ("editor".to_owned(), Value::Uuid(bob.test_uuid())),
            ("body".to_owned(), Value::String(String::new())),
        ]),
    )
    .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let mut subscription = block_on(db.subscribe_for_identity(
        &prepared,
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
        alice,
    ))
    .unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        vec![first]
    );

    db.activate_schema_for_test(2, editor_head).unwrap();
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        row(0xb2),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            ("owner".to_owned(), Value::Uuid(bob.test_uuid())),
            ("editor".to_owned(), Value::Uuid(bob.test_uuid())),
            ("body".to_owned(), Value::String(String::new())),
        ]),
    )
    .unwrap();

    let event = subscription
        .try_next_event()
        .expect("permissions-head change must refresh the live subscription");
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("permissions-head refresh must emit a delta reset");
    };
    assert!(reset);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(removed.len(), 1);
    assert_eq!(removed[0].row_uuid, first);
}

#[test]
fn db_catalogue_facade_publishes_schema_lens_and_current_write_schema() {
    let base = owner_write_schema();
    let evolved = evolved_owner_write_schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &base);
    let client = open_db(0xc1, owner, &base);
    let schema_version = SchemaVersion::new(evolved.clone());

    let lens = MigrationLens::new(
        base.version_id(),
        schema_version.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .expect("valid migration lens");
    let publication = core
        .author_schema_lineage_publication(
            schema_version.clone(),
            lens.clone(),
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    let lens_ack = core.publish_schema_with_lens(1, publication).unwrap();
    assert!(matches!(
        lens_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)]
            if ack.schema == Some(schema_version.id)
                && ack.lens == Some(lens.id)
                && ack.applied
    ));

    let pointer = CurrentWriteSchema {
        revision: 2,
        schema: schema_version.id,
    };
    core.activate_catalogue_schema_for_test(pointer).unwrap();
    assert_eq!(
        core.server.node().borrow().current_write_schema().unwrap(),
        pointer
    );

    let row = seed(&core, "todos", cells("under evolved schema", false, owner));
    let rows = core.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    // Preparing a descendant is pure local bookkeeping. Only the following
    // trusted catalogue admission is privileged, so a client can never turn
    // its locally authored UUIDs into an active authority publication.
    let client_publication = client
        .author_schema_lineage_publication(
            schema_version.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    let unauthorized = client
        .publish_schema_with_lens(1, client_publication)
        .unwrap_err();
    assert_eq!(unauthorized.code, ErrorCode::Protocol);
    assert!(
        unauthorized
            .message
            .contains("catalogue updates require a serving Node")
    );

    let unauthorized = client.publish_schema(schema_version).unwrap_err();
    assert_eq!(unauthorized.code, ErrorCode::Protocol);
    assert!(
        unauthorized
            .message
            .contains("catalogue updates require a serving Node")
    );
}

/// Authenticated app peers receive the existing app-wide metadata before a query;
/// a subjectless transport receives neither that catalogue nor application rows.
#[test]
fn catalogue_bootstrap_announces_only_to_admitted_sessions_and_scopes() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("items")
                .column("label", PublicColumnType::Text)
                .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::False)),
        ),
    );
    let author = AuthorSubject::for_test_bytes([0xe9; 16]);
    let server = open_core(0xea, AuthorSubject::SYSTEM, &schema);
    for kind in 0..3 {
        let (mut receiver, sender) = duplex();
        let connection = match kind {
            0 => server.accept_subscriber_with_trust(sender, author, CommitUnitTrust::Session),
            1 => server.server.accept_scope_isolated_relay_subscriber(
                sender,
                author,
                BTreeMap::new(),
                1,
            ),
            _ => server.server.accept_relay_subscriber(sender),
        };
        server.tick().unwrap();
        if kind < 2 {
            let Some(SyncMessage::CatalogueSnapshot(snapshot)) = receiver.try_recv() else {
                panic!("admitted session must receive catalogue before querying");
            };
            assert!(
                snapshot
                    .schemas
                    .iter()
                    .any(|version| version.id == schema.version_id())
            );
        }
        assert!(
            receiver.try_recv().is_none(),
            "bootstrap carries no rows or unbound relay metadata"
        );
        drop(connection);
    }
    let outbound = Rc::new(RefCell::new(std::collections::VecDeque::new()));
    let connection = server.accept_subscriber_with_trust(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        author,
        CommitUnitTrust::Session,
    );
    connection.borrow_mut().tick().unwrap();
    assert!(outbound.borrow().is_empty());
    connection.borrow_mut().tick().unwrap();
    assert!(matches!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    connection.borrow_mut().tick().unwrap();
    assert!(
        outbound.borrow().is_empty(),
        "accepted bootstrap is sent once"
    );
}

#[test]
fn uninitialized_catalogue_source_keeps_admitted_session_pending() {
    let empty = JazzSchema::empty();
    let families = empty.column_families();
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let (storage, _) = groove::storage::TestStorage::controlled(&refs);
    let state = block_on(Box::pin(NodeState::new_catalogue_uninitialized(
        NodeUuid::from_bytes([0xeb; 16]),
        storage,
    )))
    .unwrap();
    let source = Node::new(state);
    let (mut receiver, sender) = duplex();
    let connection = source.accept_subscriber(sender, AuthorSubject::for_test_bytes([0xec; 16]));
    tick_offline_catalogue_connection(&connection);
    assert!(receiver.try_recv().is_none());
}

/// Alice owns an external publication while Bob sends catalogue metadata and a
/// following frame. Each link retains only its head frame, waits without polling
/// itself, and resumes the FIFO after Alice settles. Detach discards only that
/// connection's frames; the publication remains Alice's responsibility.
#[test]
fn catalogue_ingress_defers_without_busy_loop_and_resumes_after_settlement() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    struct WakeCount(AtomicUsize);
    impl futures::task::ArcWake for WakeCount {
        fn wake_by_ref(this: &Arc<Self>) {
            this.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    struct Scheduler {
        calls: RefCell<Vec<TickUrgency>>,
        wake: Arc<WakeCount>,
    }
    impl TickScheduler for Scheduler {
        fn schedule_tick(&self, urgency: TickUrgency) {
            self.calls.borrow_mut().push(urgency);
        }
        fn schedule_tick_after(&self, _delay_ms: u64) {
            self.calls.borrow_mut().push(TickUrgency::Deferred);
        }
        fn query_runtime_waker(&self) -> Option<std::task::Waker> {
            Some(futures::task::waker(self.wake.clone()))
        }
    }
    for (upstream, detach) in [(true, false), (false, false), (true, true)] {
        let alice = open_db(0x91, AuthorSubject::SYSTEM, &schema());
        let scheduler = Rc::new(Scheduler {
            calls: RefCell::new(Vec::new()),
            wake: Arc::new(WakeCount(AtomicUsize::new(0))),
        });
        alice.set_tick_scheduler(Some(scheduler.clone()));
        let snapshot = alice.node.node.borrow().catalogue_snapshot().unwrap();
        let (transport, mut bob) = duplex();
        let connection = if upstream {
            block_on(alice.connect_upstream(transport))
        } else {
            alice.node.accept_subscriber_with_trust(
                transport,
                AuthorSubject::SYSTEM,
                CommitUnitTrust::TrustedBackend,
            )
        };
        block_on(connection.borrow_mut().tick()).unwrap();
        let published = block_on(async {
            alice
                .node
                .node
                .lock()
                .await
                .commit_mergeable(
                    crate::node::MergeableCommit::new(
                        "todos",
                        RowUuid::from_bytes([0x92; 16]),
                        1_000,
                    )
                    .made_by(AuthorSubject::SYSTEM)
                    .cells(BTreeMap::from([(
                        "title".to_owned(),
                        Value::String("preserved".to_owned()),
                    )])),
                )
                .await
        })
        .unwrap();
        let frame = if upstream {
            SyncMessage::CatalogueSnapshot(Box::new(snapshot.clone()))
        } else {
            SyncMessage::PublishSchema {
                author: AuthorSubject::SYSTEM,
                schema: Box::new(snapshot.schemas[0].clone()),
            }
        };
        bob.send(frame.clone()).unwrap();
        bob.send(frame).unwrap();
        scheduler.calls.borrow_mut().clear();
        for _ in 0..3 {
            block_on(connection.borrow_mut().tick()).unwrap();
            assert_eq!(connection.borrow().staged_inbound.len(), 1);
            assert!(
                scheduler.calls.borrow_mut().drain(..).next().is_none(),
                "a deferred catalogue must not self-schedule"
            );
        }
        let wake_before = scheduler.wake.0.load(Ordering::SeqCst);
        if detach {
            block_on(alice.detach_connection_async(&connection)).unwrap();
        }
        let tx_id = published.tx_id();
        let persistence = block_on(published.persist());
        alice
            .node
            .node
            .borrow_mut()
            .settle_published_transaction(tx_id, persistence)
            .unwrap();
        drop(published);
        assert!(
            scheduler.wake.0.load(Ordering::SeqCst) > wake_before,
            "settlement wakes the shared owner even if a deferred link detached"
        );
        if !detach {
            block_on(connection.borrow_mut().tick()).unwrap();
            assert!(connection.borrow().staged_inbound.is_empty());
        }
        let query = alice.prepare_query(&Query::from("todos")).unwrap();
        let rows = block_on(alice.all(&query, ReadOpts::default())).unwrap();
        assert_eq!(rows.len(), 1);
    }
}

/// Bob sends competing root commits and a catalogue in one owner turn. Alice
/// must settle the generated merge after deferring the catalogue, then replay it.
/// The internal peer boundary makes the between-frames ownership visible.
#[test]
fn catalogue_after_same_turn_merge_resumes_after_local_settlement() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .policies(
                    public_legacy_write_policy(PublicPolicyExpr::True)
                        .with_select(PublicPolicyExpr::True),
                ),
        ),
    );
    let alice = block_on(Db::open_history_complete(DbConfig::new(
        schema.clone(),
        rocks_storage(&schema),
        DbIdentity {
            node: NodeUuid::from_bytes([0xa1; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    let writer = open_db(0xa2, AuthorSubject::SYSTEM, &schema);
    let (_, unit) = block_on(async {
        writer.node.node.lock().await.commit_mergeable_unit_settled(
            crate::node::MergeableCommit::new("todos", RowUuid::from_bytes([0xa3; 16]), 1_000)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("same turn".to_owned()),
                )])),
        )
    })
    .unwrap();
    let second_writer = open_db(0xa4, AuthorSubject::SYSTEM, &schema);
    let (_, competing_unit) = second_writer
        .node
        .node
        .borrow_mut()
        .commit_mergeable_unit_settled(
            crate::node::MergeableCommit::new("todos", RowUuid::from_bytes([0xa3; 16]), 1_001)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([(
                    "title".to_owned(),
                    Value::String("competing root".to_owned()),
                )])),
        )
        .unwrap();
    let snapshot = writer.node.node.borrow().catalogue_snapshot().unwrap();
    let (transport, mut bob) = duplex();
    let connection = block_on(alice.connect_upstream(transport));
    block_on(connection.borrow_mut().tick()).unwrap();
    bob.send(unit).unwrap();
    bob.send(competing_unit).unwrap();
    bob.send(SyncMessage::CatalogueSnapshot(Box::new(snapshot)))
        .unwrap();
    block_on(connection.borrow_mut().tick()).unwrap();
    let responses = std::iter::from_fn(|| bob.try_recv()).collect::<Vec<_>>();
    assert_eq!(
        connection.borrow().staged_inbound.len(),
        1,
        "catalogue must wait for the commit accepted earlier in this turn; responses={responses:?}"
    );
    assert!(
        !alice
            .node
            .node
            .borrow()
            .defer_catalogue_for_persistence(None)
            .unwrap()
    );
    block_on(connection.borrow_mut().tick()).unwrap();
    assert!(connection.borrow().staged_inbound.is_empty());
    let query = alice.prepare_query(&Query::from("todos")).unwrap();
    assert_eq!(
        block_on(alice.all(&query, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
}

// Internal transport-boundary tests are needed to inject the authority snapshot
// independently of the local read schema; assertions use visible subscription events.
#[test]
fn trusted_snapshot_schema_switch_rebuilds_live_authorization() {
    assert_authorization_source_refresh(true, false, false);
}

#[test]
fn direct_schema_switch_with_same_policies_rebuilds_live_authorization() {
    assert_authorization_source_refresh(false, true, false);
}

#[test]
fn trusted_snapshot_schema_switch_with_same_policies_rebuilds_live_authorization() {
    assert_authorization_source_refresh(true, true, false);
}

#[test]
fn schema_switch_with_constant_deny_preserves_live_authorization() {
    assert_authorization_source_refresh(true, true, true);
    assert_authorization_source_refresh(false, true, true);
}

fn assert_authorization_source_refresh(
    snapshot_activation: bool,
    same_policies: bool,
    deny_all: bool,
) {
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let table = |with_body: u8, read_column: Option<&str>| {
        let table = PublicTableSchemaBuilder::new("todos")
            .column("title", PublicColumnType::Text)
            .column("owner", PublicColumnType::Uuid)
            .column("editor", PublicColumnType::Uuid);
        let table = if with_body > 0 {
            table.column("body", PublicColumnType::Text)
        } else {
            table
        };
        let table = if with_body > 1 {
            table.column("extra", PublicColumnType::Text)
        } else {
            table
        };
        let table = if let Some(column) = read_column {
            table.policies(PublicTablePolicies::new().with_select(if deny_all {
                PublicPolicyExpr::False
            } else {
                public_session_eq(column, &["claims", "sub"])
            }))
        } else {
            table
        };
        build_public_db_test_schema(PublicSchemaBuilder::new().table(table))
    };
    let structural = table(0, None);
    let owner_head = table(1, Some("owner"));
    let editor_head = table(2, Some(if same_policies { "owner" } else { "editor" }));
    let owner_payload = SchemaVersion::new(owner_head.clone());
    assert_ne!(owner_payload.id, editor_head.version_id());

    let db = open_db(0xa0, AuthorSubject::SYSTEM, &structural);
    db.set_test_provider_claims(alice, test_provider_claims(alice));
    db.set_test_provider_claims(bob, test_provider_claims(bob));
    let owner_lens = MigrationLens::new(
        structural.version_id(),
        owner_payload.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .expect("valid migration lens");
    let owner_publication = db
        .author_schema_lineage_publication(
            owner_payload.clone(),
            owner_lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.publish_schema_with_lens(1, owner_publication).unwrap();
    db.activate_catalogue_schema_for_test(CurrentWriteSchema {
        revision: 1,
        schema: owner_payload.id,
    })
    .unwrap();
    let editor_lens = MigrationLens::new(
        owner_payload.id,
        editor_head.version_id(),
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "extra".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    )
    .unwrap();
    let editor_publication = db
        .author_schema_lineage_publication(
            SchemaVersion::new(editor_head.clone()),
            editor_lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .unwrap();
    db.publish_schema_with_lens(2, editor_publication).unwrap();
    let first = row(0xa1);
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        first,
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("title".to_owned(), Value::String("first".to_owned())),
            ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
            ("editor".to_owned(), Value::Uuid(bob.test_uuid())),
            ("body".to_owned(), Value::String(String::new())),
        ]),
    )
    .unwrap();

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let mut subscription = block_on(db.subscribe_for_identity(
        &prepared,
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
        alice,
    ))
    .unwrap();
    assert_eq!(
        row_ids(&opened_rows(block_on(subscription.next_raw()).unwrap())),
        if deny_all { vec![] } else { vec![first] }
    );

    // Replaying the identical authority must retain the live subscription.
    let token = db.node.node.borrow().groove_runtime_token();
    let snapshot = db.node.node.borrow().catalogue_snapshot().unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_snapshot_settled(snapshot.clone())
        .unwrap();
    assert_eq!(db.node.node.borrow().groove_runtime_token(), token);
    assert!(subscription.try_next_event().is_none());
    // Advancing only the authority revision must also preserve live state.
    if snapshot_activation {
        let mut revision_only = snapshot.clone();
        revision_only.current_write_schema.revision = 2;
        db.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_snapshot_settled(revision_only)
            .unwrap();
    } else {
        db.activate_schema_for_test(2, owner_head).unwrap();
    }
    assert_eq!(db.node.node.borrow().groove_runtime_token(), token);
    assert!(subscription.try_next_event().is_none());
    if snapshot_activation {
        let mut snapshot = snapshot;
        snapshot.current_write_schema = CurrentWriteSchema {
            revision: 3,
            schema: editor_head.version_id(),
        };
        db.node
            .node
            .borrow_mut()
            .apply_trusted_catalogue_snapshot_settled(snapshot)
            .unwrap();
    } else {
        db.activate_schema_for_test(3, editor_head).unwrap();
    }
    db.seed_settled_mergeable_for_bootstrap(
        "todos",
        row(0xb2),
        AuthorSubject::SYSTEM,
        BTreeMap::from([
            ("title".to_owned(), Value::String("second".to_owned())),
            ("owner".to_owned(), Value::Uuid(alice.test_uuid())),
            ("editor".to_owned(), Value::Uuid(bob.test_uuid())),
            ("body".to_owned(), Value::String(String::new())),
        ]),
    )
    .unwrap();

    if deny_all {
        assert_eq!(db.node.node.borrow().groove_runtime_token(), token);
        assert!(subscription.try_next_event().is_none());
        return;
    }

    let event = subscription
        .try_next_event()
        .expect("permissions-head change must refresh the live subscription");
    let SubscriptionEvent::Delta {
        reset,
        added,
        updated,
        removed,
        ..
    } = event
    else {
        panic!("permissions-head refresh must emit a delta reset");
    };

    assert!(reset);
    assert!(updated.is_empty());
    if same_policies {
        assert_eq!(
            added
                .iter()
                .map(|output| output.row.row_uuid())
                .collect::<Vec<_>>(),
            vec![first, row(0xb2)]
        );
        assert!(removed.is_empty());
    } else {
        assert!(added.is_empty());
        assert_eq!(removed.len(), 1);
        assert_eq!(removed[0].row_uuid, first);
    }
}
