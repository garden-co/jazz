//! Schema publication, registry growth, and live-runtime compatibility tests.

use super::*;

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
    );
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                schema_version.clone(),
                lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: schema_version.id,
            },
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
    );
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                middle.clone(),
                enum_lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 1,
                schema: middle.id,
            },
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
    );
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 2,
            publication: Box::new(SchemaLineagePublication::new(
                latest.clone(),
                column_lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();
    db.node
        .node
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 2,
                schema: latest.id,
            },
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
    db.publish_schema_with_lens(
        1,
        SchemaLineagePublication::new(
            owner_payload.clone(),
            MigrationLens::new(
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
            ),
            Vec::<String>::new(),
            Vec::<String>::new(),
        ),
    )
    .unwrap();
    db.set_current_write_schema(CurrentWriteSchema {
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

    db.publish_schema(SchemaVersion::new(editor_head)).unwrap();
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
    );
    let lens_ack = core
        .publish_schema_with_lens(
            1,
            SchemaLineagePublication::new(
                schema_version.clone(),
                lens.clone(),
                Vec::<String>::new(),
                Vec::<String>::new(),
            ),
        )
        .unwrap();
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
    let pointer_ack = core.set_current_write_schema(pointer).unwrap();
    assert!(matches!(
        pointer_ack.as_slice(),
        [SyncMessage::CatalogueAck(ack)] if ack.revision == Some(2) && ack.schema == Some(schema_version.id) && ack.applied
    ));

    let row = seed(&core, "todos", cells("under evolved schema", false, owner));
    let rows = core.read(&Query::from("todos")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_uuid(), row);

    let unauthorized = client.publish_schema(schema_version).unwrap_err();
    assert_eq!(unauthorized.code, ErrorCode::Protocol);
    assert!(
        unauthorized
            .message
            .contains("catalogue updates require a serving Node")
    );
}
