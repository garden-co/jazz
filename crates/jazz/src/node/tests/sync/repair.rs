// Authorized row-version fetch and canonical known-state repair.

#[test]
fn relay_row_version_fetch_helper_rejects_missing_policy_binding() {
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let mut relay = PeerState::relay();

    assert!(matches!(
        relay
            .handle_row_versions_fetch(
                &mut core,
                SyncMessage::FetchRowVersions {
                    requests: Vec::new(),
                    delegated_session: None,
                },
            )
            .resolve(),
        Err(Error::InvalidStoredValue(
            "relay row-version repair requires an explicit immutable policy binding"
        ))
    ));
}

/// A repair response preflights every requested bundle before ingesting any
/// transaction. A malformed later carrier therefore cannot partially publish
/// an earlier valid carrier from the same `RowVersionPayloads` frame.
///
/// relay ──[valid A, malformed B @ max + 1]──► reader
/// reader ──typed error──► no transaction, history, or clock mutation
#[test]
fn repair_frame_rejects_late_invalid_provenance_before_any_ingest() {
    use crate::time::HLC_MAX_PHYSICAL_MS;

    let schema = schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema.clone());
    let (_reader_dir, mut reader) = open_node_with_schema(node(3), schema.clone());
    let mut requests = Vec::new();
    for (now_ms, row_uuid) in [(10, row(0xa1)), (11, row(0xa2))] {
        let tx_id = commit_mergeable_global(
            &mut writer,
            &mut core,
            MergeableCommit::new("todos", row_uuid, now_ms).cells(title_cells("repair")),
        );
        requests.push(crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id));
    }
    let mut peer = PeerState::client_link(AuthorSubject::SYSTEM);
    let messages = peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: requests.clone(),
                delegated_session: None,
            },
        )
        .unwrap();
    assert_eq!(messages.len(), 1);
    let SyncMessage::RowVersionPayloads {
        mut version_bundles,
    } = messages.into_iter().next().unwrap()
    else {
        panic!("expected row-version repair payloads");
    };
    version_bundles.sort_by_key(|bundle| bundle.tx.tx_id);
    assert_eq!(version_bundles.len(), 2);
    let original = version_bundles[1].versions[0].clone();
    version_bundles[1].versions[0] = VersionRecord::encode(
        &schema.tables[0],
        original.schema_version(),
        original.row_uuid(),
        original.parents(),
        original.created_by(),
        HLC_MAX_PHYSICAL_MS + 1,
        original.updated_by(),
        HLC_MAX_PHYSICAL_MS + 1,
        &[original.cell_at(0)],
        original.deletion(),
    )
    .unwrap()
    .with_authored_columns(original.authored_columns().cloned());

    let clock_before = reader.clock.tx_time;
    assert!(matches!(
        reader
            .apply_row_version_payloads_for_requests(&requests, version_bundles)
            .resolve(),
        Err(Error::MalformedViewUpdate(
            "row version provenance exceeds packed HLC physical-millisecond range"
        ))
    ));
    assert_eq!(reader.clock.tx_time, clock_before);
    for request in requests {
        assert!(
            reader
                .row_history("todos", request.row_uuid)
                .unwrap()
                .is_empty()
        );
        assert!(reader.transaction_record(request.tx_id()).is_none());
    }
}

#[test]
fn row_version_fetch_returns_authorized_versions_and_omits_unauthorized_rows() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    let bob = user(0xb2);
    install_test_uuid_sub_claim(&mut core, alice);
    install_test_uuid_sub_claim(&mut core, bob);
    let alice_row = row(7);
    let bob_row = row(8);

    let alice_tx = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", alice_row, 10)
            .made_by(alice)
            .cells(owner_cells(alice, "alice")),
    );
    let bob_tx = commit_mergeable_global(
        &mut writer,
        &mut core,
        MergeableCommit::new("todos", bob_row, 11)
            .made_by(bob)
            .cells(owner_cells(bob, "bob")),
    );
    let requests = vec![
        crate::protocol::RowVersionRef::new("todos", alice_row, alice_tx),
        crate::protocol::RowVersionRef::new("todos", bob_row, bob_tx),
    ];

    let mut alice_peer = PeerState::client_link(alice);
    let messages = alice_peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: requests.clone(),
                delegated_session: None,
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
        panic!("expected one row-version payload response");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].row_uuid(), alice_row);

    let mut too_many = Vec::new();
    for _ in 0..=crate::protocol_limits::MAX_FETCH_ROW_VERSIONS {
        too_many.push(crate::protocol::RowVersionRef::new(
            "todos", alice_row, alice_tx,
        ));
    }
    assert!(matches!(
        alice_peer.handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions { requests: too_many, delegated_session: None },
        ).resolve(),
        Err(Error::UnsupportedSyncMessage(
            "row-version repair request exceeds limit"
        ))
    ));
}

/// Internal boundary test: the public browser topology tests exercise the
/// admitted foreground link. Here we prove the lower serving capability cannot
/// fall back to a fresh policy evaluation or live result membership. Alice's
/// authority-delivered row remains repairable after policy would reject Bob;
/// a never-recorded physical version remains hidden.
#[test]
fn scope_relay_repair_uses_durable_authority_ledger_not_live_policy() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_relay_dir, mut relay_node) = open_node_with_schema(node(9), schema);
    relay_node.set_relay_authority_session_owner_for_test();
    let alice = user(0xa1);
    let bob = user(0xb2);
    install_test_uuid_sub_claim(&mut relay_node, alice);
    install_test_uuid_sub_claim(&mut relay_node, bob);
    let row_uuid = row(0x29);
    let tx_id = commit_mergeable_global(
        &mut writer,
        &mut relay_node,
        MergeableCommit::new("todos", row_uuid, 10)
            .made_by(alice)
            .cells(owner_cells(alice, "retained authority delivery")),
    );
    let request = crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id);
    let hidden_row = row(0x2a);
    let hidden_tx = commit_mergeable_global(
        &mut writer,
        &mut relay_node,
        MergeableCommit::new("todos", hidden_row, 11)
            .made_by(alice)
            .cells(owner_cells(alice, "not delivered to this scope")),
    );
    let update = relay_node.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate(payload) = update else {
        panic!("expected authority view update");
    };
    let bundles = crate::protocol::expand_version_carriers(&payload.version_carriers).unwrap();
    let bundles = bundles
        .into_iter()
        .filter(|bundle| bundle.tx.tx_id == tx_id)
        .collect::<Vec<_>>();
    assert_eq!(bundles.len(), 1, "test setup must retain one delivered body");
    relay_node
        .record_scope_relay_authoritative_bundles(&bundles)
        .resolve()
        .unwrap();

    assert!(
        !relay_node
            .dry_run_read_current_allows("todos", row_uuid, bob)
            .unwrap(),
        "planted control: a fresh Bob authority evaluation rejects this row"
    );
    let mut relay = PeerState::relay();
    let retained_response = relay
        .serve_row_versions(
            &mut relay_node,
            std::slice::from_ref(&request),
            crate::peer::RepairServingContext::ScopeIsolatedClientRelay,
        )
        .resolve()
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = retained_response.as_slice()
    else {
        panic!("expected retained same-scope repair response");
    };
    assert_eq!(version_bundles.len(), 1);

    let hidden = crate::protocol::RowVersionRef::new("todos", hidden_row, hidden_tx);
    let hidden_response = relay
        .serve_row_versions(
            &mut relay_node,
            &[hidden],
            crate::peer::RepairServingContext::ScopeIsolatedClientRelay,
        )
        .resolve()
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = hidden_response.as_slice()
    else {
        panic!("expected retained same-scope repair response");
    };
    assert!(version_bundles.is_empty(), "unrecorded ref stays hidden");

    // A matching digest alone is not a capability. Corrupting the retained
    // scope value must fail closed rather than serve the row under a guessed
    // scope key.
    let scope_digest = relay_node.client_relay_scope().unwrap().durable_digest();
    let table_id = relay_node
        .physical_table_id_for_schema(bundles[0].versions[0].schema_version(), "todos")
        .unwrap();
    {
        let store = relay_node
            .database
            .direct_record_store(crate::schema::SCOPE_RELAY_REPAIR_LEDGER_STORE)
            .unwrap();
        store
            .set(
            &[
                Value::Bytes(scope_digest.to_vec()),
                Value::U64(table_id.0),
                Value::Uuid(row_uuid.0),
                Value::U64(tx_id.time.0),
                Value::Uuid(tx_id.node.0),
            ],
            &[
                Value::U64(1),
                Value::String("wrong scope".to_owned()),
                Value::Nullable(None),
            ],
            )
            .resolve()
            .unwrap();
    }
    assert!(matches!(
        relay
            .serve_row_versions(
                &mut relay_node,
                std::slice::from_ref(&request),
                crate::peer::RepairServingContext::ScopeIsolatedClientRelay,
            )
            .resolve(),
        Err(Error::InvalidStoredValue("scope relay ledger value does not match admitted scope"))
    ));
    {
        let store = relay_node
            .database
            .direct_record_store(crate::schema::SCOPE_RELAY_REPAIR_LEDGER_STORE)
            .unwrap();
        store
            .set(
            &[
                Value::Bytes(scope_digest.to_vec()),
                Value::U64(table_id.0),
                Value::Uuid(row_uuid.0),
                Value::U64(tx_id.time.0),
                Value::Uuid(tx_id.node.0),
            ],
            &[
                Value::U64(2),
                Value::String("wrong scope".to_owned()),
                Value::Nullable(None),
            ],
            )
            .resolve()
            .unwrap();
    }
    assert!(matches!(
        relay
            .serve_row_versions(
                &mut relay_node,
                std::slice::from_ref(&request),
                crate::peer::RepairServingContext::ScopeIsolatedClientRelay,
            )
            .resolve(),
        Err(Error::InvalidStoredValue("unknown scope relay ledger format"))
    ));
}

/// The durable ledger is not a general replacement for authority policy.
/// A relay without a live host-attached scope must fail closed even if it has
/// the row locally; a generic relay must forward the repair to its authority.
#[test]
fn scope_relay_repair_requires_a_live_scope_capability() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (_relay_dir, mut relay_node) = open_node_with_schema(node(9), schema);
    let alice = user(0xa1);
    install_test_uuid_sub_claim(&mut relay_node, alice);
    let row_uuid = row(0x2b);
    let tx_id = commit_mergeable_global(
        &mut writer,
        &mut relay_node,
        MergeableCommit::new("todos", row_uuid, 10)
            .made_by(alice)
            .cells(owner_cells(alice, "locally cached but no capability")),
    );
    let response = PeerState::relay()
        .serve_row_versions(
            &mut relay_node,
            &[crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id)],
            crate::peer::RepairServingContext::ScopeIsolatedClientRelay,
        )
        .resolve()
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = response.as_slice() else {
        panic!("expected one row-version payload response");
    };
    assert!(version_bundles.is_empty());
}

/// A repaired version is durable knowledge for exactly the host-attested
/// scope that received it. Reopening that scope retains the closure; opening
/// the same physical database under a different admitted subject does not.
#[test]
fn scope_relay_repair_ledger_survives_reopen_only_for_exact_scope() {
    let schema = owner_policy_schema();
    let (_writer_dir, mut writer) = open_node_with_schema(node(1), schema.clone());
    let (relay_dir, mut relay_node) = open_node_with_schema(node(9), schema.clone());
    let alice = user(0xa1);
    let bob = user(0xb2);
    install_test_uuid_sub_claim(&mut relay_node, alice);
    install_test_uuid_sub_claim(&mut relay_node, bob);
    let alice_scope = unsafe {
        crate::db::ClientRelayScope::from_admitted_storage_owner(
            "relay-storage-owner".to_owned(),
            alice,
        )
    };
    relay_node
        .configure_scope_isolated_client_relay(alice_scope.clone())
        .unwrap();
    let row_uuid = row(0x2c);
    let tx_id = commit_mergeable_global(
        &mut writer,
        &mut relay_node,
        MergeableCommit::new("todos", row_uuid, 10)
            .made_by(alice)
            .cells(owner_cells(alice, "reopen retained authority delivery")),
    );
    let SyncMessage::ViewUpdate(payload) = relay_node.view_update_for_current_rows("todos").unwrap() else {
        panic!("expected authority view update");
    };
    let bundles = crate::protocol::expand_version_carriers(&payload.version_carriers).unwrap();
    let table_id = relay_node
        .physical_table_id_for_schema(bundles[0].versions[0].schema_version(), "todos")
        .unwrap();
    relay_node
        .record_scope_relay_authoritative_bundles(&bundles)
        .resolve()
        .unwrap();
    let request = crate::protocol::RowVersionRef::new("todos", row_uuid, tx_id);
    drop(relay_node);

    let mut reopened = reopen_node_at(&relay_dir, node(9), schema.clone());
    reopened
        .configure_scope_isolated_client_relay(alice_scope)
        .unwrap();
    assert!(
        reopened
            .scope_relay_repair_ledger_contains(table_id, &request)
            .resolve()
            .unwrap(),
        "same durable scope retains delivered closure"
    );
    drop(reopened);

    let mut wrong_scope = reopen_node_at(&relay_dir, node(9), schema);
    wrong_scope
        .configure_scope_isolated_client_relay(unsafe {
            crate::db::ClientRelayScope::from_admitted_storage_owner(
                "relay-storage-owner".to_owned(),
                bob,
            )
        })
        .unwrap();
    assert!(
        !wrong_scope
            .scope_relay_repair_ledger_contains(table_id, &request)
            .resolve()
            .unwrap(),
        "different admitted scope cannot reuse ledger"
    );
}

#[test]
fn declared_known_state_view_update_repairs_withheld_row_version_body() {
    let (_writer_dir, mut writer) = open_node_with_uuid(node(1));
    let (_core_dir, mut core) = open_node_with_uuid(node(9));
    let (_reader_dir, mut reader) = open_node_with_uuid(node(3));
    let row_uuid = row(7);
    let (shape, binding) = reader.whole_table_shape_binding("todos").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let (tx_id, commit_unit) = writer
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("repair me")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = commit_unit else {
        panic!("expected commit unit");
    };
    core.ingest_commit_unit_settled(tx.clone(), versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    reader
        .ingest_known_transaction(
            tx,
            Vec::new(),
            Fate::Accepted,
            Some(GlobalTime(1)),
            DurabilityTier::Global,
        )
        .unwrap();

    let mut update = core.view_update_for_current_rows("todos").unwrap();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        version_carriers,
        result_member_adds,
        ..
    }) = &mut update
    else {
        panic!("expected view update");
    };
    version_carriers.clear();
    assert_eq!(
        result_member_adds,
        &vec![("todos".to_owned().into(), row_uuid, tx_id)]
    );

    let missing = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert_eq!(
        missing,
        vec![crate::protocol::RowVersionRef::new(
            "todos", row_uuid, tx_id
        )]
    );
    let mut peer = PeerState::client_link(AuthorSubject::SYSTEM);
    let messages = peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: missing.clone(),
                delegated_session: None,
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
        panic!("expected row-version payloads");
    };
    reader
        .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
        .unwrap();
    assert!(
        reader
            .missing_known_state_row_version_refs(&update)
            .unwrap()
            .is_empty()
    );

    let tx_node_alias = reader.node_aliases.get(&tx_id.node).copied().unwrap();
    let mut batch = reader.database.open_batch();
    batch.delete(
        "jazz_transactions",
        groove::db::PrimaryKeyValue::Composite(vec![
            groove::db::PrimaryKeyValue::U64(tx_id.time.0),
            groove::db::PrimaryKeyValue::U64(tx_node_alias.0),
        ]),
    );
    let applied = crate::db::block_on(reader.database.apply_batch(batch)).unwrap();
let persisted = crate::db::block_on(applied.persist());
reader.database.finish_persistence(persisted).unwrap();
    assert_eq!(
        reader
            .missing_known_state_row_version_refs(&update)
            .unwrap(),
        missing,
        "a local version row without its transaction metadata still needs repair"
    );
    reader
        .apply_row_version_payloads_for_requests(&missing, version_bundles.clone())
        .unwrap();
    assert!(
        reader
            .missing_known_state_row_version_refs(&update)
            .unwrap()
            .is_empty()
    );

    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .current_rows("todos", DurabilityTier::Local)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(row_uuid, title_cells("repair me"))])
    );
}

/// Known-state repair requests use the receiver's projected table name, while
/// the repair response must preserve the row's canonical authored name.  A
/// table rename is therefore matched through the durable physical table id on
/// both the serving and receiving sides.
#[test]
fn renamed_known_state_repair_round_trips_canonical_authored_payload() {
    let base = schema();
    let base_version = base.version_id();
    let renamed_schema = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("tasks")
                    .column("name", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("notes")
                    .column("body", PublicColumnType::Text),
            ),
    );
    let renamed = SchemaVersion::new(renamed_schema.clone());
    let (_core_dir, mut core) = open_node_with_schema(node(0x91), base.clone());
    let row_uuid = row(0x92);
    let tx_id = accept_global(
        &mut core,
        MergeableCommit::new("todos", row_uuid, 10).cells(title_cells("canonical authored")),
    );
    publish_schema_lineage(
        &mut core,
        renamed.clone(),
        MigrationLens::new(
            base.version_id(),
            renamed.id,
            vec![TableLens {
                source_table: "todos".to_owned(),
                target_table: "tasks".to_owned(),
                ops: vec![
                    LensOp::RenameTable {
                        from: "todos".to_owned(),
                        to: "tasks".to_owned(),
                    },
                    LensOp::RenameColumn {
                        from: "title".to_owned(),
                        to: "name".to_owned(),
                    },
                ],
            }],
        ).expect("valid migration lens"),
        ["notes"],
        Vec::<String>::new(),
    )
    .unwrap();
    core.apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
        author: AuthorSubject::SYSTEM,
        pointer: CurrentWriteSchema {
            revision: 1,
            schema: renamed.id,
        },
    })
    .unwrap();

    // Install the exact production catalogue snapshot rather than fabricating
    // physical mappings in the receiver harness.  Both `tasks` and the
    // canonical `todos` payload resolve to the one durable physical table.
    let (_reader_dir, mut reader) = open_node_with_schema(node(0x93), base.clone());
    reader
        .apply_trusted_catalogue_snapshot_settled(core.catalogue_snapshot().unwrap())
        .unwrap();
    let (shape, binding) = core.whole_table_shape_binding("tasks").unwrap();
    register_shape_binding(&mut reader, &shape, &binding);

    let update = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: crate::protocol::SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        settled_through: GlobalTime(1),
        reset_result_set: false,
        version_carriers: Vec::new(),
        peer_payload_inventory: Default::default(),
        result_member_adds: vec![("tasks".to_owned().into(), row_uuid, tx_id).into()],
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        result_member_adds,
        ..
    }) = &update
    else {
        panic!("expected view update");
    };
    assert_eq!(
        result_member_adds,
        &vec![("tasks".to_owned().into(), row_uuid, tx_id)],
        "the pending update names the receiver's projected table"
    );

    let requests = reader
        .missing_known_state_row_version_refs(&update)
        .unwrap();
    assert_eq!(
        requests,
        vec![crate::protocol::RowVersionRef::new("tasks", row_uuid, tx_id)],
        "with no carrier, the update must issue FetchRowVersions"
    );
    let mut peer = PeerState::client_link(AuthorSubject::SYSTEM);
    let messages = peer
        .handle_row_versions_fetch(
            &mut core,
            SyncMessage::FetchRowVersions {
                requests: requests.clone(),
                delegated_session: None,
            },
        )
        .unwrap();
    let [SyncMessage::RowVersionPayloads { version_bundles }] = messages.as_slice() else {
        panic!("projected FetchRowVersions must return its canonical payload");
    };
    assert_eq!(version_bundles.len(), 1);
    assert_eq!(version_bundles[0].versions.len(), 1);
    assert_eq!(version_bundles[0].versions[0].table(), "todos");
    assert_eq!(version_bundles[0].versions[0].schema_version(), base_version);
    let mut inline_update = update.clone();
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        version_carriers: inline_carriers,
        ..
    }) = &mut inline_update
    else {
        unreachable!();
    };
    *inline_carriers =
        crate::protocol::build_version_carriers_from_singletons(version_bundles.clone()).unwrap();
    assert!(
        reader
            .missing_known_state_row_version_refs(&inline_update)
            .unwrap()
            .is_empty(),
        "an inline canonical `todos` witness covers the projected `tasks` member by physical identity"
    );

    reader
        .apply_row_version_payloads_for_requests(&requests, version_bundles.clone())
        .unwrap();
    assert!(
        reader
            .missing_known_state_row_version_refs(&update)
            .unwrap()
            .is_empty(),
        "the pending update resumes only after the canonical witness arrives"
    );
    let pending_update = update.clone();
    reader.apply_sync_message_settled(update).unwrap();
    assert_eq!(
        reader
            .query_rows(&shape, &binding, DurabilityTier::Global)
            .unwrap()
            .into_iter()
            .map(current_row_pair)
            .collect::<BTreeMap<_, _>>(),
        BTreeMap::from([(
            row_uuid,
            BTreeMap::from([("name".to_owned(), v("canonical authored"))]),
        )]),
        "repair publishes the exact row through the projected table"
    );

    // A same-row payload from a different physical table must not satisfy the
    // request, even if it is packaged under the requested transaction.
    let mut cross_physical = version_bundles[0].clone();
    let notes = renamed_schema
        .tables
        .iter()
        .find(|table| table.name == "notes")
        .expect("notes table");
    cross_physical.versions = vec![VersionRecord::from_cells(
        notes,
        renamed.id,
        row_uuid,
        Vec::new(),
        AuthorSubject::SYSTEM,
        tx_id.time.physical_ms(),
        AuthorSubject::SYSTEM,
        tx_id.time.physical_ms(),
        &BTreeMap::from([("body".to_owned(), v("wrong physical table"))]),
        None,
    )
    .unwrap()];
    let (_negative_dir, mut negative) = open_node_with_schema(node(0x94), schema());
    negative
        .apply_trusted_catalogue_snapshot_settled(core.catalogue_snapshot().unwrap())
        .unwrap();
    register_shape_binding(&mut negative, &shape, &binding);
    negative
        .apply_row_version_payloads_for_requests(&requests, vec![cross_physical])
        .unwrap();
    assert_eq!(
        negative
            .missing_known_state_row_version_refs(&pending_update)
            .unwrap(),
        requests,
        "a different physical table cannot satisfy a renamed repair request"
    );

    // Nor can a canonical payload with the correct physical identity but a
    // different transaction witness.
    let mut wrong_tx = version_bundles[0].clone();
    wrong_tx.tx.tx_id = TxId::new(TxTime(tx_id.time.0 + 1), tx_id.node);
    let (_wrong_tx_dir, mut wrong_tx_reader) = open_node_with_schema(node(0x95), schema());
    wrong_tx_reader
        .apply_trusted_catalogue_snapshot_settled(core.catalogue_snapshot().unwrap())
        .unwrap();
    register_shape_binding(&mut wrong_tx_reader, &shape, &binding);
    wrong_tx_reader
        .apply_row_version_payloads_for_requests(&requests, vec![wrong_tx])
        .unwrap();
    assert_eq!(
        wrong_tx_reader
            .missing_known_state_row_version_refs(&pending_update)
            .unwrap(),
        requests,
        "a repaired row must carry the requested transaction witness"
    );

    // A request whose projected table cannot be mapped is rejected before any
    // payload is considered: logical-name matching must fail closed.
    let unknown = crate::protocol::RowVersionRef::new("unknown", row_uuid, tx_id);
    assert!(
        core.row_version_payloads_for_refs(
            std::slice::from_ref(&unknown),
            crate::node::RowVersionRepairAuthorization::EnforceReadPolicy(AuthorSubject::SYSTEM),
        )
            .is_err(),
        "the serving repair path must reject an unknown projected table too"
    );
    assert!(
        wrong_tx_reader
            .apply_row_version_payloads_for_requests(&[unknown], version_bundles.clone())
            .is_err(),
        "unknown projected table mappings must never be accepted by name"
    );
}

/// A logical name can be reused after its old physical table was dropped. An
/// inline body from the old lineage must not silently cover a current result
/// member merely because their table names, row UUIDs, and transactions agree.
#[test]
fn inline_known_state_witness_rejects_reused_logical_table_name() {
    let original = renamed_tasks_schema();
    let without_tasks = build_public_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("notes").column("body", PublicColumnType::Text),
        ),
    );
    let reintroduced = build_public_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("notes")
                    .column("body", PublicColumnType::Text),
            )
            .table(
                PublicTableSchemaBuilder::new("tasks")
                    .column("name", PublicColumnType::Text),
            ),
    );
    let without_tasks_version = SchemaVersion::new(without_tasks.clone());
    let reintroduced_version = SchemaVersion::new(reintroduced.clone());
    let (_dir, mut receiver) = open_node_with_schema(node(0x96), original.clone());
    publish_schema_lineage(
        &mut receiver,
        without_tasks_version.clone(),
        MigrationLens::new(
            original.version_id(),
            without_tasks_version.id,
            Vec::new(),
        ).expect("valid migration lens"),
        ["notes"],
        ["tasks"],
    )
    .unwrap();
    publish_schema_lineage(
        &mut receiver,
        reintroduced_version.clone(),
        MigrationLens::new(
            without_tasks_version.id,
            reintroduced_version.id,
            vec![TableLens {
                source_table: "notes".to_owned(),
                target_table: "notes".to_owned(),
                ops: Vec::new(),
            }],
        ).expect("valid migration lens"),
        ["tasks"],
        Vec::<String>::new(),
    )
    .unwrap();
    receiver
        .apply_trusted_catalogue_message_settled(SyncMessage::SetCurrentWriteSchema {
            author: AuthorSubject::SYSTEM,
            pointer: CurrentWriteSchema {
                revision: 2,
                schema: reintroduced_version.id,
            },
        })
        .unwrap();
    let old_table_id = receiver
        .physical_table_id_for_schema(original.version_id(), "tasks")
        .unwrap();
    let new_table_id = receiver
        .physical_table_id_for_schema(reintroduced_version.id, "tasks")
        .unwrap();
    assert_ne!(
        old_table_id, new_table_id,
        "the active catalogue deliberately reuses `tasks` for a new physical lineage"
    );

    let shape = Query::from("tasks").validate(&reintroduced).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    register_shape_binding(&mut receiver, &shape, &binding);
    let tx_id = TxId::new(TxTime(60), node(0x97));
    let transaction = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 1,
        made_by: AuthorSubject::SYSTEM,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let task_row = row(0x98);
    let old_inline_task = VersionRecord::from_cells(
        &original.tables[0],
        original.version_id(),
        task_row,
        Vec::new(),
        AuthorSubject::SYSTEM,
        tx_id.time.physical_ms(),
        AuthorSubject::SYSTEM,
        tx_id.time.physical_ms(),
        &BTreeMap::from([("name".to_owned(), v("old physical task"))]),
        None,
    )
    .unwrap();
    let update = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: crate::protocol::SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        settled_through: GlobalTime::default(),
        reset_result_set: false,
        version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
            scope: crate::protocol::VersionBundleScope::CompleteTransaction,
            tx: transaction,
            versions: vec![old_inline_task],
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: DurabilityTier::Global,
        })],
        peer_payload_inventory: Default::default(),
        result_member_adds: vec![("tasks".to_owned().into(), task_row, tx_id).into()],
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    });
    assert_eq!(
        receiver.missing_known_state_row_version_refs(&update).unwrap(),
        vec![RowVersionRef::new("tasks", task_row, tx_id)],
        "an old same-named inline body does not cover the registered shape's reintroduced lineage"
    );
}
