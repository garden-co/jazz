//! Local deltas and branch opening, isolation, reconnect, and teardown.

use super::*;

#[test]
fn local_subscription_emits_removed_row_for_fire_and_forget_delete() {
    let schema = schema();
    let owner = AuthorId::from_bytes([0x31; 16]);
    let mut db = open_db(0x31, owner, &schema);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&mut db, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let row_id = row(0x31);
    db.insert_with_id("todos", row_id, cells("delete me", false, owner))
        .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![row_id]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete("todos", row_id).unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

#[test]
fn one_shot_and_subscription_rows_keep_identical_record_descriptors() {
    let schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]);
    let owner = AuthorId::from_bytes([0x32; 16]);
    let mut db = open_db(0x32, owner, &schema);
    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&mut db, &query, ReadOpts::default()).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();

    let row_id = row(0x32);
    db.insert_with_id(
        "todos",
        row_id,
        BTreeMap::from([
            (
                "title".to_owned(),
                Value::String("descriptor parity".to_owned()),
            ),
            ("done".to_owned(), Value::Bool(false)),
        ]),
    )
    .unwrap();
    let (added, _, _) = delta_rows(block_on(subscription.next_raw()).unwrap());
    let one_shot = prepared_all(&mut db, &query, ReadOpts::default());
    assert_eq!(added.len(), 1);
    assert_eq!(one_shot.len(), 1);
    let table = &schema.tables[0];
    assert_eq!(
        added[0].cell(&table, "title"),
        Some(Value::String("descriptor parity".to_owned()))
    );
    assert_eq!(added[0].cell(&table, "done"), Some(Value::Bool(false)));
    assert_eq!(added[0].encoded_record(), one_shot[0].encoded_record());
}

#[test]
fn session_scoped_subscription_emits_removed_row_for_owned_delete() {
    let schema = owner_id_public_schema();
    let author = AuthorId::from_bytes([0x32; 16]);
    let mut db = open_db(0x32, AuthorId::SYSTEM, &schema);
    let user_id = "local-first-user";
    let _ = db.set_identity_claims(
        author,
        BTreeMap::from([("user_id".to_owned(), Value::String(user_id.to_owned()))]),
    );
    let query = Query::from("messages");
    let prepared = prepared(&mut db, &query);
    let mut subscription =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), author)).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    let row_id = row(0x32);
    db.insert_with_id_for_identity(
        author,
        "messages",
        row_id,
        BTreeMap::from([
            ("body".to_owned(), Value::String("delete me".to_owned())),
            ("owner_id".to_owned(), Value::String(user_id.to_owned())),
        ]),
    )
    .unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![row_id]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.delete_for_identity(author, "messages", row_id).unwrap();
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .into_iter()
            .map(|row| row.row_uuid)
            .collect::<Vec<_>>(),
        vec![row_id]
    );
}

#[test]
fn subscription_retains_a_plan_from_its_selected_authorization_mode() {
    let schema = owner_id_public_schema();
    let author = AuthorId::from_bytes([0x33; 16]);
    let mut db = open_db(0x33, author, &schema);
    let _ = db.set_identity_claims(
        author,
        BTreeMap::from([("user_id".to_owned(), Value::String("alice".to_owned()))]),
    );
    let prepared = prepared(
        &mut db,
        &Query::from("messages").filter(eq(col("owner_id"), claim("user_id"))),
    );

    let client = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    assert_eq!(
        client.retained_plan_authorization_mode(),
        Some(QueryAuthorizationMode::ClientLocal)
    );

    let trusted =
        block_on(db.subscribe_for_identity(&prepared, ReadOpts::default(), author)).unwrap();
    assert_eq!(
        trusted.retained_plan_authorization_mode(),
        Some(QueryAuthorizationMode::TrustedServing)
    );
}

#[test]
fn client_local_branch_subscription_survives_sparse_first_write_delete_and_restore() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    db.node
        .node
        .borrow_mut()
        .create_branch(branch)
        .expect("create empty branch");
    let query = db.table("todos");
    let prepared_query = prepared(&mut db, &query);
    let opts = ReadOpts {
        propagation: Propagation::LocalOnly,
        ..branch_read_opts()
    };
    let mut subscription = block_on(db.subscribe(&prepared_query, opts))
        .expect("open ClientLocal subscription before sparse partition exists");
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 10)
                .cells(doctest_support::todo_cells("first pending overlay", false)),
        )
        .expect("first pending branch write creates durable partition");
    assert_eq!(db.node.refresh_subscriptions().unwrap(), 1);
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![row(0x42)]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 11).deletion(DeletionEvent::Deleted),
        )
        .expect("delete pending branch row");
    assert_eq!(db.node.refresh_subscriptions().unwrap(), 1);
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert_eq!(
        removed
            .iter()
            .map(|removed| removed.row_uuid)
            .collect::<Vec<_>>(),
        vec![row(0x42)]
    );

    db.node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 12).deletion(DeletionEvent::Restored),
        )
        .expect("restore pending branch row");
    assert_eq!(db.node.refresh_subscriptions().unwrap(), 1);
    let (added, updated, removed) = delta_rows(block_on(subscription.next_raw()).unwrap());
    assert_eq!(row_ids(&added), vec![row(0x42)]);
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn denied_branch_subscription_does_not_allocate_sparse_source() {
    let branch_policy = Query::from("jazz_branches").join_via(
        "branch_access",
        "branch_id",
        [eq(col("user_id"), claim("sub"))],
    );
    let schema = JazzSchema::new([
        TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
                ColumnSchema::new("owner_id", ColumnType::Uuid),
            ],
        ),
        TableSchema::new(
            "branch_access",
            [
                ColumnSchema::new("branch_id", ColumnType::Uuid),
                ColumnSchema::new("user_id", ColumnType::Uuid),
            ],
        )
        .with_reference("branch_id", "jazz_branches"),
    ])
    .with_branch_read_policy(branch_policy);
    let denied = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let branch = BranchId::from_bytes([0x42; 16]);
    server
        .node()
        .borrow_mut()
        .create_root_branch(branch)
        .expect("create empty root branch");
    assert!(
        !server
            .node()
            .borrow()
            .branch_subscription_source_exists_for_test("todos", schema.version_id(), branch,)
    );

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, denied);
    let query = Query::from("todos");
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Branch { branch: branch.0 },
            ..Default::default()
        },
        propagate_upstream: true,
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
            opts,
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
    let update = try_recv_subscriber_payload(client_transport.as_mut())
        .expect("denied branch subscription receives an empty view");
    assert!(matches!(
        update,
        SyncMessage::ViewUpdate {
            reset_result_set: true,
            result_member_adds,
            ..
        } if result_member_adds.is_empty()
    ));
    assert!(
        !server
            .node()
            .borrow()
            .branch_subscription_source_exists_for_test("todos", schema.version_id(), branch,)
    );

    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        !server
            .node()
            .borrow()
            .branch_subscription_source_exists_for_test("todos", schema.version_id(), branch,)
    );
}

#[test]
fn branch_subscription_reconnects_and_re_settles_after_a_fresh_view_receipt() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    server
        .node()
        .borrow_mut()
        .create_branch_as(branch, client_author)
        .expect("server creates matching branch metadata");
    client
        .create_branch_with_id(branch)
        .expect("client creates matching branch metadata");
    let branch_write = server
        .node()
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch,
            MergeableCommit::new("todos", row(0x42), 10).cells(cells(
                "branch-only",
                false,
                client_author,
            )),
        )
        .expect("commit accepted branch row");
    server
        .node()
        .borrow_mut()
        .apply_fate_update(
            branch_write,
            Fate::Accepted,
            Some(GlobalSeq(1)),
            Some(DurabilityTier::Global),
        )
        .expect("globally accept branch row");

    let (first_client_transport, first_server_transport) = duplex();
    let first_upstream = client.connect_upstream(first_client_transport);
    let mut _first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let query = Query::from("todos");
    let branch_opts = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        read_view: branch_read_opts().read_view,
        ..ReadOpts::default()
    };
    let mut subscription = prepared_subscribe(&mut client, &query, branch_opts).unwrap();
    assert!(!event_settled(&block_on(subscription.next_raw()).unwrap()));

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let settled = block_on(subscription.next_raw()).unwrap();
    assert!(event_settled(&settled));
    let (added, _, _) = delta_rows(settled);
    assert!(
        added.iter().any(|current| current.row_uuid() == row(0x42)),
        "fresh branch coverage must deliver the selected overlay row"
    );

    assert!(client.detach_connection(&first_upstream));
    let disconnected = subscription
        .receiver
        .try_recv()
        .expect("disconnect must publish a branch settlement demotion");
    assert!(
        !event_settled(&disconnected),
        "disconnect must demote a branch Edge/Global subscription"
    );

    let (reconnected_client_transport, reconnected_server_transport) = duplex();
    let mut _reconnected_upstream = client.connect_upstream(reconnected_client_transport);
    let _reconnected_subscriber =
        server.accept_subscriber(reconnected_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let binding_view = BindingViewKey::new(
        query.validate(&schema).unwrap().shape_id(),
        query
            .validate(&schema)
            .unwrap()
            .bind(BTreeMap::new())
            .unwrap()
            .binding_id(),
        RegisterShapeOptions {
            tier: DurabilityTier::Global,
            read_view: branch_read_opts().read_view,
            ..RegisterShapeOptions::default()
        }
        .read_view_key(),
    );
    let receipts = client.node.active_authority_view_receipts.borrow();
    assert!(
        receipts
            .as_ref()
            .is_some_and(|receipts| receipts.binding_views.contains(&binding_view)),
        "reconnect did not install the branch binding receipt"
    );
    drop(receipts);
    let node = client.node.node.borrow();
    assert!(
        client
            .node
            .node
            .borrow()
            .has_settled_result_set(binding_view),
        "reconnect did not restore branch settled result membership"
    );
    assert!(
        !node.opening_pending_for_binding_view(binding_view),
        "reconnect left branch binding opening pending"
    );
    drop(node);
    assert!(
        subscription_is_settled(
            &client.node.node.borrow(),
            &client.node.active_authority_view_receipts,
            &query.validate(&schema).unwrap(),
            &query
                .validate(&schema)
                .unwrap()
                .bind(BTreeMap::new())
                .unwrap(),
            DurabilityTier::Global,
            branch_read_opts().read_view,
            true,
            true,
        ),
        "the recovered branch state should be settled before its stream refresh"
    );
    let replayed = block_on(subscription.next_raw()).unwrap();
    assert!(
        !event_settled(&replayed),
        "reconnect must first publish the branch overlay replay as provisional"
    );
    let (added, _, _) = delta_rows(replayed);
    assert!(
        added.iter().any(|current| current.row_uuid() == row(0x42)),
        "the provisional replay must retain the branch overlay row"
    );
    let mut resettled = block_on(subscription.next_raw()).unwrap();
    for _ in 0..2 {
        if event_settled(&resettled) {
            break;
        }
        resettled = block_on(subscription.next_raw()).unwrap();
    }
    assert!(
        event_settled(&resettled),
        "a fresh selected-authority branch view must re-settle after reconnect"
    );
    let (added, updated, removed) = delta_rows(resettled);
    assert!(
        added.iter().any(|current| current.row_uuid() == row(0x42)),
        "the authoritative reset must retain the branch overlay row"
    );
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn branch_one_shot_waits_for_metadata_and_keeps_sibling_result_identity() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    let branch_a = BranchId::from_bytes([0x42; 16]);
    let branch_b = BranchId::from_bytes([0x43; 16]);
    for (branch, row_id, title, seq) in [
        (branch_a, row(0x44), "branch-a", GlobalSeq(1)),
        (branch_b, row(0x45), "branch-b", GlobalSeq(2)),
    ] {
        server
            .node()
            .borrow_mut()
            .create_branch_as(branch, client_author)
            .expect("server creates visible branch metadata");
        let tx = server
            .node()
            .borrow_mut()
            .commit_mergeable_on_branch(
                branch,
                MergeableCommit::new("todos", row_id, 10).cells(cells(title, false, client_author)),
            )
            .expect("server writes branch overlay");
        server
            .node()
            .borrow_mut()
            .apply_fate_update(tx, Fate::Accepted, Some(seq), Some(DurabilityTier::Global))
            .expect("server globally accepts branch overlay");
    }

    let (client_transport, server_transport) = duplex();
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let prepared = prepared(&mut client, &query);
    let opts_for = |branch: BranchId, tier| ReadOpts {
        tier,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        read_view: crate::protocol::ReadViewSpec {
            source: crate::protocol::ReadViewSourceSpec::Branch { branch: branch.0 },
            ..Default::default()
        },
        ..ReadOpts::default()
    };

    let read = |client: &mut Db, branch, tier, expected| {
        let opts = opts_for(branch, tier);
        let attachment = client
            .attach_query_with_opts(&prepared, opts.clone())
            .expect("attach branch one-shot coverage");
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        assert!(
            client.query_attachment_is_covered(&attachment),
            "metadata and the authoritative branch snapshot arrive before publication"
        );
        let rows = block_on(client.all(&prepared, opts)).expect("read covered branch snapshot");
        assert_eq!(row_ids(&rows), vec![expected]);
        client.detach_query(attachment);
    };

    read(&mut client, branch_a, DurabilityTier::Global, row(0x44));
    client
        .node
        .node
        .borrow_mut()
        .commit_mergeable_on_branch(
            branch_a,
            MergeableCommit::new("todos", row(0x46), 20).cells(cells(
                "unsent local pending",
                false,
                client_author,
            )),
        )
        .expect("stage an unsent local branch overlay");
    read(&mut client, branch_a, DurabilityTier::Edge, row(0x44));
    read(&mut client, branch_a, DurabilityTier::Global, row(0x44));
    read(&mut client, branch_b, DurabilityTier::Global, row(0x45));
}

#[test]
fn empty_branch_subscription_reconnects_with_a_settlement_only_refresh() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xc1, client_author, &schema);
    let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
    server
        .node()
        .borrow_mut()
        .create_branch_as(branch, client_author)
        .expect("server creates matching branch metadata");
    client
        .create_branch_with_id(branch)
        .expect("client creates matching branch metadata");

    let (first_client_transport, first_server_transport) = duplex();
    let first_upstream = client.connect_upstream(first_client_transport);
    let mut _first_subscriber = server.accept_subscriber(first_server_transport, client_author);
    let query = Query::from("todos");
    let branch_opts = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        read_view: branch_read_opts().read_view,
        ..ReadOpts::default()
    };
    let mut subscription = prepared_subscribe(&mut client, &query, branch_opts).unwrap();
    assert!(!event_settled(&block_on(subscription.next_raw()).unwrap()));

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let settled = subscription
        .receiver
        .try_recv()
        .expect("the initial branch receipt must publish a settlement-only refresh");
    assert!(event_settled(&settled));
    let (added, updated, removed) = delta_rows(settled);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());

    assert!(client.detach_connection(&first_upstream));
    let disconnected = subscription
        .receiver
        .try_recv()
        .expect("disconnect must publish a branch settlement demotion");
    assert!(
        !event_settled(&disconnected),
        "disconnect must demote a branch Edge/Global subscription"
    );

    let (reconnected_client_transport, reconnected_server_transport) = duplex();
    let mut _reconnected_upstream = client.connect_upstream(reconnected_client_transport);
    let _reconnected_subscriber =
        server.accept_subscriber(reconnected_server_transport, client_author);
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    let resettled = subscription
        .receiver
        .try_recv()
        .expect("a fresh branch receipt must publish a settlement-only refresh");
    assert!(event_settled(&resettled));
    let (added, updated, removed) = delta_rows(resettled);
    assert!(added.is_empty());
    assert!(updated.is_empty());
    assert!(removed.is_empty());
}

#[test]
fn dropping_a_branch_subscription_releases_its_upstream_coverage() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([0xc1; 16]);
    let mut client = open_db(0xc1, client_author, &schema);
    client
        .create_branch_with_id(BranchId(uuid::Uuid::from_bytes([0x42; 16])))
        .expect("client creates branch metadata");
    let baseline = client.runtime_stats_for_test().active_subscriptions;
    let query = Query::from("todos");
    let opts = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        read_view: branch_read_opts().read_view,
        ..ReadOpts::default()
    };

    let mut subscription = prepared_subscribe(&mut client, &query, opts).unwrap();
    let _ = block_on(subscription.next_raw()).unwrap();
    assert_eq!(
        client.runtime_stats_for_test().active_subscriptions,
        baseline + 1
    );
    assert_eq!(pending_upstream_subscribe_count(&client), 1);

    drop(subscription);
    assert_eq!(
        client.runtime_stats_for_test().active_subscriptions,
        baseline
    );
    assert_eq!(pending_upstream_unsubscribe_count(&client), 1);
}

#[test]
fn include_deleted_fails_closed_on_live_subscription_apis() {
    let mut db = doctest_support::block_on(doctest_support::open_todos_db()).unwrap();
    let query = db.table("todos");
    let prepared_query = prepared(&mut db, &query);
    let opts = ReadOpts {
        include_deleted: true,
        ..ReadOpts::default()
    };

    assert_unsupported_subscription_include_deleted(expect_error(doctest_support::block_on(
        db.subscribe(&prepared_query, opts.clone()),
    )));
    assert_unsupported_subscription_include_deleted(expect_error(doctest_support::block_on(
        db.subscribe_for_identity(&prepared_query, opts.clone(), db.identity.author),
    )));

    let rows = doctest_support::block_on(db.all(&prepared_query, opts)).unwrap();
    assert!(rows.is_empty());
}
