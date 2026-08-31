//! RegisterShape and Subscribe admission, rejection, and protocol limits.

use super::*;

#[test]
fn db_subscription_stream_surfaces_upstream_rejection_after_open() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let db = open_db(0x51, owner, &schema);
    let (client_transport, mut server_transport) = duplex();
    let upstream = crate::db::block_on(db.connect_upstream(client_transport));

    let prepared = db.prepare_query(&Query::from("todos")).unwrap();
    let mut subscription = block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("local subscription should open before upstream response");
    assert!(matches!(
        block_on(subscription.next_raw()),
        Some(SubscriptionEvent::Delta { reset: true, .. })
    ));

    upstream.borrow_mut().tick().unwrap();
    let mut subscribed = None;
    while let Some(message) = server_transport.try_recv() {
        if let SyncMessage::Subscribe(subscribe) = message {
            subscribed = Some(subscribe.subscription);
        }
    }
    let subscribed = subscribed.expect("expected upstream subscribe command");

    server_transport
        .send(SyncMessage::SubscribeRejected {
            subscription: subscribed,
            reason: SubscribeRejectReason::UnsupportedShapeCapability {
                detail: "server does not support this maintained shape".to_owned(),
            },
        })
        .unwrap();
    upstream.borrow_mut().tick().unwrap();

    match block_on(subscription.next_raw()) {
        Some(SubscriptionEvent::Rejected {
            reason: SubscribeRejectReason::UnsupportedShapeCapability { detail },
        }) => assert_eq!(detail, "server does not support this maintained shape"),
        other => panic!("expected stream-carried rejection, got {other:?}"),
    }
}

#[test]
fn upstream_transport_rejects_forged_system_catalogue_publication() {
    let base = schema();
    let client_author = AuthorSubject::for_test_bytes([0x51; 16]);
    let client = open_db(0x51, client_author, &base);
    let (client_transport, mut upstream_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let target = SchemaVersion::new(build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .column("body", PublicColumnType::Text),
        ),
    ));
    let lens = MigrationLens::new(
        base.version_id(),
        target.id,
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
    upstream_transport
        .send(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(
                SchemaLineagePublication::author_from_prior(
                    &base,
                    &crate::protocol::PhysicalIdentityManifest::allocate(&base),
                    target.clone(),
                    lens,
                    Vec::<String>::new(),
                    Vec::<String>::new(),
                )
                .unwrap(),
            ),
        })
        .unwrap();

    let error = upstream.borrow_mut().tick().unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(error.message.contains("unauthorized catalogue update"));
    assert!(client.catalogue_schema(target.id).is_none());
}

#[test]
fn subscriber_connection_surfaces_server_table_not_found_without_silence() {
    let server_schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x53, AuthorSubject::SYSTEM, &server_schema);
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, owner);
    let shape_id = ShapeId(uuid::Uuid::from_bytes([0x52; 16]));
    let subscription = SubscriptionKey {
        shape_id,
        binding_id: BindingId(uuid::Uuid::nil()),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    // This must exercise the wire boundary because public query preparation
    // correctly refuses an unknown table before it can be sent. Previously the
    // server dropped this registration, so the following Subscribe would have
    // waited forever; the public stream routing is covered separately above.
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id,
            ast: ShapeAst::new(Query::from("people"), server_schema.version_id()),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id,
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();

    match try_recv_subscriber_payload(client_transport.as_mut()) {
        Some(SyncMessage::SubscribeRejected {
            subscription: rejected_subscription,
            reason:
                SubscribeRejectReason::ServerFailure {
                    code: SubscribeServerFailureCode::TableNotFound,
                },
        }) => assert_eq!(rejected_subscription, subscription),
        other => panic!("expected table-not-found rejection, got {other:?}"),
    }
}

#[test]
fn subscriber_connection_serves_default_ordered_window_alongside_unbounded_shape() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    seed(&server, "todos", cells("first", false, owner));
    seed(&server, "todos", cells("second", false, owner));

    // Protocol-level coverage for the current prepared/policy routing path:
    // keep an ordinary root and a default-ordered offset window live together.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let supported_shape = Query::from("todos").validate(&schema).unwrap();
    let window_shape = Query::from("todos")
        .offset(1)
        .limit(1)
        .validate(&schema)
        .unwrap();
    let supported_binding = supported_shape.bind(BTreeMap::new()).unwrap();
    let window_binding = window_shape.bind(BTreeMap::new()).unwrap();
    let supported_subscription = SubscriptionKey {
        shape_id: supported_shape.shape_id(),
        binding_id: supported_binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let window_subscription = SubscriptionKey {
        shape_id: window_shape.shape_id(),
        binding_id: window_binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: window_shape.shape_id(),
            ast: ShapeAst::from_validated(&window_shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: supported_shape.shape_id(),
            ast: ShapeAst::from_validated(&supported_shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: supported_shape.shape_id(),
            subscription: supported_subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    assert_view_update_for_subscription(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        supported_subscription,
    );

    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: window_shape.shape_id(),
            subscription: window_subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    seed(&server, "todos", cells("third", false, owner));
    let subscriptions = drive_subscriber_until_payloads(&subscriber, client_transport.as_mut(), 2)
        .into_iter()
        .map(|message| match message {
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription, ..
            }) => subscription,
            other => panic!("expected ViewUpdate, got {other:?}"),
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(
        subscriptions,
        BTreeSet::from([supported_subscription, window_subscription]),
        "both the unbounded and default-ordered window subscriptions remain served"
    );
}

#[test]
fn subscriber_connection_rejects_local_tier_register_shape() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    // Internal sync-loop coverage: public propagated subscriptions normalize
    // local reads before sending RegisterShape, so this sends protocol messages
    // directly to exercise the lower serving fence.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Local,
        read_view: ReadViewSpec::default(),
        ..RegisterShapeOptions::default()
    };
    let rejected_read_view = opts.read_view_key();

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_subscribe_rejected_unsupported_shape_capability_detail(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected local-tier registration rejection"),
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::nil()),
            read_view: rejected_read_view,
        },
        "global-tier registration",
    );

    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
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
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    assert_view_update_for_subscription(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
    );
}

#[test]
fn subscriber_connection_rejects_subscribe_without_link_shape_options() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);

    // Internal sync-loop coverage: pre-register the shape in the served node but
    // not on this link. The subscriber must still RegisterShape on its own
    // connection so serving options cannot leak across links.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let shared_node = server.node();
    let mut node = shared_node.borrow_mut();
    let outcome = crate::db::block_on(node.apply_sync_message(SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(&shape),
        opts: RegisterShapeOptions::default(),
    }))
    .unwrap();
    crate::db::block_on(node.persist_and_settle_outcome(outcome)).unwrap();
    drop(node);

    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            },
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );
}

#[test]
fn subscriber_connection_drops_oversized_known_state_and_keeps_serving() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
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
            subscription,
            values: Vec::new(),
            known_state: Some(KnownStateDeclaration::ExactVersionSet {
                versions: oversized_row_version_refs(MAX_KNOWN_STATE_EXACT_REFS + 1),
            }),
            delegated_session: None,
        }))
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "oversized known-state request should not receive a view update"
    );

    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: crate::time::GlobalTime::default(),
            }),
            delegated_session: None,
        }))
        .unwrap();

    assert_view_update_for_subscription(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
    );
}

#[derive(Clone, Copy, Debug)]
enum ActiveSubscriptionKeyReuse {
    ExactReplay,
    ChangedBinding,
    ChangedKnownState,
    MismatchedShape,
}

fn assert_protocol_view_update_rows(
    message: SyncMessage,
    expected_subscription: SubscriptionKey,
    expected_reset: bool,
    expected_rows: BTreeSet<RowUuid>,
) {
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        reset_result_set,
        result_member_adds,
        result_member_removes,
        ..
    }) = message
    else {
        panic!("expected ViewUpdate");
    };
    assert_eq!(subscription, expected_subscription);
    assert_eq!(reset_result_set, expected_reset);
    assert!(result_member_removes.is_empty());
    let added_rows = result_member_adds
        .iter()
        .map(|member| {
            member
                .as_real_row()
                .expect("whole-table query should only publish real rows")
                .row_uuid
        })
        .collect::<BTreeSet<_>>();
    assert_eq!(added_rows, expected_rows);
}

fn active_groove_subscriptions(server: &CoreDb) -> usize {
    server
        .node()
        .borrow()
        .runtime_stats_for_test()
        .active_subscriptions
}

fn assert_active_subscription_key_reuse(reuse: ActiveSubscriptionKeyReuse) {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let initial_a = seed(&server, "todos", cells("A", false, owner));
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos")
        .filter(eq(col("title"), param("wanted")))
        .validate(&schema)
        .unwrap();
    let other_shape = Query::from("todos")
        .filter(eq(col("done"), param("wanted_done")))
        .validate(&schema)
        .unwrap();
    let opts = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        propagate_upstream: false,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x7a; 16])),
        read_view: opts.read_view_key(),
    };
    let original_binding = shape
        .bind(BTreeMap::from([(
            "wanted".to_owned(),
            Value::String("A".to_owned()),
        )]))
        .unwrap();
    let maintained_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: original_binding.binding_id(),
        read_view: opts.read_view_key(),
    };

    for registered_shape in [&shape, &other_shape] {
        client_transport
            .send(SyncMessage::RegisterShape {
                shape_id: registered_shape.shape_id(),
                ast: ShapeAst::from_validated(registered_shape),
                opts: opts.clone(),
            })
            .unwrap();
    }

    let active_before = active_groove_subscriptions(&server);
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: vec![Value::String("A".to_owned())],
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    assert_protocol_view_update_rows(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
        true,
        BTreeSet::from([initial_a]),
    );
    assert_eq!(active_groove_subscriptions(&server), active_before + 1);
    {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("expected subscriber connection");
        };
        assert_eq!(state.served.len(), 1);
        assert_eq!(state.coverage_groups.len(), 1);
        assert_eq!(state.served_current_rows.len(), 0);
        assert!(
            state
                .peer
                .subscription_result_sets(maintained_subscription)
                .is_some()
        );
    }

    let dropped_before = server
        .node()
        .borrow()
        .sync_metrics()
        .dropped_peer_request_messages;
    let (shape_id, values, known_state, expected_dropped) = match reuse {
        ActiveSubscriptionKeyReuse::ExactReplay => (
            shape.shape_id(),
            vec![Value::String("A".to_owned())],
            None,
            0,
        ),
        ActiveSubscriptionKeyReuse::ChangedBinding => (
            shape.shape_id(),
            vec![Value::String("B".to_owned())],
            None,
            1,
        ),
        ActiveSubscriptionKeyReuse::ChangedKnownState => (
            shape.shape_id(),
            vec![Value::String("A".to_owned())],
            Some(KnownStateDeclaration::Fast {
                completeness: KnownStateCompleteness::FastCurrentMembership,
                position: GlobalTime(u64::MAX),
            }),
            0,
        ),
        ActiveSubscriptionKeyReuse::MismatchedShape => {
            (other_shape.shape_id(), vec![Value::Bool(true)], None, 1)
        }
    };
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id,
            subscription,
            values,
            known_state,
            delegated_session: None,
        }))
        .unwrap();
    for _ in 0..2 {
        subscriber.borrow_mut().tick().unwrap();
    }

    match reuse {
        ActiveSubscriptionKeyReuse::ExactReplay => assert_protocol_view_update_rows(
            try_recv_subscriber_payload(client_transport.as_mut())
                .expect("exact replay should refresh the active usage"),
            subscription,
            true,
            BTreeSet::from([initial_a]),
        ),
        ActiveSubscriptionKeyReuse::ChangedKnownState => assert_protocol_view_update_rows(
            try_recv_subscriber_payload(client_transport.as_mut())
                .expect("known-state replay should refresh the active usage"),
            subscription,
            true,
            BTreeSet::from([initial_a]),
        ),
        ActiveSubscriptionKeyReuse::ChangedBinding
        | ActiveSubscriptionKeyReuse::MismatchedShape => assert!(
            try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
            "{reuse:?} must not emit a reset or rejection for conflicting reuse"
        ),
    }
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "{reuse:?} emitted more than one replay response"
    );
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        dropped_before + expected_dropped,
        "{reuse:?} used the wrong malformed-request accounting"
    );
    assert_eq!(
        active_groove_subscriptions(&server),
        active_before + 1,
        "{reuse:?} installed another maintained subscription"
    );
    {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("expected subscriber connection");
        };
        assert_eq!(state.served.len(), 1, "{reuse:?} replaced the live usage");
        assert_eq!(
            state.coverage_groups.len(),
            1,
            "{reuse:?} installed conflicting coverage"
        );
        assert!(
            state
                .peer
                .subscription_result_sets(maintained_subscription)
                .is_some()
        );
    }

    let conflicting_row = seed(&server, "todos", cells("B", true, owner));
    for _ in 0..2 {
        subscriber.borrow_mut().tick().unwrap();
    }
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "{reuse:?} delivered a row from the conflicting binding or shape"
    );

    let original_row = seed(&server, "todos", cells("A", false, owner));
    assert_protocol_view_update_rows(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
        false,
        BTreeSet::from([original_row]),
    );
    assert_ne!(original_row, conflicting_row);
    for _ in 0..2 {
        subscriber.borrow_mut().tick().unwrap();
    }
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "{reuse:?} produced mixed or duplicate delivery"
    );

    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        active_groove_subscriptions(&server),
        active_before,
        "{reuse:?} left a maintained subscription behind"
    );
    let subscriber = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &subscriber.link else {
        panic!("expected subscriber connection");
    };
    assert!(state.served.is_empty());
    assert!(state.coverage_groups.is_empty());
    assert!(
        state
            .peer
            .subscription_result_sets(maintained_subscription)
            .is_none()
    );
}

/// A canonical replay keeps its existing subscription usage instead of creating a second producer.
/// alice's client replays its `A` request to the server: `alice ──Subscribe(A)──► server`.
/// Wire and internal-state inspection are necessary here because the contract is no extra reset,
/// maintained runtime, or coverage group—effects that public row state alone cannot distinguish.
#[test]
fn active_subscription_key_exact_canonical_replay_refreshes_existing_usage() {
    assert_active_subscription_key_reuse(ActiveSubscriptionKeyReuse::ExactReplay);
}

/// A reused key with a changed binding is rejected before it can alter the live `A` subscription.
/// alice first subscribes to `A`, then sends `B`: `alice ──Subscribe(A)──► server ◄──Subscribe(B)── alice`.
/// Wire and internal-state inspection prove reject-before-side-effects: no reply, replacement
/// producer, or changed coverage is observable through the client row view alone.
#[test]
fn active_subscription_key_drops_changed_binding_before_side_effects() {
    assert_active_subscription_key_reuse(ActiveSubscriptionKeyReuse::ChangedBinding);
}

/// A replay with changed known state refreshes the already-live canonical subscription without rebinding it.
/// alice opens `A`, then supplies a known-state declaration: `alice ──Subscribe(A)──► server ◄──Subscribe(A, known)── alice`.
/// Wire and internal-state inspection are needed to prove the replay causes neither another reset
/// nor another maintained usage, distinctions that final rows cannot expose.
#[test]
fn active_subscription_key_changed_known_state_refreshes_existing_usage() {
    assert_active_subscription_key_reuse(ActiveSubscriptionKeyReuse::ChangedKnownState);
}

/// A reused key with a different query shape is rejected before it can replace the live subscription.
/// alice opens the title-`A` shape, then submits the done-`true` shape under that key:
/// `alice ──Subscribe(title=A)──► server ◄──Subscribe(done=true)── alice`.
/// Wire and internal-state inspection establish reject-before-side-effects because client rows
/// cannot reveal a transient replacement, duplicate producer, or conflicting coverage group.
#[test]
fn active_subscription_key_drops_mismatched_shape_before_side_effects() {
    assert_active_subscription_key_reuse(ActiveSubscriptionKeyReuse::MismatchedShape);
}

/// An ordinary whole-table subscription cannot take the key owned by the server's current-row producer.
/// alice opens current rows, then sends an ordinary whole-table subscribe with that key:
/// `server ──current rows──► alice ──ordinary Subscribe(same key)──► server`.
/// Wire and internal-state inspection are required to show no second producer or coverage is
/// installed; the final row set would otherwise look identical.
#[test]
fn current_row_subscription_key_rejects_ordinary_whole_table_collision() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let initial_row = seed(&server, "todos", cells("initial", false, owner));
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let active_before = active_groove_subscriptions(&server);

    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    let current_rows_update =
        try_recv_subscriber_payload(client_transport.as_mut()).expect("current-row view update");
    let current_rows_subscription = match &current_rows_update {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. }) => {
            *subscription
        }
        other => panic!("expected current-row ViewUpdate, got {other:?}"),
    };
    assert_protocol_view_update_rows(
        current_rows_update,
        current_rows_subscription,
        false,
        BTreeSet::from([initial_row]),
    );
    assert_eq!(
        current_rows_subscription,
        server
            .node()
            .borrow()
            .whole_table_subscription_key("todos")
            .unwrap()
    );
    assert_eq!(active_groove_subscriptions(&server), active_before + 1);

    let whole_table_shape = Query::from("todos").validate(&schema).unwrap();
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: whole_table_shape.shape_id(),
            ast: ShapeAst::from_validated(&whole_table_shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    let dropped_before = server
        .node()
        .borrow()
        .sync_metrics()
        .dropped_peer_request_messages;
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: whole_table_shape.shape_id(),
            subscription: current_rows_subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    for _ in 0..2 {
        subscriber.borrow_mut().tick().unwrap();
    }

    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "ordinary collision must not emit a second whole-table producer"
    );
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        dropped_before + 1
    );
    assert_eq!(
        active_groove_subscriptions(&server),
        active_before + 1,
        "ordinary collision installed another maintained subscription"
    );
    {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("expected subscriber connection");
        };
        assert!(state.served.is_empty());
        assert!(state.coverage_groups.is_empty());
        assert_eq!(state.served_current_rows.len(), 1);
        assert_eq!(
            state.served_current_rows.get(&current_rows_subscription),
            Some(&"todos".to_owned())
        );
        assert!(
            state
                .peer
                .subscription_result_sets(current_rows_subscription)
                .is_some()
        );
    }

    let added_row = seed(&server, "todos", cells("current only", false, owner));
    assert_protocol_view_update_rows(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        current_rows_subscription,
        false,
        BTreeSet::from([added_row]),
    );
    for _ in 0..2 {
        subscriber.borrow_mut().tick().unwrap();
    }
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "whole-table collision produced mixed or duplicate delivery"
    );
}

/// The current-row producer refuses a key already owned by an ordinary whole-table subscription.
/// alice starts the ordinary subscription, then the server tries to serve current rows:
/// `alice ──ordinary Subscribe(key)──► server ──serve_current_rows(key)──► alice`.
/// Wire and internal-state inspection prove the refusal leaves the ordinary owner and its runtime
/// intact without a transient current-row producer, which public rows alone cannot establish.
#[test]
fn current_row_subscription_key_refuses_existing_ordinary_owner() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let initial_row = seed(&server, "todos", cells("initial", false, owner));
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let subscription = server
        .node()
        .borrow()
        .whole_table_subscription_key("todos")
        .unwrap();
    let active_before = active_groove_subscriptions(&server);

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
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    let error = subscriber
        .borrow_mut()
        .serve_current_rows("todos")
        .unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        error
            .message
            .contains("already owned by an ordinary subscription")
    );
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "current-row refusal must not send from either producer"
    );
    assert_eq!(active_groove_subscriptions(&server), active_before);
    {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("expected subscriber connection");
        };
        assert!(state.served_current_rows.is_empty());
    }

    subscriber.borrow_mut().tick().unwrap();
    assert_protocol_view_update_rows(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("ordinary owner should publish its pending initial reset"),
        subscription,
        true,
        BTreeSet::from([initial_row]),
    );
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "ordinary owner emitted more than one initial reset"
    );
    assert_eq!(active_groove_subscriptions(&server), active_before + 1);
    {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("expected subscriber connection");
        };
        assert_eq!(state.served.len(), 1);
        assert_eq!(state.coverage_groups.len(), 1);
        assert!(state.served.contains_key(&subscription));
        assert!(state.served_current_rows.is_empty());
    }
    let added_row = seed(&server, "todos", cells("ordinary only", false, owner));
    assert_protocol_view_update_rows(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
        false,
        BTreeSet::from([added_row]),
    );

    client_transport
        .send(SyncMessage::Unsubscribe { subscription })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(active_groove_subscriptions(&server), active_before);
    let subscriber = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &subscriber.link else {
        panic!("expected subscriber connection");
    };
    assert!(state.served.is_empty());
    assert!(state.coverage_groups.is_empty());
    assert!(state.served_current_rows.is_empty());
    assert!(state.peer.subscription_result_sets(subscription).is_none());
}

#[test]
fn subscriber_connection_drops_oversized_fetch_row_versions_and_keeps_serving() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::FetchRowVersions {
            requests: oversized_row_version_refs(MAX_FETCH_ROW_VERSIONS + 1),
            delegated_session: None,
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );

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
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    assert_view_update_for_subscription(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
    );
}

#[test]
fn subscriber_connection_drops_mismatched_shape_id_and_keeps_serving() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    seed(&server, "todos", cells("after malformed", false, owner));

    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let other_shape = Query::from("todos")
        .filter(eq(col("done"), lit(true)))
        .validate(&schema)
        .unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: other_shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        1
    );

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
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    assert_view_update_for_subscription(
        drive_subscriber_until_payload(&subscriber, client_transport.as_mut()),
        subscription,
    );
}

#[test]
fn local_live_subscription_requests_global_upstream_coverage() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);

    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    seed(&server, "todos", cells("first", false, owner));

    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, ReadOpts::default()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());
    client.tick().unwrap();
    server.tick().unwrap();

    // Internal sync-loop coverage: the public subscription is local-tier, but
    // the remote coverage request must be settled-only because local state is
    // link-local to the subscribing client.
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        coverage_groups, ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(coverage_groups.len(), 1);
    let coverage = coverage_groups.keys().next().unwrap();
    assert_eq!(coverage.opts.tier, DurabilityTier::Global);
    assert!(coverage.opts.read_view.is_default());
}

#[test]
fn edge_live_subscription_requests_global_upstream_coverage() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, server_transport) = duplex();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let query = Query::from("todos");
    let mut subscription = prepared_subscribe(&client, &query, edge_subscribe_opts()).unwrap();
    assert!(opened_rows(block_on(subscription.next_raw()).unwrap()).is_empty());

    client.tick().unwrap();
    server.tick().unwrap();

    // Edge-tier is the local visible tier for browser clients, but propagated
    // upstream coverage is still registered at global tier. Edge serving is
    // link-local; the subscription's settled contract is satisfied when the
    // globally settled coverage arrives back at the client.
    let subscriber_ref = subscriber.borrow();
    let ConnectionLink::Subscriber(SubscriberConnectionState {
        coverage_groups, ..
    }) = &subscriber_ref.link
    else {
        panic!("expected subscriber connection");
    };
    assert_eq!(coverage_groups.len(), 1);
    let coverage = coverage_groups.keys().next().unwrap();
    assert_eq!(coverage.opts.tier, DurabilityTier::Global);
    assert!(coverage.opts.read_view.is_default());
}

#[test]
fn subscriber_connection_rejects_non_global_register_shape_options() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);

    // Internal sync-loop coverage: public APIs normalize local subscriptions to
    // global upstream coverage. Malformed/direct peers must not install an
    // unsupported edge-tier subscription.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let edge_opts = RegisterShapeOptions {
        tier: DurabilityTier::Edge,
        read_view: ReadViewSpec::default(),
        ..RegisterShapeOptions::default()
    };
    let rejected_read_view = edge_opts.read_view_key();

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: edge_opts,
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert_subscribe_rejected_unsupported_shape_capability_detail(
        try_recv_subscriber_payload(client_transport.as_mut())
            .expect("expected edge-tier registration rejection"),
        SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::nil()),
            read_view: rejected_read_view,
        },
        "global-tier registration",
    );
}

#[test]
fn subscriber_connection_accepts_array_subquery_register_shape_for_serving_subscription() {
    let schema = relation_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);

    // Internal sync-loop coverage: array-subquery subscriptions are served as
    // flat relation-edge facts, so direct wire registration should be accepted.
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let shape = Query::from("users")
        .array_subquery(ArraySubquery::new("todos", "todos", "owner_id", "id"))
        .validate(&schema)
        .unwrap();

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert!(
        try_recv_subscriber_payload(client_transport.as_mut()).is_none(),
        "registering a supported array-subquery shape should not emit a rejection"
    );
}

#[test]
fn subscriber_connection_accepts_relation_register_shape_for_serving_subscription() {
    let schema = relation_schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    server
        .insert_with_id(
            "users",
            row(0xa1),
            BTreeMap::from([("name".to_owned(), Value::String("alice".to_owned()))]),
        )
        .unwrap();
    server
        .insert_with_id(
            "todos",
            row(0x11),
            BTreeMap::from([
                ("title".to_owned(), Value::String("alice todo".to_owned())),
                ("owner_id".to_owned(), Value::Uuid(row(0xa1).0)),
            ]),
        )
        .unwrap();
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, client_author);

    let relation = RelationQuery {
        rel: RelationExpr::Project {
            input: Box::new(RelationExpr::Join {
                left: Box::new(RelationExpr::TableScan {
                    table: "users".to_owned(),
                    alias: None,
                }),
                right: Box::new(RelationExpr::TableScan {
                    table: "todos".to_owned(),
                    alias: Some("__hop_0".to_owned()),
                }),
                on: vec![crate::query::RelationJoinCondition {
                    left: RelationColumnRef {
                        scope: Some("users".to_owned()),
                        column: "id".to_owned(),
                    },
                    right: RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    },
                }],
                join_kind: RelationJoinKind::Inner,
            }),
            columns: vec![
                crate::query::RelationProjectColumn {
                    alias: "id".to_owned(),
                    expr: RelationProjectExpr::RowId(RelationRowIdRef::Current),
                },
                crate::query::RelationProjectColumn {
                    alias: "title".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "title".to_owned(),
                    }),
                },
                crate::query::RelationProjectColumn {
                    alias: "owner_id".to_owned(),
                    expr: RelationProjectExpr::Column(RelationColumnRef {
                        scope: Some("__hop_0".to_owned()),
                        column: "owner_id".to_owned(),
                    }),
                },
            ],
        },
    };
    let normalized = relation_query_to_query(&relation)
        .unwrap()
        .validate(&schema)
        .unwrap();
    let binding = normalized.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: normalized.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: normalized.shape_id(),
            ast: ShapeAst::new_relation(relation, schema.version_id()),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: normalized.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();

    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription: served,
        result_member_adds,
        ..
    }) = drive_subscriber_until_payload(&subscriber, client_transport.as_mut())
    else {
        panic!("expected relation facade subscription view update");
    };
    assert_eq!(served, subscription);
    assert!(
        result_member_adds.iter().any(|member| {
            let Some(member) = member.as_real_row() else {
                return false;
            };
            member.table.as_str() == "todos" && member.row_uuid == row(0x11)
        }),
        "relation facade subscription should deliver the projected target row"
    );
}
