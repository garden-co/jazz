//! Authorization-scope protocol validation and hostile-peer boundaries.

use super::*;

fn schema_with_explicit_public_read() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::True)),
        ),
    )
}

#[test]
fn legacy_authorization_scope_subscribe_is_rejected_before_shape_admission() {
    let schema = schema_with_explicit_public_read();
    let identity = AuthorId::from_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::AuthorizationScopeSubscribe {
            subscribe: Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: Vec::new(),
                known_state: None,
            },
            purpose: AuthorizationScopePurpose {
                action: PermissionAdviceAction::Read {
                    table: "todos".to_owned(),
                    row: row(1),
                },
            },
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    let mut received_view = false;
    let mut received_receipt = false;
    while let Some(message) = client_transport.try_recv() {
        match message {
            SyncMessage::CatalogueSnapshot(_) => {}
            SyncMessage::ViewUpdate {
                subscription: received,
                ..
            } => {
                assert_eq!(received, subscription);
                received_view = true;
            }
            SyncMessage::AuthorizationScopeReceipt {
                subscription: received,
                receipt,
            } => {
                assert!(received_view, "receipt must follow its support view");
                assert_eq!(received, subscription);
                assert_eq!(receipt.link, *identity.as_bytes());
                assert_eq!(receipt.key.subject, identity);
                received_receipt = true;
            }
            other => panic!("unexpected authorization-scope response: {other:?}"),
        }
    }
    assert!(!received_view);
    assert!(!received_receipt);
}

/* Retired with the caller-authored scope protocol.  Authority-owned intent
 * coverage lives with the permission advice tests above.
fn legacy_authorization_scope_subscribe_refreshes_claims() {
    server.node().borrow_mut().set_session_claims(
        identity,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    subscriber.borrow_mut().tick().unwrap();
    let mut refreshed_view = false;
    let mut refreshed_receipt = None;
    while let Some(message) = client_transport.try_recv() {
        match message {
            SyncMessage::ViewUpdate { .. } => refreshed_view = true,
            SyncMessage::AuthorizationScopeReceipt { receipt, .. } => {
                assert!(
                    refreshed_view,
                    "replacement receipt follows replacement view"
                );
                refreshed_receipt = Some(receipt);
            }
            other => panic!("unexpected claims-refresh response: {other:?}"),
        }
    }
    let refreshed_receipt = refreshed_receipt.expect("claims change must reissue receipt");
    assert_eq!(refreshed_receipt.claims_revision, 1);
}
*/

#[test]
fn authorization_scope_rejects_unrelated_caller_intent() {
    let schema = schema_with_explicit_public_read();
    let identity = AuthorId::from_bytes([0xc2; 16]);
    let server = open_core(0x5f, AuthorId::SYSTEM, &schema);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);
    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    client_transport
        .send(SyncMessage::AuthorizationScopeSubscribe {
            subscribe: Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: Vec::new(),
                known_state: None,
            },
            purpose: AuthorizationScopePurpose {
                action: PermissionAdviceAction::Read {
                    table: "not_todos".to_owned(),
                    row: row(1),
                },
            },
        })
        .unwrap();

    subscriber.borrow_mut().tick().unwrap();
    assert!(
        client_transport.try_recv().is_none(),
        "an unrelated action must not produce a support view or receipt"
    );
}

#[test]
fn legacy_authorization_scope_subscribe_never_assembles_multiple_clauses() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .column("owner", PublicColumnType::Uuid),
            )
            .table(PublicTableSchemaBuilder::new("support_using"))
            .table(PublicTableSchemaBuilder::new("support_check")),
    );
    let identity = AuthorId::from_bytes([0xc3; 16]);
    let server = open_core(0x60, AuthorId::SYSTEM, &schema);
    server
        .node()
        .borrow_mut()
        .mutate_current_schema_for_testing(|compiled| {
            compiled
                .tables
                .iter_mut()
                .find(|table| table.name == "todos")
                .expect("todos table")
                .write_policies = WritePolicies {
                update_using: Some(Query::from("support_using")),
                update_check: Some(Query::from("support_check")),
                ..WritePolicies::default()
            };
        });
    let action = PermissionAdviceAction::Update {
        table: "todos".to_owned(),
        row: row(1),
        patch: BTreeMap::new(),
    };
    let expected = server
        .node()
        .borrow()
        .authorization_support_scope(identity, &action)
        .unwrap();
    assert_eq!(expected.subscriptions.len(), 2);
    let entries = expected
        .subscriptions
        .iter()
        .map(|(shape, binding)| {
            let subscription = SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: RegisterShapeOptions::default().read_view_key(),
            };
            (shape.clone(), subscription)
        })
        .collect::<Vec<_>>();
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);
    for (shape, _) in &entries {
        client_transport
            .send(SyncMessage::RegisterShape {
                shape_id: shape.shape_id(),
                ast: ShapeAst::from_validated(shape),
                opts: RegisterShapeOptions::default(),
            })
            .unwrap();
    }
    let send_scope = |client: &mut Box<dyn Transport>, shape: &ValidatedQuery, subscription| {
        client
            .send(SyncMessage::AuthorizationScopeSubscribe {
                subscribe: Subscribe {
                    shape_id: shape.shape_id(),
                    subscription,
                    values: Vec::new(),
                    known_state: None,
                },
                purpose: AuthorizationScopePurpose {
                    action: action.clone(),
                },
            })
            .unwrap();
    };
    send_scope(&mut client_transport, &entries[1].0, entries[1].1);
    subscriber.borrow_mut().tick().unwrap();
    while let Some(message) = client_transport.try_recv() {
        assert!(
            !matches!(message, SyncMessage::AuthorizationScopeReceipt { .. }),
            "one update clause must never prove the full update scope"
        );
    }
    send_scope(&mut client_transport, &entries[0].0, entries[0].1);
    subscriber.borrow_mut().tick().unwrap();
    let mut saw_second_view = false;
    let mut saw_receipt = false;
    while let Some(message) = client_transport.try_recv() {
        match message {
            SyncMessage::ViewUpdate { .. } => saw_second_view = true,
            SyncMessage::AuthorizationScopeReceipt { receipt, .. } => {
                assert!(
                    saw_second_view,
                    "aggregate receipt follows final clause view"
                );
                assert_eq!(receipt.key, expected.key);
                saw_receipt = true;
            }
            SyncMessage::CatalogueSnapshot(_) => {}
            other => panic!("unexpected aggregate-scope response: {other:?}"),
        }
    }
    assert!(!saw_receipt);
}

#[test]
fn authorization_scope_aggregate_bounds_cuts_and_progress_independently() {
    let subscription = |seed| SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([seed; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([seed.wrapping_add(1); 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let mut applied = BTreeMap::new();
    applied.insert(subscription(1), (GlobalSeq(5), 100));
    applied.insert(subscription(2), (GlobalSeq(10), 1));

    assert_eq!(
        aggregate_authorization_scope_bounds(&applied),
        Some((GlobalSeq(5), 1)),
        "a later support view may be the limiting authorization generation"
    );
}

#[test]
fn authorization_scope_claims_or_policy_away_and_back_requires_fresh_every_clause() {
    let subscription = |seed| SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([seed; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([seed.wrapping_add(1); 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let first = subscription(0x21);
    let second = subscription(0x31);
    let expected_support = BTreeSet::from([
        (first.shape_id, first.binding_id),
        (second.shape_id, second.binding_id),
    ]);
    let key = AuthorizationSupportScopeKey {
        support_shape_digest: [0x41; 32],
        subject: AuthorId::from_bytes([0x42; 16]),
        claims_digest: [0x43; 32],
        policy_digest: [0x44; 32],
    };
    let mut aggregates = BTreeMap::from([(
        key.clone(),
        ScopeAggregate {
            expected_support: expected_support.clone(),
            members: BTreeMap::from([
                (first, (first.shape_id, first.binding_id)),
                (second, (second.shape_id, second.binding_id)),
            ]),
            applied: BTreeMap::from([(first, (GlobalSeq(5), 5)), (second, (GlobalSeq(5), 5))]),
        },
    )]);

    // A claims or policy transition can make both compiled clauses disappear.
    // Returning to the same digest must not revive either previous cut.
    remove_scope_aggregate_member(&mut aggregates, &key, first);
    remove_scope_aggregate_member(&mut aggregates, &key, second);
    assert!(aggregates.is_empty());

    let aggregate = aggregates.entry(key).or_insert_with(|| ScopeAggregate {
        expected_support,
        members: BTreeMap::new(),
        applied: BTreeMap::new(),
    });
    aggregate
        .members
        .insert(first, (first.shape_id, first.binding_id));
    aggregate
        .members
        .insert(second, (second.shape_id, second.binding_id));
    assert!(
        aggregate.applied.is_empty(),
        "the first returning clause has no receipt until its replacement view arrives"
    );
    aggregate.applied.insert(first, (GlobalSeq(6), 6));
    assert!(
        aggregate
            .members
            .keys()
            .any(|member| !aggregate.applied.contains_key(member)),
        "one refreshed clause still cannot prove the aggregate"
    );
    assert!(
        aggregate
            .members
            .keys()
            .any(|member| !aggregate.applied.contains_key(member))
    );
    aggregate.applied.insert(second, (GlobalSeq(6), 6));
    assert!(
        aggregate
            .members
            .keys()
            .all(|member| aggregate.applied.contains_key(member))
    );
}

#[test]
fn authorization_scope_transport_rejects_stale_component_after_applied_view() {
    let link = [0x8b; 16];
    let context = AuthorityContext {
        authority: [0x8a; 16],
        link,
        connection_id: 1,
        connection_epoch: 7,
        claims_revision: 3,
        policy_epoch: 11,
        authorization_progress: 9,
        settled_through: 17,
    };
    let key = AuthorizationSupportScopeKey {
        support_shape_digest: [1; 32],
        subject: AuthorId::from_bytes(link),
        claims_digest: [2; 32],
        policy_digest: [3; 32],
    };
    let receipt = AuthorizationScopeReceipt {
        key,
        authority: context.authority,
        link,
        authority_epoch: context.connection_epoch,
        claims_revision: context.claims_revision,
        policy_epoch: context.policy_epoch,
        settled_through: GlobalSeq(17),
        authorization_progress: 9,
    };
    assert!(authorization_scope_receipt_matches_transport_context(
        &receipt,
        context,
        Some(GlobalSeq(17)),
    ));
    assert!(
        !authorization_scope_receipt_matches_transport_context(
            &AuthorizationScopeReceipt {
                authorization_progress: 8,
                ..receipt.clone()
            },
            context,
            Some(GlobalSeq(17)),
        ),
        "a stale authorization generation must not ride a fresh support view"
    );
    assert!(
        !authorization_scope_receipt_matches_transport_context(
            &AuthorizationScopeReceipt {
                settled_through: GlobalSeq(16),
                ..receipt
            },
            context,
            Some(GlobalSeq(17)),
        ),
        "a stale support cut must not ride a fresh authorization generation"
    );
}

#[test]
fn authorization_scope_requires_canonical_current_global_support_options() {
    let expected = RegisterShapeOptions::default();
    let subscription_for = |opts: &RegisterShapeOptions| SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x51; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x52; 16])),
        read_view: opts.read_view_key(),
    };
    assert!(authorization_scope_support_options_match(
        &expected,
        &expected,
        subscription_for(&expected),
    ));
    let variants = [
        RegisterShapeOptions {
            tier: DurabilityTier::Global,
            read_view: ReadViewSpec {
                source: ReadViewSourceSpec::Branch {
                    branch: uuid::Uuid::from_bytes([0x53; 16]),
                },
                ..ReadViewSpec::default()
            },
            ..RegisterShapeOptions::default()
        },
        RegisterShapeOptions {
            tier: DurabilityTier::Global,
            read_view: ReadViewSpec {
                source: ReadViewSourceSpec::Snapshot {
                    snapshot: SnapshotRef {
                        owner: NodeUuid::from_bytes([0x54; 16]),
                        global_base: GlobalSeq(0),
                        local_base: TxTime(0),
                        dots: Vec::new(),
                    },
                },
                ..ReadViewSpec::default()
            },
            ..RegisterShapeOptions::default()
        },
        RegisterShapeOptions {
            tier: DurabilityTier::Local,
            read_view: ReadViewSpec::default(),
            ..RegisterShapeOptions::default()
        },
    ];
    for actual in variants {
        assert!(
            !authorization_scope_support_options_match(
                &expected,
                &actual,
                subscription_for(&actual),
            ),
            "noncanonical scope support must not satisfy the pure admission fence"
        );
    }
}

#[test]
fn legacy_authorization_scope_subscribe_rejects_every_read_view() {
    let schema = schema_with_explicit_public_read();
    let identity = AuthorId::from_bytes([0xc4; 16]);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let historical = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Snapshot {
                snapshot: SnapshotRef {
                    owner: NodeUuid::from_bytes([0x61; 16]),
                    global_base: GlobalSeq(0),
                    local_base: TxTime(0),
                    dots: Vec::new(),
                },
            },
            ..ReadViewSpec::default()
        },
        ..RegisterShapeOptions::default()
    };
    let variants = [
        RegisterShapeOptions {
            tier: DurabilityTier::Global,
            read_view: ReadViewSpec {
                source: ReadViewSourceSpec::Branch {
                    branch: uuid::Uuid::from_bytes([0x62; 16]),
                },
                ..ReadViewSpec::default()
            },
            ..RegisterShapeOptions::default()
        },
        historical,
        RegisterShapeOptions {
            tier: DurabilityTier::Local,
            read_view: ReadViewSpec::default(),
            ..RegisterShapeOptions::default()
        },
    ];

    // This is the matching positive control: the same shape/binding receives
    // a proof under the sole canonical current/global registration. Removing
    // both admission guards below makes the noncanonical cases fail instead.
    let canonical_opts = RegisterShapeOptions::default();
    let canonical_subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: canonical_opts.read_view_key(),
    };
    let canonical_server = open_core(0x64, AuthorId::SYSTEM, &schema);
    let (mut canonical_client, canonical_transport) = duplex();
    let canonical_subscriber = canonical_server.accept_subscriber(canonical_transport, identity);
    canonical_client
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: canonical_opts,
        })
        .unwrap();
    canonical_client
        .send(SyncMessage::AuthorizationScopeSubscribe {
            subscribe: Subscribe {
                shape_id: shape.shape_id(),
                subscription: canonical_subscription,
                values: Vec::new(),
                known_state: None,
            },
            purpose: AuthorizationScopePurpose {
                action: PermissionAdviceAction::Read {
                    table: "todos".to_owned(),
                    row: row(1),
                },
            },
        })
        .unwrap();
    canonical_subscriber.borrow_mut().tick().unwrap();
    let mut saw_view = false;
    let mut saw_receipt = false;
    while let Some(message) = canonical_client.try_recv() {
        match message {
            SyncMessage::CatalogueSnapshot(_) => {}
            SyncMessage::ViewUpdate { .. } => saw_view = true,
            SyncMessage::AuthorizationScopeReceipt { .. } => {
                assert!(saw_view, "canonical receipt follows its support view");
                saw_receipt = true;
            }
            other => panic!("unexpected canonical support response: {other:?}"),
        }
    }
    assert!(
        !saw_view && !saw_receipt,
        "the authority-owned protocol rejects client-selected support even at the canonical view"
    );

    for opts in variants {
        let server = open_core(0x63, AuthorId::SYSTEM, &schema);
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: opts.read_view_key(),
        };
        let (mut client_transport, server_transport) = duplex();
        let subscriber = server.accept_subscriber(server_transport, identity);
        client_transport
            .send(SyncMessage::RegisterShape {
                shape_id: shape.shape_id(),
                ast: ShapeAst::from_validated(&shape),
                opts,
            })
            .unwrap();
        subscriber.borrow_mut().tick().unwrap();
        while client_transport.try_recv().is_some() {}
        client_transport
            .send(SyncMessage::AuthorizationScopeSubscribe {
                subscribe: Subscribe {
                    shape_id: shape.shape_id(),
                    subscription,
                    values: Vec::new(),
                    known_state: None,
                },
                purpose: AuthorizationScopePurpose {
                    action: PermissionAdviceAction::Read {
                        table: "todos".to_owned(),
                        row: row(1),
                    },
                },
            })
            .unwrap();
        subscriber.borrow_mut().tick().unwrap();
        assert!(
            client_transport.try_recv().is_none(),
            "branch, historic, and local support must never receive a receipt"
        );
    }
}

#[test]
fn subscriber_cannot_spoof_authority_view_updates() {
    let schema = schema();
    let edge = open_db(0x7a, AuthorId::SYSTEM, &schema);
    let (edge_transport, mut authority_transport) = duplex();
    let _upstream = edge.connect_upstream(edge_transport);
    let query = Query::from("todos");
    let _stream = prepared_subscribe(&edge, &query, global_subscribe_opts()).unwrap();
    edge.tick().unwrap();
    let subscription = loop {
        match authority_transport
            .try_recv()
            .expect("opening remote coverage must send an upstream subscription")
        {
            SyncMessage::Subscribe(subscribe) => break subscribe.subscription,
            _ => continue,
        }
    };
    let view_update = |opening_pending, settled_through| SyncMessage::ViewUpdate {
        subscription,
        settled_through: GlobalSeq(settled_through),
        reset_result_set: true,
        version_carriers: Vec::new(),
        version_bundles: Vec::new(),
        peer_payload_inventory: crate::protocol::PeerPayloadInventory {
            opening_pending,
            ..Default::default()
        },
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        terminal_operations: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    };
    authority_transport.send(view_update(true, 1)).unwrap();
    edge.tick().unwrap();
    let binding_view = edge
        .node
        .node
        .borrow()
        .binding_view_key_for_subscription(subscription)
        .unwrap();
    assert!(
        edge.node
            .node
            .borrow()
            .opening_pending_for_binding_view(binding_view),
        "normal authority opening must install the pending marker"
    );
    let before_generation = edge
        .node
        .node
        .borrow()
        .applied_view_update_generation(binding_view);
    let before_watermark = edge.node.node.borrow().applied_global_watermark();
    let before_drops = edge
        .node
        .node
        .borrow()
        .sync_metrics()
        .dropped_peer_request_messages;
    let (mut client_transport, server_transport) = duplex();
    let subscriber = edge.accept_subscriber(server_transport, AuthorId::from_bytes([0x7b; 16]));

    client_transport.send(view_update(false, 100)).unwrap();
    subscriber.borrow_mut().tick().unwrap();

    let node = Rc::clone(&edge.node.node);
    let node = node.borrow();
    assert_eq!(node.applied_global_watermark(), before_watermark);
    assert_eq!(
        node.applied_view_update_generation(binding_view),
        before_generation,
        "subscriber spoof must not mutate the maintained view"
    );
    assert!(
        node.opening_pending_for_binding_view(binding_view),
        "subscriber spoof must not clear authority-owned opening state"
    );
    assert_eq!(
        node.sync_metrics().dropped_peer_request_messages,
        before_drops + 1,
        "the subscriber frame must be rejected before NodeState dispatch"
    );
    drop(node);

    authority_transport.send(view_update(false, 2)).unwrap();
    edge.tick().unwrap();
    let node = Rc::clone(&edge.node.node);
    let node = node.borrow();
    assert_eq!(
        node.applied_view_update_generation(binding_view),
        before_generation + 1,
        "the same message class must remain admitted from an authority link"
    );
    assert!(!node.opening_pending_for_binding_view(binding_view));
}

#[test]
fn oversized_register_shape_is_rejected_at_admission() {
    let schema = schema();
    let server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let huge_table = "t".repeat(MAX_SHAPE_AST_BYTES + 1);
    let ast = ShapeAst::new(Query::from(huge_table), schema.version_id());
    let error = server
        .node()
        .borrow_mut()
        .apply_sync_message(SyncMessage::RegisterShape {
            shape_id: ShapeId(uuid::Uuid::from_bytes([0x99; 16])),
            ast,
            opts: RegisterShapeOptions::default(),
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::node::Error::UnsupportedSyncMessage("shape AST exceeds byte limit")
    ));
}

#[test]
fn resume_cursor_restores_connection_claims_before_serving_same_identity_siblings() {
    let schema = membership_scoped_relation_schema();
    let reader = AuthorId::from_bytes([0xb3; 16]);
    let normal_claims = BTreeMap::new();
    let invite_claims = BTreeMap::from([
        ("user_id".to_owned(), Value::String(reader.0.to_string())),
        (
            "join_code".to_owned(),
            Value::String("resume-only-invite".to_owned()),
        ),
    ]);
    let server = open_core(0x5f, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc6, reader, &schema);
    let sibling = open_db(0xc7, reader, &schema);
    let chat = row(0xc3);
    server
        .insert_with_id(
            "chats",
            chat,
            BTreeMap::from([
                ("name".to_owned(), Value::String("secret".to_owned())),
                ("is_public".to_owned(), Value::Bool(false)),
                ("created_by".to_owned(), Value::String("author".to_owned())),
                (
                    "join_code".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String(
                        "resume-only-invite".to_owned(),
                    )))),
                ),
            ]),
        )
        .unwrap();

    let (client_transport, server_transport) = duplex();
    let upstream = client.connect_upstream(client_transport);
    let subscriber = server.accept_subscriber_with_claims(server_transport, reader, normal_claims);
    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    assert!(server.server.detach_connection(&subscriber));
    assert!(client.detach_connection(&upstream));

    // This same-identity sibling is admitted with a broader invite claim. The
    // resumed ordinary session must restore its own empty invite context, not
    // inherit the process-local compiler cache that this sibling last bound.
    let (sibling_transport, sibling_server_transport) = duplex();
    let _sibling_upstream = sibling.connect_upstream(sibling_transport);
    let _sibling_subscriber =
        server.accept_subscriber_with_claims(sibling_server_transport, reader, invite_claims);
    let (resumed_transport, resumed_server_transport) = duplex();
    let _resumed_upstream = client.connect_upstream(resumed_transport);
    let _resumed = server.accept_subscriber_with_resume(resumed_server_transport, reader, cursor);

    let query = prepared(
        &client,
        &Query::from("chats").filter(eq(col("id"), lit(chat.0))),
    );
    let attachment = client
        .attach_query_with_opts(&query, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&attachment));
    assert!(
        block_on(client.all(&query, edge_subscribe_opts()))
            .unwrap()
            .is_empty(),
        "a resumed empty-claim session must not inherit its sibling's invite claim",
    );
}

#[test]
fn subscriber_wire_claims_cannot_escalate_host_admission() {
    let schema = membership_scoped_relation_schema();
    let reader = AuthorId::from_bytes([0xb4; 16]);
    let normal_claims =
        BTreeMap::from([("user_id".to_owned(), Value::String(reader.0.to_string()))]);
    let self_asserted_invite = BTreeMap::from([
        ("user_id".to_owned(), Value::String(reader.0.to_string())),
        (
            "join_code".to_owned(),
            Value::String("self-asserted-invite".to_owned()),
        ),
    ]);
    let server = open_core(0x60, AuthorId::SYSTEM, &schema);
    let client = open_db(0xc8, reader, &schema);
    let chat = row(0xc4);
    server
        .insert_with_id(
            "chats",
            chat,
            BTreeMap::from([
                ("name".to_owned(), Value::String("secret".to_owned())),
                ("is_public".to_owned(), Value::Bool(false)),
                ("created_by".to_owned(), Value::String("author".to_owned())),
                (
                    "join_code".to_owned(),
                    Value::Nullable(Some(Box::new(Value::String(
                        "self-asserted-invite".to_owned(),
                    )))),
                ),
            ]),
        )
        .unwrap();

    let (client_transport, server_transport) = duplex();
    let _upstream = client.connect_upstream(client_transport);
    let _subscriber = server.accept_subscriber_with_claims(server_transport, reader, normal_claims);
    let dropped_before = server
        .node()
        .borrow()
        .sync_metrics()
        .dropped_peer_request_messages;

    // This is an unverified wire message from an already admitted session,
    // not an authenticated host refresh. It must not replace the admission
    // claim map even though it carries the connection's real identity.
    client.set_identity_claims(reader, self_asserted_invite);
    client.tick().unwrap();
    server.tick().unwrap();
    assert_eq!(
        server
            .node()
            .borrow()
            .sync_metrics()
            .dropped_peer_request_messages,
        dropped_before + 1,
    );

    let query = prepared(
        &client,
        &Query::from("chats").filter(eq(col("id"), lit(chat.0))),
    );
    let attachment = client
        .attach_query_with_opts(&query, edge_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert!(client.query_attachment_is_covered(&attachment));
    assert!(
        block_on(client.all(&query, edge_subscribe_opts()))
            .unwrap()
            .is_empty(),
        "a subscriber cannot grant itself an invite claim after host admission",
    );
}
