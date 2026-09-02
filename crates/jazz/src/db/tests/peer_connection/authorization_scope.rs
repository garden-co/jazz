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

/// Peer updates disclose the exact authority-approved source closure. A client
/// derives result membership locally, so protocol-facing tests inspect source
/// inputs rather than the retired authority-rendered member payload.
fn covered_input_rows(facts: &[crate::protocol::ProgramFactEntry]) -> Vec<RowUuid> {
    facts
        .iter()
        .filter_map(|fact| match fact {
            crate::protocol::ProgramFactEntry::CoveredInput(input) => Some(input.source_row),
            _ => None,
        })
        .collect()
}

// Wire inspection is required because coverage-group keys and server-stamped
// authorization generations are intentionally absent from the public API.
// Final convergence is still asserted through the receiver's public read.
fn assert_delayed_duplicate_usage_reset(replacement_row: bool) {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    let stale = row(0x61);
    server
        .insert_with_id("todos", stale, cells("live", false, owner))
        .unwrap();

    let (client_transport, server_transport, client_sent, server_sent) = duplex_with_taps();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos").filter(eq(col("title"), lit("live")));
    let prepared = prepared(&client, &query);
    let _first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        upstream.borrow_mut().tick().unwrap();
        if row_ids(&prepared_all(&client, &query, global_subscribe_opts())) == vec![stale] {
            break;
        }
    }
    assert_eq!(
        row_ids(&prepared_all(&client, &query, global_subscribe_opts())),
        vec![stale]
    );

    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(BTreeMap::from([(
            "generation".to_owned(),
            Value::U64(1),
        )]));
    subscriber.borrow_mut().tick().unwrap();
    upstream.borrow_mut().tick().unwrap();
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(BTreeMap::from([(
            "generation".to_owned(),
            Value::U64(2),
        )]));
    subscriber.borrow_mut().tick().unwrap();

    server
        .update(
            "todos",
            stale,
            BTreeMap::from([("title".to_owned(), Value::String("gone".to_owned()))]),
        )
        .unwrap();
    let fresh = replacement_row.then(|| {
        let fresh = row(0x62);
        server
            .insert_with_id("todos", fresh, cells("live", false, owner))
            .unwrap();
        fresh
    });

    // Fully drain the canonical maintained view before the duplicate attaches,
    // but hold its first-usage delivery so the client still advertises stale A.
    subscriber.borrow_mut().tick().unwrap();
    let held_first_usage_updates = server_sent.borrow_mut().drain(..).collect::<Vec<_>>();

    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let second_subscription = second_attachment.subscription();
    client.tick().unwrap();
    let second_subscribe = client_sent
        .borrow()
        .iter()
        .rev()
        .find_map(|message| match message {
            SyncMessage::Subscribe(subscribe) if subscribe.subscription == second_subscription => {
                Some(subscribe.clone())
            }
            _ => None,
        })
        .expect("second usage site must send a real Subscribe");
    let known_authorization_progress = match &second_subscribe.known_state {
        Some(KnownStateDeclaration::FastWithAuthorizationProgress {
            authorization_progress,
            ..
        }) => *authorization_progress,
        other => panic!("expected authorization-aware fast cursor, got {other:?}"),
    };
    assert_ne!(
        known_authorization_progress, 2,
        "the second usage site must exercise a stale authorization cursor"
    );
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        if server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. })
                    if *subscription == second_subscription
            )
        }) {
            break;
        }
    }
    server_sent.borrow_mut().extend(held_first_usage_updates);

    let second_update = server_sent
        .borrow()
        .iter()
        .rev()
        .find_map(|message| match message {
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription, ..
            }) if *subscription == second_subscription => Some(message.clone()),
            _ => None,
        })
        .expect("duplicate usage site must receive its own ViewUpdate");
    let SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        reset_result_set,
        peer_payload_inventory,
        program_fact_adds,
        program_fact_removes,
        ..
    }) = &second_update
    else {
        unreachable!();
    };
    assert!(*reset_result_set);
    assert_eq!(peer_payload_inventory.authorization_progress, Some(2));
    assert_eq!(
        covered_input_rows(program_fact_adds).len(),
        usize::from(replacement_row)
    );
    assert!(covered_input_rows(program_fact_removes).is_empty());
    if let Some(fresh) = fresh {
        assert_eq!(covered_input_rows(program_fact_adds), vec![fresh]);
    }

    upstream.borrow_mut().tick().unwrap();
    assert_eq!(
        row_ids(&prepared_all(&client, &query, global_subscribe_opts())),
        fresh.into_iter().collect::<Vec<_>>(),
        "authoritative duplicate fanout must replace stale client membership"
    );
}

#[test]
fn delayed_duplicate_usage_resets_stale_authorization_with_empty_canonical_set() {
    assert_delayed_duplicate_usage_reset(false);
}

#[test]
fn delayed_duplicate_usage_resets_stale_authorization_with_replacement_row() {
    assert_delayed_duplicate_usage_reset(true);
}

#[test]
fn late_detached_view_update_does_not_cover_equal_shape_reattachment() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc1, client_author, &schema);
    let (client_transport, server_transport, _client_sent, server_sent) = duplex_with_taps();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos");
    let prepared = prepared(&client, &query);

    let first = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .expect("attach first Global query usage");
    let first_subscription = first.subscription();
    client.tick().unwrap();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        if server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    peer_payload_inventory,
                    ..
                }) if *subscription == first_subscription
                    && !peer_payload_inventory.opening_pending
            )
        }) {
            break;
        }
    }
    let late_update_index = server_sent
        .borrow()
        .iter()
        .position(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    peer_payload_inventory,
                    ..
                }) if *subscription == first_subscription
                    && !peer_payload_inventory.opening_pending
            )
        })
        .expect("first usage receives a complete ViewUpdate");
    let late_first_update = server_sent
        .borrow_mut()
        .remove(late_update_index)
        .expect("held first ViewUpdate");
    upstream.borrow_mut().tick().unwrap();

    client.detach_query(first);
    client.tick().unwrap();
    let second = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .expect("reattach equal-shape Global query");
    assert_ne!(
        second.subscription(),
        first_subscription,
        "fresh authority coverage owns a distinct wire subscription"
    );
    client.tick().unwrap();

    server_sent.borrow_mut().push_back(late_first_update);
    upstream.borrow_mut().tick().unwrap();
    assert!(
        !client.query_attachment_is_covered(&second),
        "a processed late update for the detached subscription cannot cover the reattachment"
    );

    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        upstream.borrow_mut().tick().unwrap();
        if client.query_attachment_is_covered(&second) {
            break;
        }
    }
    assert!(
        client.query_attachment_is_covered(&second),
        "the reattachment is covered by its own complete ViewUpdate"
    );
    client.detach_query(second);
}

#[test]
fn legacy_authorization_scope_subscribe_is_rejected_before_shape_admission() {
    let schema = schema_with_explicit_public_read();
    let identity = AuthorSubject::for_test_bytes([0xc1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
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
                delegated_session: None,
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
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription: received,
                ..
            }) => {
                assert_eq!(received, subscription);
                received_view = true;
            }
            SyncMessage::AuthorizationScopeReceipt {
                subscription: received,
                receipt,
            } => {
                assert!(received_view, "receipt must follow its support view");
                assert_eq!(received, subscription);
                assert_eq!(receipt.link, identity);
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
    server.node().borrow_mut().set_test_provider_claims(
        identity,
        BTreeMap::from([("role".to_owned(), Value::String("editor".to_owned()))]),
    );
    subscriber.borrow_mut().tick().unwrap();
    let mut refreshed_view = false;
    let mut refreshed_receipt = None;
    while let Some(message) = client_transport.try_recv() {
        match message {
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. }) => refreshed_view = true,
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
    let identity = AuthorSubject::for_test_bytes([0xc2; 16]);
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema);
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
                delegated_session: None,
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
    let identity = AuthorSubject::for_test_bytes([0xc3; 16]);
    let server = open_core(0x60, AuthorSubject::SYSTEM, &schema);
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
                    delegated_session: None,
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
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. }) => {
                saw_second_view = true
            }
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
    applied.insert(subscription(1), (GlobalTime(5), 100));
    applied.insert(subscription(2), (GlobalTime(10), 1));

    assert_eq!(
        aggregate_authorization_scope_bounds(&applied),
        Some((GlobalTime(5), 1)),
        "a later support view may be the limiting authorization generation"
    );
}

// The server-only generation stamp and scope receipt are wire details that the
// public query API intentionally hides, so this stays a narrow internal test.
#[test]
fn sibling_scope_receipt_uses_the_view_stamped_canonical_generation() {
    let canonical_generation = PeerPayloadInventory {
        authorization_progress: Some(7),
        ..PeerPayloadInventory::default()
    };
    assert_eq!(
        authorization_progress_for_view_receipt(&canonical_generation, 0),
        7,
        "a sibling receipt must use the canonical generation already stamped on its view"
    );
    assert_eq!(
        authorization_progress_for_view_receipt(&PeerPayloadInventory::default(), 3),
        3,
        "an ordinary unstamped view keeps its usage-site generation"
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
        subject: AuthorSubject::for_test_bytes([0x42; 16]),
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
            applied: BTreeMap::from([(first, (GlobalTime(5), 5)), (second, (GlobalTime(5), 5))]),
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
    aggregate.applied.insert(first, (GlobalTime(6), 6));
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
    aggregate.applied.insert(second, (GlobalTime(6), 6));
    assert!(
        aggregate
            .members
            .keys()
            .all(|member| aggregate.applied.contains_key(member))
    );
}

#[test]
fn authorization_scope_transport_rejects_stale_component_after_applied_view() {
    let link = AuthorSubject::for_test_bytes([0x8b; 16]);
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
        subject: link,
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
        settled_through: GlobalTime(17),
        authorization_progress: 9,
    };
    assert!(authorization_scope_receipt_matches_transport_context(
        &receipt,
        context,
        Some(GlobalTime(17)),
    ));
    assert!(
        !authorization_scope_receipt_matches_transport_context(
            &AuthorizationScopeReceipt {
                authorization_progress: 8,
                ..receipt.clone()
            },
            context,
            Some(GlobalTime(17)),
        ),
        "a stale authorization generation must not ride a fresh support view"
    );
    assert!(
        !authorization_scope_receipt_matches_transport_context(
            &AuthorizationScopeReceipt {
                settled_through: GlobalTime(16),
                ..receipt
            },
            context,
            Some(GlobalTime(17)),
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
                source: ReadViewSourceSpec::Snapshot {
                    snapshot: SnapshotRef {
                        owner: NodeUuid::from_bytes([0x54; 16]),
                        global_base: GlobalTime(0),
                        local_base: TxTime(0),
                        dots: Vec::new(),
                    },
                },
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
    let identity = AuthorSubject::for_test_bytes([0xc4; 16]);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let historical = RegisterShapeOptions {
        tier: DurabilityTier::Global,
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Snapshot {
                snapshot: SnapshotRef {
                    owner: NodeUuid::from_bytes([0x61; 16]),
                    global_base: GlobalTime(0),
                    local_base: TxTime(0),
                    dots: Vec::new(),
                },
            },
        },
        ..RegisterShapeOptions::default()
    };
    let variants = [
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
    let canonical_server = open_core(0x64, AuthorSubject::SYSTEM, &schema);
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
                delegated_session: None,
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
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { .. }) => saw_view = true,
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
        let server = open_core(0x63, AuthorSubject::SYSTEM, &schema);
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
                    delegated_session: None,
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
    let edge = open_db(0x7a, AuthorSubject::SYSTEM, &schema);
    let (edge_transport, mut authority_transport) = duplex();
    let _upstream = crate::db::block_on(edge.connect_upstream(edge_transport));
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
    let view_update = |opening_pending, settled_through| {
        SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
            subscription,
            settled_through: GlobalTime(settled_through),
            reset_result_set: true,
            version_carriers: Vec::new(),
            peer_payload_inventory: crate::protocol::PeerPayloadInventory {
                opening_pending,
                ..Default::default()
            },
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        })
    };
    authority_transport.send(view_update(true, 1)).unwrap();
    edge.tick().unwrap();
    let authority_result_key = edge
        .node
        .node
        .borrow()
        .authority_result_key_for_subscription(subscription)
        .unwrap();
    assert!(
        edge.node
            .node
            .borrow()
            .opening_pending_for_authority_result(&authority_result_key),
        "normal authority opening must install the pending marker"
    );
    let before_generation = edge
        .node
        .node
        .borrow()
        .applied_authority_result_generation(&authority_result_key);
    let before_watermark = edge.node.node.borrow().committed_global_time();
    let before_drops = edge
        .node
        .node
        .borrow()
        .sync_metrics()
        .dropped_peer_request_messages;
    let (mut client_transport, server_transport) = duplex();
    let subscriber =
        edge.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x7b; 16]));

    client_transport.send(view_update(false, 100)).unwrap();
    subscriber.borrow_mut().tick().unwrap();

    let node = Rc::clone(&edge.node.node);
    let node = node.borrow();
    assert_eq!(node.committed_global_time(), before_watermark);
    assert_eq!(
        node.applied_authority_result_generation(&authority_result_key),
        before_generation,
        "subscriber spoof must not mutate the maintained view"
    );
    assert!(
        node.opening_pending_for_authority_result(&authority_result_key),
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
        node.applied_authority_result_generation(&authority_result_key),
        before_generation + 1,
        "the same message class must remain admitted from an authority link"
    );
    assert!(!node.opening_pending_for_authority_result(&authority_result_key));
}

// This stays internal because the admission ordering and retained peer registration
// state are not exposed through the public client API.
#[test]
fn oversized_register_shape_read_view_is_rejected_before_key_derivation_or_retention() {
    let schema = schema();
    let server = open_core(0x5d, AuthorSubject::SYSTEM, &schema);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let oversized_opts = RegisterShapeOptions {
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Snapshot {
                snapshot: SnapshotRef {
                    owner: NodeUuid::from_bytes([0x98; 16]),
                    global_base: GlobalTime(0),
                    local_base: TxTime(0),
                    dots: vec![
                        TxId::new(TxTime(1), NodeUuid::from_bytes([0x97; 16]));
                        MAX_SHAPE_REGISTRATION_BYTES
                    ],
                },
            },
        },
        ..RegisterShapeOptions::default()
    };
    let shape_id = shape.shape_id();
    let (mut client_transport, server_transport) = duplex();
    let subscriber =
        server.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x96; 16]));

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id,
            ast: ShapeAst::from_validated(&shape),
            opts: oversized_opts,
        })
        .unwrap();
    let error = subscriber.borrow_mut().tick().unwrap_err();

    assert_eq!(error.code, crate::db::ErrorCode::Protocol);
    assert!(error.message.contains("shape registration size"));
    assert!(
        client_transport.try_recv().is_none(),
        "an invalid oversized registration must terminate the link instead of using a new wire-level rejection convention"
    );
    let subscriber = subscriber.borrow();
    let crate::db::peer_connection::ConnectionLink::Subscriber(state) = &subscriber.link else {
        panic!("accepted subscriber must retain subscriber connection state");
    };
    assert!(
        state.shape_registrations.is_empty(),
        "oversized read-view options must not be retained"
    );
}

#[test]
fn oversized_register_shape_is_rejected_at_admission() {
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let huge_table = "t".repeat(MAX_SHAPE_REGISTRATION_BYTES + 1);
    let ast = ShapeAst::new(Query::from(huge_table), schema.version_id());
    let error = server
        .node()
        .borrow_mut()
        .apply_sync_message_settled(SyncMessage::RegisterShape {
            shape_id: ShapeId(uuid::Uuid::from_bytes([0x99; 16])),
            ast,
            opts: RegisterShapeOptions::default(),
        })
        .unwrap_err();
    assert!(matches!(
        error,
        crate::node::Error::UnsupportedSyncMessage("shape registration exceeds byte limit")
    ));
}

#[test]
fn resume_cursor_restores_connection_claims_before_serving_same_identity_siblings() {
    let schema = membership_scoped_relation_schema();
    let reader = AuthorSubject::for_test_bytes([0xb3; 16]);
    let normal_claims = BTreeMap::new();
    let invite_claims = BTreeMap::from([
        (
            "user_id".to_owned(),
            Value::String(reader.test_uuid().to_string()),
        ),
        (
            "join_code".to_owned(),
            Value::String("resume-only-invite".to_owned()),
        ),
    ]);
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema);
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
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber_with_claims(server_transport, reader, normal_claims);
    let cursor = subscriber.borrow_mut().take_resume_cursor().unwrap();
    assert!(server.server.detach_connection(&subscriber));
    assert!(client.detach_connection(&upstream));

    // This same-identity sibling is admitted with a broader invite claim. The
    // resumed ordinary session must restore its own empty invite context, not
    // inherit the process-local compiler cache that this sibling last bound.
    let (sibling_transport, sibling_server_transport) = duplex();
    let _sibling_upstream = crate::db::block_on(sibling.connect_upstream(sibling_transport));
    let _sibling_subscriber =
        server.accept_subscriber_with_claims(sibling_server_transport, reader, invite_claims);
    let (resumed_transport, resumed_server_transport) = duplex();
    let _resumed_upstream = crate::db::block_on(client.connect_upstream(resumed_transport));
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
    let reader = AuthorSubject::for_test_bytes([0xb4; 16]);
    let normal_claims = BTreeMap::from([(
        "user_id".to_owned(),
        Value::String(reader.test_uuid().to_string()),
    )]);
    let self_asserted_invite = BTreeMap::from([
        (
            "user_id".to_owned(),
            Value::String(reader.test_uuid().to_string()),
        ),
        (
            "join_code".to_owned(),
            Value::String("self-asserted-invite".to_owned()),
        ),
    ]);
    let server = open_core(0x60, AuthorSubject::SYSTEM, &schema);
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

    let (client_transport, server_transport, client_sent) = duplex_with_client_outbound_tap();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber_with_claims(server_transport, reader, normal_claims);
    let dropped_before = server
        .node()
        .borrow()
        .sync_metrics()
        .dropped_peer_request_messages;

    // An ordinary session transport no longer forwards its local provider
    // claims at all. Inject the malicious frame at the wire boundary instead:
    // it is an unverified peer assertion from an already admitted session, not
    // an authenticated host refresh, and must not replace the admission map.
    client_sent
        .borrow_mut()
        .push_back(SyncMessage::SessionClaims {
            identity: reader,
            claims: self_asserted_invite,
        });
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

#[test]
fn duplicate_usage_delivers_drained_canonical_delta_to_every_established_sibling_first() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa2; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc2; 16]);
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc2, client_author, &schema);
    let stale = row(0x63);
    server
        .insert_with_id("todos", stale, cells("live", false, owner))
        .unwrap();

    let (client_transport, server_transport, _client_sent, server_sent) = duplex_with_taps();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos").filter(eq(col("title"), lit("live")));
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let first_subscription = first_attachment.subscription();
    client.tick().unwrap();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        upstream.borrow_mut().tick().unwrap();
        if row_ids(&prepared_all(&client, &query, global_subscribe_opts())) == vec![stale] {
            break;
        }
    }

    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let second_subscription = second_attachment.subscription();
    client.tick().unwrap();
    let mut second_opening_received = false;
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        second_opening_received = server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    ..
                }) if *subscription == second_subscription
            )
        });
        upstream.borrow_mut().tick().unwrap();
        if second_opening_received {
            break;
        }
    }
    assert!(
        second_opening_received,
        "second usage must receive its own opening view before the client consumes it"
    );
    assert_ne!(first_subscription, second_subscription);
    server_sent.borrow_mut().clear();

    server
        .update(
            "todos",
            stale,
            BTreeMap::from([("title".to_owned(), Value::String("gone".to_owned()))]),
        )
        .unwrap();
    let fresh = row(0x64);
    server
        .insert_with_id("todos", fresh, cells("live", false, owner))
        .unwrap();

    let clone_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let clone_subscription = clone_attachment.subscription();
    client.tick().unwrap();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        if server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    reset_result_set: true,
                    ..
                }) if *subscription == clone_subscription
            )
        }) {
            break;
        }
    }

    let messages = server_sent.borrow();
    let sibling_updates = [first_subscription, second_subscription].map(|sibling| {
        messages
            .iter()
            .enumerate()
            .filter_map(|(index, message)| match message {
                SyncMessage::ViewUpdate(payload) if payload.subscription == sibling => {
                    Some((index, payload))
                }
                _ => None,
            })
            .collect::<Vec<_>>()
    });
    let clone_updates = messages
        .iter()
        .enumerate()
        .filter_map(|(index, message)| match message {
            SyncMessage::ViewUpdate(payload) if payload.subscription == clone_subscription => {
                Some((index, payload))
            }
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(sibling_updates[0].len(), 1);
    assert_eq!(sibling_updates[1].len(), 1);
    assert_eq!(clone_updates.len(), 1);
    let (clone_index, clone_update) = clone_updates[0];
    for updates in &sibling_updates {
        let (sibling_index, sibling_update) = updates[0];
        assert!(
            sibling_index < clone_index,
            "every canonical sibling delta must precede completion of the new clone"
        );
        assert!(!sibling_update.reset_result_set);
        assert_eq!(
            covered_input_rows(&sibling_update.program_fact_adds),
            vec![fresh]
        );
        assert_eq!(
            covered_input_rows(&sibling_update.program_fact_removes),
            vec![stale]
        );
        assert_eq!(
            sibling_update.peer_payload_inventory.authorization_progress,
            sibling_updates[0][0]
                .1
                .peer_payload_inventory
                .authorization_progress,
            "sibling fanout must stamp one canonical authorization generation"
        );
    }
    assert!(
        messages.iter().all(|message| !matches!(
            message,
            SyncMessage::AuthorizationScopeReceipt { subscription, .. }
                if [first_subscription, second_subscription, clone_subscription]
                    .contains(subscription)
        )),
        "ordinary query usages must not acquire unpaired authorization-scope receipts"
    );
    if clone_update.reset_result_set {
        assert!(covered_input_rows(&clone_update.program_fact_removes).is_empty());
    } else {
        assert_eq!(
            covered_input_rows(&clone_update.program_fact_removes),
            vec![stale]
        );
    }
    assert_eq!(
        covered_input_rows(&clone_update.program_fact_adds),
        vec![fresh]
    );
    drop(messages);

    for _ in 0..32 {
        upstream.borrow_mut().tick().unwrap();
        if row_ids(&prepared_all(&client, &query, global_subscribe_opts())) == vec![fresh] {
            break;
        }
    }
    assert_eq!(
        row_ids(&prepared_all(&client, &query, global_subscribe_opts())),
        vec![fresh],
        "both established usages and the clone must converge"
    );
}

#[test]
fn cloned_usage_reset_failure_still_publishes_canonical_delta_to_every_sibling() {
    let schema = schema();
    let owner = AuthorSubject::for_test_bytes([0xa3; 16]);
    let client_author = AuthorSubject::for_test_bytes([0xc3; 16]);
    let server = open_core(0x60, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc3, client_author, &schema);
    let stale = row(0x65);
    server
        .insert_with_id("todos", stale, cells("live", false, owner))
        .unwrap();

    let (client_transport, server_transport, client_sent, server_sent) = duplex_with_taps();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, client_author);
    let query = Query::from("todos").filter(eq(col("title"), lit("live")));
    let prepared = prepared(&client, &query);
    let first_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let first_subscription = first_attachment.subscription();
    client.tick().unwrap();
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        upstream.borrow_mut().tick().unwrap();
        if row_ids(&prepared_all(&client, &query, global_subscribe_opts())) == vec![stale] {
            break;
        }
    }
    let second_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let second_subscription = second_attachment.subscription();
    client.tick().unwrap();
    let mut second_opening_received = false;
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        second_opening_received = server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                    subscription,
                    ..
                }) if *subscription == second_subscription
            )
        });
        upstream.borrow_mut().tick().unwrap();
        if second_opening_received {
            break;
        }
    }
    assert!(
        second_opening_received,
        "second usage must receive its own opening view before the client consumes it"
    );
    server_sent.borrow_mut().clear();

    server
        .update(
            "todos",
            stale,
            BTreeMap::from([("title".to_owned(), Value::String("gone".to_owned()))]),
        )
        .unwrap();
    let fresh = row(0x66);
    server
        .insert_with_id("todos", fresh, cells("live", false, owner))
        .unwrap();

    crate::peer::fail_next_cloned_subscription_reset_for_test();
    let failed_attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let failed_subscription = failed_attachment.subscription();
    client.tick().unwrap();
    let failed_subscribe = client_sent
        .borrow()
        .iter()
        .rev()
        .find_map(|message| match message {
            SyncMessage::Subscribe(subscribe) if subscribe.subscription == failed_subscription => {
                Some(subscribe.clone())
            }
            _ => None,
        })
        .expect("the failed clone must have a concrete wire admission to retry");
    // `server_sent` is the upstream connection's live inbound queue. Keep the
    // receiver idle until the subscription-addressed ordering is inspected.
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        if server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::SubscribeRejected { subscription, .. }
                    if *subscription == failed_subscription
            )
        }) {
            break;
        }
    }

    let messages = server_sent.borrow();
    assert_eq!(
        messages
            .iter()
            .filter(|message| matches!(message, SyncMessage::SubscribeRejected { .. }))
            .count(),
        1,
        "the reset failure must reject only the new usage"
    );
    let rejection_index = messages
        .iter()
        .position(|message| {
            matches!(
                message,
                SyncMessage::SubscribeRejected {
                    subscription,
                    reason: SubscribeRejectReason::ServerFailure {
                        code: SubscribeServerFailureCode::SchemaResolution,
                    },
                } if *subscription == failed_subscription
            )
        })
        .expect("injected clone reset failure must reject only the new usage");
    let mut sibling_authorization_progress = Vec::new();
    for sibling in [first_subscription, second_subscription] {
        let updates = messages
            .iter()
            .enumerate()
            .filter_map(|(index, message)| match message {
                SyncMessage::ViewUpdate(payload) if payload.subscription == sibling => {
                    Some((index, payload))
                }
                _ => None,
            })
            .collect::<Vec<_>>();
        assert_eq!(
            updates.len(),
            1,
            "each established sibling must receive the consumed canonical transition exactly once"
        );
        let (update_index, update) = updates[0];
        assert!(update_index < rejection_index);
        assert_eq!(covered_input_rows(&update.program_fact_adds), vec![fresh]);
        assert_eq!(
            covered_input_rows(&update.program_fact_removes),
            vec![stale]
        );
        sibling_authorization_progress.push(update.peer_payload_inventory.authorization_progress);
    }
    assert_eq!(
        sibling_authorization_progress[0], sibling_authorization_progress[1],
        "failure fanout must retain one canonical authorization generation"
    );
    assert!(
        messages.iter().all(|message| !matches!(
            message,
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
                subscription,
                ..
            }) if *subscription == failed_subscription
        )),
        "the injected target reset failure must not fabricate a clone update"
    );
    assert!(
        messages.iter().all(|message| !matches!(
            message,
            SyncMessage::AuthorizationScopeReceipt { subscription, .. }
                if [first_subscription, second_subscription, failed_subscription]
                    .contains(subscription)
        )),
        "ordinary query usages must not acquire unpaired authorization-scope receipts"
    );
    drop(messages);

    // This test stays at the connection seam because the injected clone reset
    // failure is deliberately below the public client API. The rejected usage
    // must leave no served registration, policy binding, or shared-group
    // ownership behind; the two established siblings remain intact.
    {
        let connection = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!("core keeps serving the client link");
        };
        let coverage = state.served[&first_subscription].clone();
        let group = &state.coverage_groups[&coverage];
        assert!(!state.served.contains_key(&failed_subscription));
        assert_eq!(
            group.subscribers,
            BTreeSet::from([first_subscription, second_subscription])
        );
        assert_eq!(
            group.pending_initial_subscribers,
            BTreeSet::new(),
            "a rejected usage cannot remain eligible for a later initial reset"
        );
        assert!(
            state
                .peer
                .subscription_policy_binding(failed_subscription)
                .is_none(),
            "rejection must discard the usage-site authorization snapshot"
        );
    }

    let later = row(0x67);
    server
        .insert_with_id("todos", later, cells("live", false, owner))
        .unwrap();
    for _ in 0..8 {
        subscriber.borrow_mut().tick().unwrap();
    }
    assert!(
        server_sent.borrow().iter().all(|message| !matches!(
            message,
            SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. })
                if *subscription == failed_subscription
        )),
        "later canonical deltas must not resurrect a rejected usage"
    );
    // Replay the exact rejected wire handle. This is intentionally not a new
    // attachment: callers may retry after a transient server failure, and the
    // usage-site id must be cleanly reusable.
    client_sent
        .borrow_mut()
        .push_back(SyncMessage::Subscribe(failed_subscribe));
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        if server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. })
                    if *subscription == failed_subscription
            )
        }) {
            break;
        }
    }
    assert!(
        server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload { subscription, .. })
                    if *subscription == failed_subscription
            )
        }),
        "the same usage-site id must be admitted after its failed clone reset is rolled back"
    );

    for _ in 0..32 {
        upstream.borrow_mut().tick().unwrap();
        if row_ids(&prepared_all(&client, &query, global_subscribe_opts())) == vec![fresh, later] {
            break;
        }
    }
    assert_eq!(
        row_ids(&prepared_all(&client, &query, global_subscribe_opts())),
        vec![fresh, later],
        "established siblings must converge even though the new clone was rejected"
    );
}
