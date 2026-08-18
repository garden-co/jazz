//! Link admission, authority selection, permission advice, and routed fates.

use super::*;

#[test]
fn catalogue_fingerprint_change_is_eager_only_on_trusted_backend_link() {
    // This stays internal because trust is authenticated by the host at the
    // transport boundary; exposing it through a public client fixture would
    // test the HTTP/WebSocket bootstrap race rather than this hop contract.
    let mut base = schema();
    let mut core = open_core(0x5e, AuthorId::SYSTEM, &base);

    let (mut edge_transport, core_edge_transport) = duplex();
    let mut edge_link = core.accept_subscriber_with_trust(
        core_edge_transport,
        AuthorId::from_bytes([0xe1; 16]),
        CommitUnitTrust::TrustedBackend,
    );
    let (mut client_transport, core_client_transport) = duplex();
    let client_link =
        core.accept_subscriber(core_client_transport, AuthorId::from_bytes([0xc1; 16]));

    edge_link.borrow_mut().tick().unwrap();
    assert!(matches!(
        edge_transport.try_recv(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    assert!(edge_transport.try_recv().is_none());
    edge_link.borrow_mut().tick().unwrap();
    assert!(
        edge_transport.try_recv().is_none(),
        "an unchanged catalogue fingerprint must not resend its snapshot"
    );
    client_link.borrow_mut().tick().unwrap();
    assert!(
        client_transport.try_recv().is_none(),
        "ordinary sessions must not receive authority catalogue snapshots"
    );

    let mut evolved = SchemaVersion::new(JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())]));
    let mut lens = MigrationLens::new(
        base.version_id(),
        evolved.id,
        vec![TableLens {
            source_table: "todos".to_owned(),
            target_table: "todos".to_owned(),
            ops: vec![LensOp::AddColumn {
                column: "body".to_owned(),
                default: Value::String(String::new()),
            }],
        }],
    );
    core.server
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message(SyncMessage::PublishSchemaWithLens {
            author: AuthorId::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(SchemaLineagePublication::new(
                evolved.clone(),
                lens,
                Vec::<String>::new(),
                Vec::<String>::new(),
            )),
        })
        .unwrap();

    edge_link.borrow_mut().tick().unwrap();
    let Some(SyncMessage::CatalogueSnapshot(snapshot)) = edge_transport.try_recv() else {
        panic!("trusted edge must receive the changed catalogue before any subscription");
    };
    assert!(
        snapshot
            .schemas
            .iter()
            .any(|schema| schema.id == evolved.id),
        "changed snapshot carries the newly published schema"
    );
    assert!(edge_transport.try_recv().is_none());

    client_link.borrow_mut().tick().unwrap();
    assert!(
        client_transport.try_recv().is_none(),
        "catalogue changes stay authority-only on ordinary session links"
    );
}

#[test]
fn admitted_duplex_context_binds_peer_epochs_and_rejects_cross_wiring() {
    let mut identity = AuthorId::from_bytes([0x71; 16]);
    let mut schema = schema();
    let mut client = open_db(0x72, identity, &schema);
    let mut server = open_core(0x73, AuthorId::SYSTEM, &schema);
    let mut client_node = NodeUuid::from_bytes([0x72; 16]);
    let mut server_node = NodeUuid::from_bytes([0x73; 16]);
    let (client_transport, server_transport) =
        duplex_with_admitted_session_context(identity, client_node, 41, server_node, 97);
    let mut upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, identity);
    assert_eq!(upstream.borrow().connection_epoch, 41);
    assert_eq!(subscriber.borrow().connection_epoch, 97);

    let mut expected = AuthorityContext {
        authority: *server_node.as_bytes(),
        link: *identity.as_bytes(),
        connection_id: 41,
        connection_epoch: 97,
        claims_revision: 0,
        policy_epoch: 0,
        authorization_progress: 0,
        settled_through: 0,
    };
    let mut receipt = AuthorizationScopeReceipt {
        key: AuthorizationSupportScopeKey {
            support_shape_digest: [1; 32],
            subject: identity,
            claims_digest: [2; 32],
            policy_digest: [3; 32],
        },
        authority: expected.authority,
        link: expected.link,
        authority_epoch: expected.connection_epoch,
        claims_revision: 0,
        policy_epoch: 0,
        settled_through: GlobalSeq(0),
        authorization_progress: 0,
    };
    assert!(authorization_scope_receipt_matches_transport_context(
        &receipt,
        expected,
        Some(GlobalSeq(0)),
    ));
    assert!(
        !authorization_scope_receipt_matches_transport_context(
            &AuthorizationScopeReceipt {
                authority: *client_node.as_bytes(),
                authority_epoch: 41,
                ..receipt.clone()
            },
            expected,
            Some(GlobalSeq(0)),
        ),
        "a receipt from the opposite duplex endpoint must not cross-wire"
    );

    let (reconnected_client, reconnected_server) =
        duplex_with_admitted_session_context(identity, client_node, 42, server_node, 98);
    let mut reconnect = client.connect_upstream(reconnected_client);
    let mut resumed = server.accept_subscriber(reconnected_server, identity);
    assert_ne!(
        upstream.borrow().connection_epoch,
        reconnect.borrow().connection_epoch
    );
    assert_ne!(
        subscriber.borrow().connection_epoch,
        resumed.borrow().connection_epoch
    );
}

#[test]
fn permission_advice_uses_authenticated_link_identity_without_mutating() {
    let mut schema = owner_read_schema();
    let mut alice = AuthorId::from_bytes([0xa1; 16]);
    let mut mallory = AuthorId::from_bytes([0xb2; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut owned = server
        .insert("todos", cells("secret", false, alice))
        .unwrap()
        .row_uuid();

    let mut alice_client = open_db(0xa1, alice, &schema);
    let (alice_transport, alice_server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _alice_upstream = alice_client.connect_upstream(alice_transport);
    let mut _alice_subscriber = server.accept_subscriber(alice_server_transport, alice);
    let mut alice_advice = alice_client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: owned,
    });

    let mut mallory_client = open_db(0xb2, mallory, &schema);
    let (mallory_transport, mallory_server_transport) = duplex_with_admitted_session_context(
        mallory,
        NodeUuid::from_bytes([0xb2; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let mut _mallory_upstream = mallory_client.connect_upstream(mallory_transport);
    let mut _mallory_subscriber = server.accept_subscriber(mallory_server_transport, mallory);
    let mut mallory_advice =
        mallory_client.request_permission_advice(PermissionAdviceAction::Read {
            table: "todos".to_owned(),
            row: owned,
        });

    alice_client.tick().unwrap();
    mallory_client.tick().unwrap();
    server.tick().unwrap();
    alice_client.tick().unwrap();
    mallory_client.tick().unwrap();

    assert_eq!(block_on(alice_advice), PermissionAdvice::Allowed);
    assert_eq!(block_on(mallory_advice), PermissionAdvice::Denied);
    assert_eq!(server.read(&Query::from("todos")).unwrap().len(), 1);
}

#[test]
fn distinct_advice_actions_with_one_compiled_scope_hydrate_once() {
    let mut schema = owner_read_schema();
    let mut alice = AuthorId::from_bytes([0xa1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut allowed = server
        .insert("todos", cells("owned", false, alice))
        .unwrap()
        .row_uuid();
    let mut denied = server
        .insert(
            "todos",
            cells("other", false, AuthorId::from_bytes([0xb2; 16])),
        )
        .unwrap()
        .row_uuid();
    let mut client = open_db(0xa1, alice, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, alice);

    let mut first = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: allowed,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(first), PermissionAdvice::Allowed);

    let mut second = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: denied,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(second), PermissionAdvice::Denied);

    let mut hydration_count = match &subscriber.borrow().link {
        ConnectionLink::Subscriber {
            authority_scope_hydration_count,
            ..
        } => *authority_scope_hydration_count,
        ConnectionLink::Upstream { .. } => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        hydration_count, 1,
        "candidate rows must share the compiled authority support hydration"
    );
}

#[test]
fn authority_claim_revision_invalidates_cached_scope_and_rehydrates() {
    let mut schema = owner_read_schema();
    let mut alice = AuthorId::from_bytes([0xa1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut target = server
        .insert("todos", cells("owned", false, alice))
        .unwrap()
        .row_uuid();
    let mut client = open_db(0xa1, alice, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, alice);

    let mut first = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(first), PermissionAdvice::Allowed);

    server.node().borrow_mut().set_session_claims(
        alice,
        BTreeMap::from([("fresh".to_owned(), Value::Bool(true))]),
    );
    server.tick().unwrap();
    client.tick().unwrap();

    let mut refreshed = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(refreshed), PermissionAdvice::Allowed);

    server.node().borrow_mut().set_session_claims(
        alice,
        BTreeMap::from([("fresh".to_owned(), Value::Bool(false))]),
    );
    server.tick().unwrap();
    client.tick().unwrap();
    let mut advanced = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(advanced), PermissionAdvice::Allowed);

    let mut hydration_count = match &subscriber.borrow().link {
        ConnectionLink::Subscriber {
            authority_scope_hydration_count,
            ..
        } => *authority_scope_hydration_count,
        ConnectionLink::Upstream { .. } => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        hydration_count, 3,
        "each 0→1→2 authority claim transition must reject stale evidence and rehydrate"
    );
}

#[test]
fn terminal_core_write_fates_prove_exact_insert_update_and_delete_actions() {
    let mut schema = owner_write_schema();
    let mut alice = AuthorId::from_bytes([0xa1; 16]);
    let mut bob = AuthorId::from_bytes([0xb2; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    // A Core may also maintain an upstream relay; that topology fact must not
    // turn its client ingress into Edge routing or bypass local proof.
    let (core_upstream, _upstream_peer) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0x5e; 16]),
        9,
        NodeUuid::from_bytes([0xc0; 16]),
        9,
    );
    let mut _core_upstream = server.server.connect_upstream(core_upstream);
    let mut client = open_db(0xa1, alice, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut subscriber = server.accept_subscriber(server_transport, alice);

    let mut inserted = client
        .insert("todos", cells("owned", false, alice))
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    // The previous-row policy may allow Alice, but the update-check candidate
    // switches ownership to Bob and must be denied by the terminal core.
    let mut changed_owner = client
        .update(
            "todos",
            inserted.row_uuid(),
            BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.0))]),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(matches!(
        changed_owner.write_state().unwrap().fate,
        Fate::Rejected(_)
    ));

    let mut deleted = client.delete("todos", inserted.row_uuid()).unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(matches!(
        deleted.write_state().unwrap().fate,
        Fate::Accepted
    ));

    let mut proofs = match &subscriber.borrow().link {
        ConnectionLink::Subscriber { peer, .. } => peer.terminal_authority_scope_proof_count(),
        ConnectionLink::Upstream { .. } => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        proofs, 3,
        "production terminal fate admission must execute one exact aggregate proof per operation"
    );
}

#[test]
fn concurrent_upstreams_keep_selected_owner_until_detach_handoff() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let mut edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let (a_transport, _a_peer) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let mut a = edge.server.connect_upstream(a_transport);
    let mut first = *edge.server.admitted_upstream_authority.borrow();
    let (b_transport, _b_peer) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        11,
        NodeUuid::from_bytes([0xb2; 16]),
        21,
    );
    let mut _b = edge.server.connect_upstream(b_transport);
    assert_eq!(
        *edge.server.admitted_upstream_authority.borrow(),
        first,
        "a concurrent admitted upstream must not steal existing route ownership"
    );
    assert_eq!(edge.server.admitted_upstream_authorities.borrow().len(), 2);
    let mut tx_id = edge
        .node()
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x91), 1).cells(cells("handoff", false, identity)),
        )
        .unwrap();
    let mut queue = Rc::new(RefCell::new(Vec::new()));
    edge.server.edge_fate_routes.borrow_mut().insert(
        tx_id,
        vec![EdgeFateRoute {
            authority: Some(first.unwrap()),
            queue: Rc::downgrade(&queue),
        }],
    );
    assert!(edge.server.detach_connection(&a));
    assert_ne!(
        *edge.server.admitted_upstream_authority.borrow(),
        first,
        "detaching the selected owner must deterministically hand off future routes"
    );
    let mut handoff = edge.server.admitted_upstream_authority.borrow().unwrap();
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id][0].authority,
        Some(handoff),
        "an Edge-Accepted caller route must follow the selected handoff rather than vanish"
    );
}

#[test]
fn edge_route_capacity_rejects_instead_of_reporting_edge_acceptance() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let (upstream, _authority) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xe0; 16]),
        1,
        NodeUuid::from_bytes([0xc0; 16]),
        1,
    );
    let mut _upstream = edge.server.connect_upstream(upstream);
    let mut selected = edge
        .server
        .admitted_upstream_authority
        .borrow()
        .expect("admitted upstream");

    let mut client = open_db(0xa1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0xe0; 16]),
        2,
    );
    let mut _client_upstream = client.connect_upstream(client_transport);
    let mut _subscriber = edge.server.accept_edge_authority_subscriber_with_claims(
        edge_transport,
        identity,
        BTreeMap::new(),
    );
    let mut write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("bounded".to_owned()))]),
        )
        .unwrap();
    let mut queue = Rc::new(RefCell::new(Vec::new()));
    edge.server.edge_fate_routes.borrow_mut().insert(
        write.mergeable_tx_id(),
        (0..MAX_EDGE_FATE_ROUTES_PER_TX)
            .map(|_| EdgeFateRoute {
                authority: Some(selected),
                queue: Rc::downgrade(&queue),
            })
            .collect(),
    );
    client.tick().unwrap();
    edge.server.tick().unwrap();
    client.tick().unwrap();
    assert!(matches!(
        write.write_state().unwrap().fate,
        Fate::Rejected(RejectionReason::MalformedCommit(_))
    ));
}

/// An admitted Edge routes a terminal fate from its selected upstream authority
/// to exactly the downstream client that uploaded the commit.
///
/// This deliberately reaches the route registry directly because the contract
/// is below the public database API: it proves that authenticated session
/// admission binds the parked route to one authority epoch before a websocket
/// adapter or a server lifecycle can obscure the exact wire recipient.
///
/// ```text
/// alice --CommitUnit--> edge --park(tx, core epoch)--> core
/// alice <--FateUpdate-- edge <--FateUpdate------------ core
/// ```
#[test]
fn admitted_edge_session_routes_selected_authority_fate_to_uploading_client() {
    let mut schema = schema();
    let mut alice = AuthorId::from_bytes([0xa1; 16]);
    let mut edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let mut core_node = NodeUuid::from_bytes([0xc0; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);

    // The upstream endpoint is the authority that is allowed to discharge a
    // downstream Edge-accepted write. The client endpoint is deliberately a
    // different admitted session, so it cannot supply that authority context.
    let (edge_upstream_transport, core_transport) =
        duplex_with_admitted_session_context(AuthorId::SYSTEM, edge_node, 41, core_node, 97);
    let mut edge_upstream = edge.server.connect_upstream(edge_upstream_transport);
    let mut core = open_core(0xc0, AuthorId::SYSTEM, &schema);
    let mut core_session = core.accept_subscriber(core_transport, AuthorId::SYSTEM);

    let mut client = open_db(0xa1, alice, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        11,
        edge_node,
        13,
    );
    let mut _client_upstream = client.connect_upstream(client_transport);
    let mut edge_client = edge.server.accept_edge_authority_subscriber_with_claims(
        edge_transport,
        alice,
        BTreeMap::new(),
    );

    let mut write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("routed".to_owned()))]),
        )
        .unwrap();
    let mut tx_id = write.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();

    let mut expected_authority = AuthorityContext {
        authority: *core_node.as_bytes(),
        link: *AuthorId::SYSTEM.as_bytes(),
        connection_id: 41,
        connection_epoch: 97,
        claims_revision: 0,
        policy_epoch: 0,
        authorization_progress: 0,
        settled_through: 0,
    };
    let mut routes = edge.server.edge_fate_routes.borrow();
    let mut routes_for_tx = routes.get(&tx_id).expect("edge must park the upload route");
    assert_eq!(routes_for_tx.len(), 1);
    assert_eq!(routes_for_tx[0].authority, Some(expected_authority));
    drop(routes);

    // Scope receipts advance authorization metadata on the same physical
    // connection. They must not turn that admitted link into a different fate
    // authority: FateUpdate carries no receipt generation of its own.
    {
        let mut edge_upstream = edge_upstream.borrow_mut();
        let ConnectionLink::Upstream {
            expected_scope_authority,
            ..
        } = &mut edge_upstream.link
        else {
            panic!("edge upstream must retain its admitted authority context");
        };
        let mut authority_context = expected_scope_authority
            .as_mut()
            .expect("admitted authority context");
        authority_context.claims_revision = 3;
        authority_context.policy_epoch = 5;
        authority_context.authorization_progress = 7;
        authority_context.settled_through = 11;
    }

    let mut fate = SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Accepted,
        global_seq: Some(GlobalSeq(17)),
        durability: Some(DurabilityTier::Global),
    };

    // Receipt metadata is intentionally not a fate-route discriminator, but
    // every physical link discriminator still is. A FateUpdate from a
    // different epoch, local connection, authority, or admitted subject must
    // remain unable to discharge Alice's parked route.
    let mut advanced_context = {
        let mut edge_upstream = edge_upstream.borrow();
        let ConnectionLink::Upstream {
            expected_scope_authority,
            ..
        } = &edge_upstream.link
        else {
            panic!("edge upstream must retain its admitted authority context");
        };
        expected_scope_authority.expect("advanced authority context")
    };
    for physically_different in [
        AuthorityContext {
            connection_id: advanced_context.connection_id.wrapping_add(1),
            ..advanced_context
        },
        AuthorityContext {
            connection_epoch: advanced_context.connection_epoch.wrapping_add(1),
            ..advanced_context
        },
        AuthorityContext {
            authority: *NodeUuid::from_bytes([0xc2; 16]).as_bytes(),
            ..advanced_context
        },
        AuthorityContext {
            link: *AuthorId::from_bytes([0xb2; 16]).as_bytes(),
            ..advanced_context
        },
    ] {
        {
            let mut edge_upstream = edge_upstream.borrow_mut();
            let ConnectionLink::Upstream {
                expected_scope_authority,
                ..
            } = &mut edge_upstream.link
            else {
                unreachable!("edge upstream shape remains stable");
            };
            *expected_scope_authority = Some(physically_different);
        }
        core_session
            .borrow_mut()
            .transport
            .send(SyncMessage::FateUpdate {
                tx_id,
                fate: Fate::Rejected(RejectionReason::MalformedCommit(
                    "wrong physical link".to_owned(),
                )),
                global_seq: None,
                durability: None,
            })
            .unwrap();
        edge_upstream.borrow_mut().tick().unwrap();
        assert!(
            edge_client.borrow().downstream_fates.borrow().is_empty(),
            "a physically distinct authority context must not reach Alice"
        );
        assert_eq!(
            edge.node().borrow_mut().transaction_state(tx_id).unwrap().0,
            Fate::Pending,
            "a rejected fate from a different physical link must not alter edge state"
        );
    }
    {
        let mut edge_upstream = edge_upstream.borrow_mut();
        let ConnectionLink::Upstream {
            expected_scope_authority,
            ..
        } = &mut edge_upstream.link
        else {
            unreachable!("edge upstream shape remains stable");
        };
        *expected_scope_authority = Some(advanced_context);
    }
    core_session
        .borrow_mut()
        .transport
        .send(fate.clone())
        .unwrap();
    // Step only the selected upstream connection. This makes the exact
    // downstream fate observable before the client session consumes it.
    edge_upstream.borrow_mut().tick().unwrap();
    assert_eq!(
        edge_client.borrow().downstream_fates.borrow().as_slice(),
        [fate.clone()],
        "the authority's terminal fate must be queued once for Alice's session"
    );
    assert!(
        !edge.server.edge_fate_routes.borrow().contains_key(&tx_id),
        "terminal delivery must retire its exact authority route"
    );

    edge_client.borrow_mut().tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Global
    );
}

#[test]
fn stale_upstream_epoch_cannot_settle_routed_local_fate_before_selected_epoch() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let (a_transport, mut a_peer) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xe0; 16]),
        1,
        NodeUuid::from_bytes([0xa2; 16]),
        1,
    );
    let mut _a = edge.server.connect_upstream(a_transport);
    let mut selected = edge.server.admitted_upstream_authority.borrow().unwrap();
    let (b_transport, mut b_peer) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xe0; 16]),
        2,
        NodeUuid::from_bytes([0xb2; 16]),
        2,
    );
    let mut _b = edge.server.connect_upstream(b_transport);
    let mut tx_id = edge
        .node()
        .borrow_mut()
        .commit_mergeable(
            MergeableCommit::new("todos", row(0x44), 1).cells(cells("pending", false, identity)),
        )
        .unwrap();
    let mut downstream = Rc::new(RefCell::new(Vec::new()));
    edge.server.edge_fate_routes.borrow_mut().insert(
        tx_id,
        vec![EdgeFateRoute {
            authority: Some(selected),
            queue: Rc::downgrade(&downstream),
        }],
    );
    b_peer
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.server.tick().unwrap();
    assert!(matches!(
        edge.node().borrow_mut().transaction_state(tx_id).unwrap().0,
        Fate::Pending
    ));
    assert!(downstream.borrow().is_empty());
    a_peer
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.server.tick().unwrap();
    assert!(matches!(
        edge.node().borrow_mut().transaction_state(tx_id).unwrap().0,
        Fate::Accepted
    ));
    assert_eq!(downstream.borrow().len(), 1);
}

#[test]
fn edge_fate_handoff_redrives_real_downstream_write_and_ignores_old_authority() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let mut authority_a = open_core(0xa2, AuthorId::SYSTEM, &schema);
    let mut authority_b = open_core(0xb2, AuthorId::SYSTEM, &schema);
    let mut edge_node = NodeUuid::from_bytes([0xe0; 16]);

    let (edge_a_transport, a_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let mut edge_a = edge.server.connect_upstream(edge_a_transport);
    let mut a = authority_a.accept_subscriber(a_transport, identity);
    let (edge_b_transport, b_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        11,
        NodeUuid::from_bytes([0xb2; 16]),
        21,
    );
    let mut edge_b = edge.server.connect_upstream(edge_b_transport);
    let mut _b = authority_b.accept_subscriber(b_transport, identity);

    let mut client = open_db(0xc1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xc1; 16]),
        1,
        edge_node,
        2,
    );
    let mut _client_upstream = client.connect_upstream(client_transport);
    let mut edge_client = edge.server.accept_edge_authority_subscriber_with_claims(
        edge_transport,
        identity,
        BTreeMap::new(),
    );

    let mut write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("handoff".to_owned()))]),
        )
        .unwrap();
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );

    // B is a real connected authority but it is not the selected one.  Have
    // it consume the same upload and reject it while permission state is
    // unavailable; that real early fate must not settle or forward the
    // parked downstream write.
    authority_b.server.set_permissions_ready(false).unwrap();
    authority_b.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );
    {
        let mut edge_b = edge_b.borrow();
        let ConnectionLink::Upstream { uploaded, .. } = &edge_b.link else {
            panic!("B must be an upstream connection");
        };
        assert!(
            uploaded.contains(&write.mergeable_tx_id()),
            "B must have already uploaded the write before it becomes owner"
        );
    }

    assert!(edge.server.detach_connection(&edge_a));
    // The detach schedules a handoff immediately, and the successor must
    // re-upload even though it was already connected before selection.
    {
        let mut edge_b = edge_b.borrow();
        let ConnectionLink::Upstream { uploaded, .. } = &edge_b.link else {
            panic!("B must remain the upstream handoff connection");
        };
        assert!(
            !uploaded.contains(&write.mergeable_tx_id()),
            "handoff must clear B's prior upload suppression before redriving"
        );
    }
    authority_b.server.set_permissions_ready(true).unwrap();
    edge.tick().unwrap();
    authority_b.tick().unwrap();
    // Step B's actual upstream connection separately so the downstream fate
    // queue is observable before the edge-client connection flushes it.
    edge_b.borrow_mut().tick().unwrap();
    assert_eq!(
        edge_client.borrow().downstream_fates.borrow().len(),
        1,
        "B's terminal fate must enqueue exactly one downstream notification"
    );
    assert!(
        !edge
            .server
            .edge_fate_routes
            .borrow()
            .contains_key(&write.mergeable_tx_id()),
        "forwarding the terminal fate must retire its route"
    );
    edge_client.borrow_mut().tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Global
    );

    // A late packet from the detached authority has no route and cannot add a
    // second terminal notification for the original downstream handle.
    a.borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::MalformedCommit("late A".to_owned())),
            global_seq: None,
            durability: None,
        })
        .unwrap();
    edge.tick().unwrap();
    edge_client.borrow_mut().tick().unwrap();
    client.tick().unwrap();
    assert!(
        edge_client.borrow().downstream_fates.borrow().is_empty(),
        "late A must not enqueue a second downstream fate"
    );
    assert!(
        !edge
            .server
            .edge_fate_routes
            .borrow()
            .contains_key(&write.mergeable_tx_id()),
        "late A must not recreate the retired route"
    );
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Global
    );
}

#[test]
fn edge_parks_downstream_fate_until_a_later_authority_connects() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let mut authority_a = open_core(0xa2, AuthorId::SYSTEM, &schema);
    let mut edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let (edge_a_transport, a_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let mut edge_a = edge.server.connect_upstream(edge_a_transport);
    let mut _a = authority_a.accept_subscriber(a_transport, identity);

    let mut client = open_db(0xc1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xc1; 16]),
        1,
        edge_node,
        2,
    );
    let mut _client_upstream = client.connect_upstream(client_transport);
    let mut _edge_client = edge.server.accept_edge_authority_subscriber_with_claims(
        edge_transport,
        identity,
        BTreeMap::new(),
    );
    let mut write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("parked".to_owned()))]),
        )
        .unwrap();
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );

    assert!(edge.server.detach_connection(&edge_a));
    assert_eq!(edge.server.edge_fate_routes.borrow().len(), 1);
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&write.mergeable_tx_id()][0].authority,
        None,
        "a route whose authority disconnected remains parked without stale authority claims"
    );

    let mut authority_c = open_core(0xc2, AuthorId::SYSTEM, &schema);
    let (edge_c_transport, c_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        12,
        NodeUuid::from_bytes([0xc2; 16]),
        22,
    );
    let mut _edge_c = edge.server.connect_upstream(edge_c_transport);
    let mut _c = authority_c.accept_subscriber(c_transport, identity);
    edge.tick().unwrap();
    authority_c.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Global
    );
    assert!(edge.server.edge_fate_routes.borrow().is_empty());
}

/// An offline-ready Edge retains a client's fate route when a write arrives
/// before normal upstream admission.
///
/// A validated durable Edge may serve while its Core is offline. Its local
/// acceptance therefore has to retain an unbound downstream obligation, bind
/// it to the first authenticated authority, and redrive the canonical unit.
///
/// ```text
/// alice --write--> edge (no upstream yet) --later attach--> core
///                    \-- park(tx, alice) --bind(core)--> global fate
/// ```
#[test]
fn edge_write_before_upstream_admission_binds_and_redrives_fate_route() {
    let mut schema = schema();
    let mut alice = AuthorId::from_bytes([0xa1; 16]);
    let mut edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let mut core_node = NodeUuid::from_bytes([0xc0; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xa1, alice, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        11,
        edge_node,
        13,
    );
    let mut client_upstream = client.connect_upstream(client_transport);
    let mut edge_client = edge.server.accept_edge_authority_subscriber_with_claims(
        edge_transport,
        alice,
        BTreeMap::new(),
    );

    let mut write = client
        .insert("todos", cells("startup race", false, alice))
        .unwrap();
    let mut tx_id = write.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id][0].authority,
        None,
        "an offline-ready edge retains the downstream obligation without inventing authority"
    );
    client_upstream
        .borrow_mut()
        .transport
        .send(
            client
                .node
                .node
                .borrow_mut()
                .commit_unit_for(tx_id)
                .unwrap(),
        )
        .unwrap();
    edge.tick().unwrap();
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id].len(),
        1,
        "a retransmitted pre-admission unit must reuse the same downstream route"
    );

    let (edge_upstream_transport, core_transport) =
        duplex_with_admitted_session_context(AuthorId::SYSTEM, edge_node, 41, core_node, 97);
    let mut _edge_upstream = edge.server.connect_upstream(edge_upstream_transport);
    assert!(
        edge.server.edge_fate_routes.borrow()[&tx_id][0]
            .authority
            .is_some(),
        "the first authenticated authority binds the parked route"
    );
    let mut core = open_core(0xc0, AuthorId::SYSTEM, &schema);
    let mut core_session = core.accept_subscriber(core_transport, AuthorId::SYSTEM);
    edge.tick().unwrap();
    let mut uploaded = std::iter::from_fn(|| core_session.borrow_mut().transport.try_recv())
        .any(|message| matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id));
    assert!(
        uploaded,
        "binding the first authority redrives the parked unit"
    );
    core_session
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    edge_client.borrow_mut().tick().unwrap();
    client.tick().unwrap();

    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Global,
        "the late Core fate must discharge the offline client's parked route"
    );
    assert!(edge.server.edge_fate_routes.borrow().is_empty());
}

#[test]
fn stale_same_authority_session_cannot_settle_or_forward_a_routed_fate() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa1; 16]);
    let mut edge = open_core(0xe0, AuthorId::SYSTEM, &schema);
    let mut edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let mut authority_node = NodeUuid::from_bytes([0xa2; 16]);
    let mut old_authority = open_core(0xa2, AuthorId::SYSTEM, &schema);
    let mut current_authority = open_core(0xa2, AuthorId::SYSTEM, &schema);

    let (edge_old_transport, old_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 10, authority_node, 20);
    let mut _edge_old = edge.server.connect_upstream(edge_old_transport);
    let mut old = old_authority.accept_subscriber(old_transport, identity);
    let (edge_current_transport, current_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 11, authority_node, 21);
    let mut _edge_current = edge.server.connect_upstream(edge_current_transport);
    let mut current = current_authority.accept_subscriber(current_transport, identity);

    let mut client = open_db(0xc1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xc1; 16]),
        1,
        edge_node,
        2,
    );
    let mut _client_upstream = client.connect_upstream(client_transport);
    let mut _edge_client = edge.server.accept_edge_authority_subscriber_with_claims(
        edge_transport,
        identity,
        BTreeMap::new(),
    );
    let mut write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("epoch".to_owned()))]),
        )
        .unwrap();
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );

    // Model the already-admitted successor taking ownership while the old
    // same-UUID socket still has an in-flight frame.  UUID equality alone is
    // deliberately insufficient: connection id and remote epoch bind the
    // route to the current authenticated session.
    let mut current_context = edge.server.admitted_upstream_authorities.borrow()[1];
    *edge.server.admitted_upstream_authority.borrow_mut() = Some(current_context);
    edge.server
        .edge_fate_routes
        .borrow_mut()
        .get_mut(&write.mergeable_tx_id())
        .expect("routed edge write")[0]
        .authority = Some(current_context);
    old.borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::MalformedCommit("old session".to_owned())),
            global_seq: None,
            durability: None,
        })
        .unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );

    current
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Accepted,
            global_seq: Some(GlobalSeq(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(write.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Global
    );
}

#[test]
fn public_permission_advice_accepts_an_explicit_zero_clause_receipt() {
    let mut schema = schema();
    let mut identity = AuthorId::from_bytes([0xa3; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut target = server
        .insert("todos", cells("public", false, identity))
        .unwrap()
        .row_uuid();
    let mut client = open_db(0xa3, identity, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xa3; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, identity);
    let mut advice = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Allowed);
}

#[test]
fn permission_advice_is_unknown_until_authority_permissions_are_ready() {
    let mut schema = schema();
    let mut author = AuthorId::from_bytes([0xa1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    server.server.set_permissions_ready(false).unwrap();
    let mut client = open_db(0xa1, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, author);
    let mut advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Unknown);
    assert!(server.read(&Query::from("todos")).unwrap().is_empty());
}

#[test]
fn partial_replica_cannot_act_as_permission_advice_authority() {
    let mut schema = schema();
    let mut author = AuthorId::from_bytes([0xa1; 16]);
    let mut partial = open_db(0x5e, AuthorId::SYSTEM, &schema);
    let mut client = open_db(0xa1, author, &schema);
    let (client_transport, partial_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = partial.accept_subscriber(partial_transport, author);
    let mut advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });

    client.tick().unwrap();
    partial.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Unknown);
}

#[test]
fn permission_advice_update_evaluates_post_patch_update_check() {
    let mut policy = Query::from("todos").filter(eq(col("done"), lit(false)));
    let mut schema = JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("done", ColumnType::Bool),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policies(WritePolicies {
        insert_check: Policy::public(),
        update_using: Policy::public(),
        update_check: Some(policy),
        delete_using: Policy::public(),
    })]);
    let mut author = AuthorId::from_bytes([0xa1; 16]);
    let mut server = open_core(0x5e, AuthorId::SYSTEM, &schema);
    let mut target = server
        .insert("todos", cells("target", false, author))
        .unwrap()
        .row_uuid();
    let mut client = open_db(0xa1, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut _subscriber = server.accept_subscriber(server_transport, author);
    let mut advice = client.request_permission_advice(PermissionAdviceAction::Update {
        table: "todos".to_owned(),
        row: target,
        patch: BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Denied);

    let mut missing = client.request_permission_advice(PermissionAdviceAction::Update {
        table: "todos".to_owned(),
        row: row(0xee),
        patch: BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(missing), PermissionAdvice::Denied);
}

#[test]
fn permission_advice_response_wire_cannot_carry_policy_rows_or_reasons() {
    let mut request_id = PermissionAdviceRequestId([7; 16]);
    let mut message = SyncMessage::PermissionAdviceResponse {
        request_id,
        advice: PermissionAdvice::Denied,
    };
    assert_eq!(
        message,
        SyncMessage::PermissionAdviceResponse {
            request_id,
            advice: PermissionAdvice::Denied,
        }
    );
}

#[test]
fn cancelled_permission_advice_ignores_late_or_replayed_response_ids() {
    let mut schema = schema();
    let mut author = AuthorId::from_bytes([0xa1; 16]);
    let mut client = open_db(0xa1, author, &schema);
    let (client_transport, mut authority_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);

    let mut cancelled = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    client.tick().unwrap();
    let mut cancelled_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap()
    {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    drop(cancelled);

    let mut current = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(2),
    });
    client.tick().unwrap();
    let mut current_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    assert_ne!(cancelled_id, current_id);

    authority_transport
        .send(SyncMessage::AuthorizationScopeUnavailable {
            request_id: cancelled_id,
        })
        .unwrap();
    authority_transport
        .send(SyncMessage::AuthorizationScopeUnavailable {
            request_id: current_id,
        })
        .unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(current), PermissionAdvice::Unknown);
}

#[test]
fn identical_permission_advice_requests_share_one_authority_intent() {
    let mut schema = schema();
    let mut author = AuthorId::from_bytes([0xa4; 16]);
    let mut client = open_db(0xa4, author, &schema);
    let (client_transport, mut authority_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa4; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _upstream = client.connect_upstream(client_transport);
    let mut action = PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    };
    let mut first = client.request_permission_advice(action.clone());
    let mut second = client.request_permission_advice(action);
    client.tick().unwrap();

    let mut request_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected one authority scope intent, got {message:?}"),
    };
    assert!(
        try_recv_subscriber_payload(authority_transport.as_mut()).is_none(),
        "coalesced advice must not allocate a second support hydration"
    );
    authority_transport
        .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
        .unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(first), PermissionAdvice::Unknown);
    assert_eq!(block_on(second), PermissionAdvice::Unknown);
}

#[test]
fn dropped_permission_advice_is_not_sent_and_reopened_nodes_use_fresh_ids() {
    let mut schema = schema();
    let mut author = AuthorId::from_bytes([0xa1; 16]);

    let mut first = open_db(0xa1, author, &schema);
    let (first_transport, mut first_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let mut _first_upstream = first.connect_upstream(first_transport);
    let mut cancelled = first.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("sensitive", false, author),
    });
    drop(cancelled);
    first.tick().unwrap();
    assert!(try_recv_subscriber_payload(first_authority.as_mut()).is_none());

    let mut first_live = first.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    first.tick().unwrap();
    let mut first_id = match try_recv_subscriber_payload(first_authority.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    drop(first_live);

    let mut reopened = open_db(0xa1, author, &schema);
    let (reopened_transport, mut reopened_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let mut _reopened_upstream = reopened.connect_upstream(reopened_transport);
    let mut reopened_live = reopened.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    reopened.tick().unwrap();
    let mut reopened_id = match try_recv_subscriber_payload(reopened_authority.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    drop(reopened_live);

    assert_ne!(first_id, reopened_id);
}
