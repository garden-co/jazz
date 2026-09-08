//! Internal because the pilot exposes a typed reconciliation hook, not a public
//! API. Real Core/Edge node owners and authenticated transport admission run;
//! every frame crosses the production native wire codec.
use super::*;
use crate::db::row_availability::CurrentRowsResult;
use crate::protocol::{CurrentRowOutcome, CurrentRowsReceipt, PolicyBindingKey};

struct CurrentRowsWire {
    inner: Box<dyn Transport>,
    delegate: bool,
}
impl Transport for CurrentRowsWire {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        let bytes = crate::wire::encode_sync_message_for_features(
            &message,
            crate::wire::current_wire_features(),
        )
        .unwrap();
        let decoded = crate::wire::decode_sync_message_for_features(
            &bytes,
            crate::wire::current_wire_features(),
        )
        .unwrap();
        self.inner.send(decoded)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.inner.connection_session_context().map(|mut context| {
            context.negotiated_features |= crate::wire::FEATURE_CURRENT_ROW_AVAILABILITY;
            context
        })
    }
    fn permits_delegated_sessions(&self) -> bool {
        self.delegate
    }
}
fn link(
    identity: AuthorSubject,
    client: u8,
    server: u8,
    delegate: bool,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (up, down) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([client; 16]),
        1,
        NodeUuid::from_bytes([server; 16]),
        1,
    );
    (
        Box::new(CurrentRowsWire {
            inner: up,
            delegate,
        }),
        Box::new(CurrentRowsWire {
            inner: down,
            delegate: false,
        }),
    )
}
fn applied(result: CurrentRowsResult) -> CurrentRowsReceipt {
    match result {
        CurrentRowsResult::Applied(receipt) => receipt,
        CurrentRowsResult::Unknown => panic!("expected applied current-row receipt"),
    }
}

/// Alice requests a known row from Core Bob. Revocation returns no successor
/// bytes; Bob's own SYSTEM context remains readable. Alice -> Core -> Alice.
#[test]
fn core_current_rows_same_row_revocation_has_no_hidden_payload() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb1; 16]);
    let core = open_core(0xc1, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let target = row(0xd1);
    core.insert_with_id("todos", target, cells("before", false, alice))
        .unwrap();
    let client = open_db(0xe1, alice, &schema);
    let (up, down) = link(alice, 0xe1, 0xc1, false);
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let coordinate = core
        .server
        .node()
        .borrow()
        .current_row_coordinate("todos", target)
        .unwrap();
    let context = PolicyBindingKey::from_canonical_parts(alice, test_provider_claims(alice));
    let read = client
        .node
        .request_current_rows(vec![coordinate.clone()], context.clone());
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let read = applied(block_on(read));
    assert_eq!(read.outcomes, [CurrentRowOutcome::Readable]);
    assert!(!read.version_carriers.is_empty());
    core.update("todos", target, cells("hidden successor", false, bob))
        .unwrap();
    let denied = client
        .node
        .request_current_rows(vec![coordinate.clone()], context);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let denied = applied(block_on(denied));
    assert_eq!(denied.outcomes, [CurrentRowOutcome::CurrentUnavailable]);
    assert!(denied.version_carriers.is_empty());
    assert!(denied.authorization_progress > read.authorization_progress);
    assert!(denied.settled_through > read.settled_through);

    let edge = open_db(0xe2, AuthorSubject::SYSTEM, &schema);
    let (up, down) = link(AuthorSubject::SYSTEM, 0xe2, 0xc1, true);
    let _edge_up = block_on(edge.connect_upstream(up));
    let _edge_down = core.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    for _ in 0..4 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    let own = edge.node.request_current_rows(
        vec![coordinate],
        PolicyBindingKey::from_canonical_parts(
            AuthorSubject::SYSTEM,
            test_provider_claims(AuthorSubject::SYSTEM),
        ),
    );
    for _ in 0..4 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    let own = applied(block_on(own));
    assert_eq!(own.outcomes, [CurrentRowOutcome::Readable]);
    assert_eq!(own.context.identity, AuthorSubject::SYSTEM);
}

/// Alice -> Edge Bob -> Core Carol. Bob retains an old grant and can read the
/// successor as SYSTEM. Alice's delegated request still reaches Carol and gets
/// generic unavailability, with neither the successor nor hidden grant bytes.
#[test]
fn edge_proxies_current_rows_after_related_grant_revocation() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .column("owner", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_select(public_exists(
                        "grants",
                        [
                            public_outer_eq("target", "id"),
                            public_session_eq("subject", &["claims", "sub"]),
                        ],
                    ))),
            )
            .table(
                PublicTableSchemaBuilder::new("grants")
                    .fk_column("target", "todos")
                    .column("subject", PublicColumnType::Uuid)
                    .policies(PublicTablePolicies::new().with_select(PublicPolicyExpr::False)),
            ),
    );
    let alice = AuthorSubject::for_test_bytes([0xa2; 16]);
    let mallory = AuthorSubject::for_test_bytes([0xa3; 16]);
    let core = open_core(0xc2, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let target = row(0xd2);
    let grant = row(0xd3);
    core.insert_with_id("todos", target, cells("before", false, alice))
        .unwrap();
    core.insert_with_id(
        "grants",
        grant,
        BTreeMap::from([
            ("target".to_owned(), Value::Uuid(target.0)),
            ("subject".to_owned(), Value::Uuid(alice.test_uuid())),
        ]),
    )
    .unwrap();
    let edge = open_core(0xe3, AuthorSubject::SYSTEM, &schema);
    assert!(edge.server.node().borrow().is_history_complete());
    assert!(!edge.server.node().borrow().can_mint_current_row_receipts());
    let (up, down) = link(AuthorSubject::SYSTEM, 0xe3, 0xc2, true);
    let _edge_up = block_on(edge.server.connect_upstream(up));
    let _core_down = core.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let client = open_db(0xe4, alice, &schema);
    let (up, down) = link(alice, 0xe4, 0xe3, false);
    let _client_up = block_on(client.connect_upstream(up));
    let _edge_down = edge.accept_subscriber(down, alice);
    let pump = || {
        for _ in 0..8 {
            client.tick().unwrap();
            edge.tick().unwrap();
            core.tick().unwrap();
        }
    };
    pump();
    let coordinate = core
        .server
        .node()
        .borrow()
        .current_row_coordinate("todos", target)
        .unwrap();
    let grant_coordinate = core
        .server
        .node()
        .borrow()
        .current_row_coordinate("grants", grant)
        .unwrap();
    let system = PolicyBindingKey::from_canonical_parts(
        AuthorSubject::SYSTEM,
        test_provider_claims(AuthorSubject::SYSTEM),
    );
    let warm = edge
        .server
        .request_current_rows(vec![grant_coordinate], system.clone());
    pump();
    assert_eq!(
        applied(block_on(warm)).outcomes,
        [CurrentRowOutcome::Readable]
    );
    let context = PolicyBindingKey::from_canonical_parts(alice, test_provider_claims(alice));
    let initial = client
        .node
        .request_current_rows(vec![coordinate.clone()], context.clone());
    pump();
    let initial = applied(block_on(initial));
    assert_eq!(initial.outcomes, [CurrentRowOutcome::Readable]);
    assert_eq!(initial.core, NodeUuid::from_bytes([0xc2; 16]));
    assert_eq!(initial.context, context);
    for carrier in &initial.version_carriers {
        for bundle in carrier.bundle_refs().unwrap() {
            assert!(
                bundle
                    .versions
                    .iter()
                    .all(|version| version.row_uuid() == target)
            );
        }
    }
    settle(
        &core,
        vec![
            MergeableCommit::new("grants", grant, 20)
                .made_by(AuthorSubject::SYSTEM)
                .cells(BTreeMap::from([
                    ("target".to_owned(), Value::Uuid(target.0)),
                    ("subject".to_owned(), Value::Uuid(mallory.test_uuid())),
                ])),
            MergeableCommit::new("todos", target, 20)
                .made_by(AuthorSubject::SYSTEM)
                .cells(cells("forbidden successor", false, alice)),
        ],
    );
    let own = edge
        .server
        .request_current_rows(vec![coordinate.clone()], system);
    pump();
    assert_eq!(
        applied(block_on(own)).outcomes,
        [CurrentRowOutcome::Readable]
    );
    let denied = client
        .node
        .request_current_rows(vec![coordinate], context.clone());
    pump();
    let denied = applied(block_on(denied));
    assert_eq!(denied.context, context);
    assert_eq!(denied.outcomes, [CurrentRowOutcome::CurrentUnavailable]);
    assert!(denied.version_carriers.is_empty());
    assert_eq!(denied.core, NodeUuid::from_bytes([0xc2; 16]));
}

fn settle(core: &CoreDb, commits: Vec<MergeableCommit>) {
    let node = core.server.node();
    let published = block_on(node.borrow_mut().commit_mergeable_many(commits)).unwrap();
    let tx = block_on(node.borrow_mut().persist_and_settle_transaction(published)).unwrap();
    let outcome = block_on(node.borrow_mut().finalize_local_mergeable_commit(tx)).unwrap();
    block_on(node.borrow_mut().persist_and_settle_outcome(outcome)).unwrap();
    core.server.mark_subscriber_connections_dirty();
}

/// Alice can read Bob's deleted row preimage. Core sends the ordinary deletion
/// witness with the readable content, preserving includeDeleted semantics.
#[test]
fn current_rows_readable_tombstone_is_not_generic_unavailable() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
    let core = open_core(0xc4, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let target = row(0xd4);
    core.insert_with_id("todos", target, cells("readable preimage", false, alice))
        .unwrap();
    settle(
        &core,
        vec![
            MergeableCommit::new("todos", target, 20)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Deleted),
        ],
    );
    let client = open_db(0xe5, alice, &schema);
    let (up, down) = link(alice, 0xe5, 0xc4, false);
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let coordinate = core
        .server
        .node()
        .borrow()
        .current_row_coordinate("todos", target)
        .unwrap();
    let result = client.node.request_current_rows(
        vec![coordinate],
        PolicyBindingKey::from_canonical_parts(alice, test_provider_claims(alice)),
    );
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let receipt = applied(block_on(result));
    assert_eq!(receipt.outcomes, [CurrentRowOutcome::Readable]);
    let versions = crate::protocol::expand_version_carriers(&receipt.version_carriers)
        .unwrap()
        .into_iter()
        .flat_map(|bundle| bundle.versions)
        .collect::<Vec<_>>();
    assert!(versions.iter().any(|version| version.deletion().is_none()));
    assert!(
        versions
            .iter()
            .any(|version| version.deletion() == Some(crate::tx::DeletionEvent::Deleted))
    );
}

/// Alice's known coordinate and generic result use the named postcard semantic
/// codec. Bob cannot decode these appended variants without negotiated support.
#[test]
fn current_rows_wire_v1_corpus_and_feature_gate() {
    let request = crate::protocol::CurrentRowsRequest {
        request_id: PermissionAdviceRequestId([1; 16]),
        rows: vec![crate::protocol::CurrentRowCoordinate {
            schema: SchemaVersionId::from_bytes([2; 16]),
            table: "rows".into(),
            physical_table: crate::ids::GlobalPhysicalTableId(uuid::Uuid::from_bytes([3; 16])),
            row: row(4),
        }],
        delegated_session: None,
    };
    let receipt = crate::db::row_availability::unknown_receipt(
        &request,
        PolicyBindingKey::from_canonical_parts(AuthorSubject::SYSTEM, BTreeMap::new()),
    );
    for (name, message) in [
        ("request", SyncMessage::CurrentRowsRequest(request.clone())),
        ("receipt", SyncMessage::CurrentRowsReceipt(receipt)),
        (
            "cancel",
            SyncMessage::CurrentRowsCancel {
                request_id: request.request_id,
            },
        ),
    ] {
        let bytes = crate::wire::encode_sync_message(&message).unwrap();
        let expected = match name {
            "request" => {
                "1f0101010101010101010101010101010101100202020202020202020202020202020204726f77731003030303030303030303030303030303100404040404040404040404040404040400"
            }
            "receipt" => {
                "200101010101010101010101010101010101100202020202020202020202020202020204726f77731003030303030303030303030303030303100404040404040404040404040404040401021c5b2275726e3a6a617a7a3a73797374656d222c2273797374656d225d001000000000000000000000000000000000000000000000"
            }
            "cancel" => "2101010101010101010101010101010101",
            _ => unreachable!(),
        };
        assert_eq!(hex::encode(&bytes), expected);
        assert_eq!(
            crate::wire::decode_sync_message_for_features(
                &bytes,
                crate::wire::current_wire_features()
            )
            .unwrap(),
            message
        );
        assert!(
            crate::wire::decode_sync_message_for_features(
                &bytes,
                crate::wire::current_wire_features()
                    & !crate::wire::FEATURE_CURRENT_ROW_AVAILABILITY
            )
            .is_err()
        );
    }
}

/// Alice cannot consume a Bob-scoped or partial receipt, and dropping Alice's
/// future cancels its exact live nonce. Internal nonce-state assertion is needed
/// because this pilot deliberately has no public unavailable-source side effect.
#[test]
fn current_rows_reject_wrong_context_partial_receipt_and_cancel() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa5; 16]);
    let client = open_db(0xe6, alice, &schema);
    let (up, _down) = link(alice, 0xe6, 0xc6, false);
    let connection = block_on(client.connect_upstream(up));
    let coordinate = client
        .node
        .node()
        .borrow()
        .current_row_coordinate("todos", row(0xd6))
        .unwrap();
    let context = PolicyBindingKey::from_canonical_parts(alice, test_provider_claims(alice));
    let future = client.node.request_current_rows(vec![coordinate], context);
    client.tick().unwrap();
    let router = &client.node.current_rows;
    let (id, request, context, expected) = {
        let router = router.borrow();
        let (id, route) = router.routes.iter().next().unwrap();
        (
            *id,
            route.request.clone(),
            route.context.clone(),
            route.upstream.unwrap(),
        )
    };
    let mut wrong = crate::db::row_availability::unknown_receipt(&request, context.clone());
    wrong.context.identity = AuthorSubject::SYSTEM;
    block_on(crate::db::row_availability::receive_current_rows(
        &client.node.node(),
        router,
        Some(expected),
        Some(expected),
        true,
        wrong,
    ))
    .unwrap();
    assert!(router.borrow().routes.contains_key(&id));
    let mut partial = crate::db::row_availability::unknown_receipt(&request, context);
    partial.outcomes.clear();
    block_on(crate::db::row_availability::receive_current_rows(
        &client.node.node(),
        router,
        Some(expected),
        Some(expected),
        true,
        partial,
    ))
    .unwrap();
    assert!(router.borrow().routes.contains_key(&id));
    drop(future);
    assert!(!router.borrow().routes.contains_key(&id));
    assert_eq!(router.borrow().cancels.len(), 1);
    client.tick().unwrap();
    assert!(router.borrow().cancels.is_empty());
    drop(connection);
}

/// Alice's old peer Bob has not negotiated the new feature. The pending request
/// resolves Unknown without emitting an availability message or a denial.
#[test]
fn current_rows_unsupported_peer_is_unknown() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa6; 16]);
    let client = open_db(0xe7, alice, &schema);
    let (up, _down) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xe7; 16]),
        1,
        NodeUuid::from_bytes([0xc7; 16]),
        1,
    );
    let _connection = block_on(client.connect_upstream(up));
    let coordinate = client
        .node
        .node()
        .borrow()
        .current_row_coordinate("todos", row(0xd7))
        .unwrap();
    let future = client.node.request_current_rows(
        vec![coordinate],
        PolicyBindingKey::from_canonical_parts(alice, test_provider_claims(alice)),
    );
    client.tick().unwrap();
    assert!(matches!(block_on(future), CurrentRowsResult::Unknown));
    assert!(client.node.current_rows.borrow().routes.is_empty());
}

/// Bob's trusted Edge revalidates more than 64 sequential immutable contexts at
/// Core Carol. Completed nonce floors retire safely instead of permanently
/// exhausting the router's bounded evidence cache.
#[test]
fn current_rows_more_than_64_sequential_contexts_remain_functional() {
    let schema = owner_read_schema();
    let core = open_core(0xc8, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let target = row(0xd8);
    core.insert_with_id(
        "todos",
        target,
        cells(
            "public to system",
            false,
            AuthorSubject::for_test_bytes([0xa8; 16]),
        ),
    )
    .unwrap();
    let edge = open_db(0xe8, AuthorSubject::SYSTEM, &schema);
    let (up, down) = link(AuthorSubject::SYSTEM, 0xe8, 0xc8, true);
    let _up = block_on(edge.connect_upstream(up));
    let _down = core.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    for _ in 0..4 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    let coordinate = core
        .server
        .node()
        .borrow()
        .current_row_coordinate("todos", target)
        .unwrap();
    for index in 0..130 {
        let context = PolicyBindingKey::from_canonical_parts(
            AuthorSubject::SYSTEM,
            BTreeMap::from([("probe".to_owned(), Value::U64(index))]),
        );
        let future = edge
            .node
            .request_current_rows(vec![coordinate.clone()], context.clone());
        for _ in 0..4 {
            edge.tick().unwrap();
            core.tick().unwrap();
        }
        let receipt = applied(block_on(future));
        assert_eq!(receipt.context, context);
        assert_eq!(receipt.outcomes, [CurrentRowOutcome::Readable]);
        assert!(edge.node.current_rows.borrow().floors.len() <= 64);
    }
}

/// Alice receives Bob's authorized tombstone and preimage. Removing the content
/// carrier must make the receipt invalid before Readable can clear a durable
/// unavailable marker. Internal ingestion is used to isolate receipt validation.
/// Bob -> complete receipt -> Alice; deletion-only replay -> rejected.
#[test]
fn current_rows_reject_deletion_only_readable_receipt() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa4; 16]);
    let core = open_core(0xc4, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let target = row(0xd4);
    core.insert_with_id("todos", target, cells("readable preimage", false, alice))
        .unwrap();
    settle(
        &core,
        vec![
            MergeableCommit::new("todos", target, 20)
                .made_by(AuthorSubject::SYSTEM)
                .deletion(crate::tx::DeletionEvent::Deleted),
        ],
    );
    let client = open_db(0xe5, alice, &schema);
    let (up, down) = link(alice, 0xe5, 0xc4, false);
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let coordinate = core
        .server
        .node()
        .borrow()
        .current_row_coordinate("todos", target)
        .unwrap();
    let result = client.node.request_current_rows(
        vec![coordinate],
        PolicyBindingKey::from_canonical_parts(alice, test_provider_claims(alice)),
    );
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let receipt = applied(block_on(result));
    assert_eq!(receipt.outcomes, [CurrentRowOutcome::Readable]);
    let versions = crate::protocol::expand_version_carriers(&receipt.version_carriers)
        .unwrap()
        .into_iter()
        .flat_map(|bundle| bundle.versions)
        .collect::<Vec<_>>();
    assert!(versions.iter().any(|version| version.deletion().is_none()));
    assert!(
        versions
            .iter()
            .any(|version| version.deletion() == Some(crate::tx::DeletionEvent::Deleted))
    );
    let mut partial = receipt.clone();
    partial.version_carriers.retain(|carrier| {
        carrier.bundle_refs().unwrap().iter().all(|bundle| {
            bundle
                .versions
                .iter()
                .all(|version| version.deletion().is_some())
        })
    });
    assert!(!partial.version_carriers.is_empty());
    let result = block_on(
        client
            .node
            .node()
            .borrow_mut()
            .ingest_current_rows_receipt(&partial),
    );
    assert!(
        result.is_err(),
        "Readable must carry a content witness, not only deletion"
    );
}
