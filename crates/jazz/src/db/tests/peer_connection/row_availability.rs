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
        self.inner.connection_session_context()
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
/// codec. All three messages are mandatory even with no optional features.
#[test]
fn current_rows_wire_v1_corpus_is_mandatory() {
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
        assert_eq!(message.required_wire_features(), crate::wire::FEATURE_NONE);
        assert_eq!(
            crate::wire::encode_sync_message_for_features(&message, crate::wire::FEATURE_NONE)
                .unwrap(),
            bytes
        );
        assert_eq!(
            crate::wire::decode_sync_message_for_features(&bytes, crate::wire::FEATURE_NONE)
                .unwrap(),
            message
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

/// Alice repairs an exact older body through a partial Edge. Core authorizes
/// the row first; a later Core-only owner change blocks Edge's stale grant.
/// Alice -> Edge cached body -> Core current-read receipt -> exact repair.
#[test]
fn partial_edge_repairs_require_current_core_readability_and_keep_fifo() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0x81; 16]);
    let bob = AuthorSubject::for_test_bytes([0x82; 16]);
    let core = open_core(0x83, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let target = row(0x84);
    let write = core
        .insert_with_id(
            "todos",
            target,
            cells("original authorized body", false, alice),
        )
        .unwrap();
    let request = crate::protocol::RowVersionRef::new("todos", target, write.mergeable_tx_id());
    let edge = open_core(0x85, AuthorSubject::SYSTEM, &schema);
    let bundles = core
        .server
        .node()
        .borrow_mut()
        .row_version_payloads_for_refs(
            &[request.clone()],
            crate::node::RowVersionRepairAuthorization::EnforceReadPolicy(AuthorSubject::SYSTEM),
        )
        .unwrap();
    edge.server
        .node()
        .borrow_mut()
        .apply_row_version_payloads_for_requests(&[request.clone()], bundles)
        .unwrap();
    let (up, down) = link(AuthorSubject::SYSTEM, 0x85, 0x83, true);
    let _upstream = block_on(edge.server.connect_upstream(up));
    let _core_down = core.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (mut client, down) = duplex();
    let subscriber = edge.accept_subscriber(down, alice);
    subscriber.borrow_mut().set_partial_edge_query_host();
    for _ in 0..4 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    client
        .send(SyncMessage::FetchRowVersions {
            requests: vec![request.clone()],
            delegated_session: None,
        })
        .unwrap();
    for _ in 0..8 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    let mut responses = Vec::new();
    while let Some(message) = client.try_recv() {
        if let SyncMessage::RowVersionPayloads { version_bundles } = message {
            responses.push(version_bundles);
        }
    }
    assert_eq!(responses.len(), 1);
    assert_eq!(
        responses[0].iter().map(|b| b.versions.len()).sum::<usize>(),
        1
    );
    assert_eq!(responses[0][0].tx.tx_id, request.tx_id());
    core.update("todos", target, cells("hidden newer body", false, bob))
        .unwrap();
    client
        .send(SyncMessage::FetchRowVersions {
            requests: vec![request.clone()],
            delegated_session: None,
        })
        .unwrap();
    for _ in 0..8 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    let mut denied = Vec::new();
    while let Some(message) = client.try_recv() {
        if let SyncMessage::RowVersionPayloads { version_bundles } = message {
            denied.push(version_bundles);
        }
    }
    assert_eq!(denied.len(), 1);
    assert!(denied[0].is_empty());
    assert_eq!(
        edge.server
            .node()
            .borrow_mut()
            .row_version_payloads_for_refs(
                &[request.clone()],
                crate::node::RowVersionRepairAuthorization::EnforceReadPolicy(alice)
            )
            .unwrap()
            .len(),
        1,
        "Edge still has the stale local grant"
    );

    let (mut delegated, down) = duplex();
    let privileged = edge.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedAuthority,
    );
    privileged.borrow_mut().admit_authority_query_delegate();
    privileged.borrow_mut().set_partial_edge_query_host();
    delegated
        .send(SyncMessage::FetchRowVersions {
            requests: vec![request.clone()],
            delegated_session: Some(crate::protocol::DelegatedSessionBinding {
                identity: alice,
                claims: test_provider_claims(alice),
            }),
        })
        .unwrap();
    delegated
        .send(SyncMessage::FetchRowVersions {
            requests: vec![request],
            delegated_session: None,
        })
        .unwrap();
    for _ in 0..12 {
        edge.tick().unwrap();
        core.tick().unwrap();
    }
    let mut ordered = Vec::new();
    while let Some(message) = delegated.try_recv() {
        if let SyncMessage::RowVersionPayloads { version_bundles } = message {
            ordered.push(version_bundles);
        }
    }
    assert_eq!(ordered.len(), 2);
    assert!(
        ordered[0].is_empty(),
        "delegated denial must precede SYSTEM repair"
    );
    assert!(!ordered[1].is_empty(), "trusted SYSTEM keeps cache access");
}

/// Unknown Core evidence stays pending. Removing either physical link cancels
/// its active nonce; cached bytes never become a fallback authorization source.
#[test]
fn partial_edge_pending_repair_cancels_on_link_loss_without_cache_fallback() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0x86; 16]);
    let edge = open_core(0x87, AuthorSubject::SYSTEM, &schema);
    let target = row(0x88);
    let write = edge
        .insert_with_id(
            "todos",
            target,
            cells("unverified repair cache", false, alice),
        )
        .unwrap();
    let request = crate::protocol::RowVersionRef::new("todos", target, write.mergeable_tx_id());
    let (up, _undriven_core) = link(AuthorSubject::SYSTEM, 0x87, 0x89, true);
    let upstream = block_on(edge.server.connect_upstream(up));
    let (mut client, down) = duplex();
    let subscriber = edge.accept_subscriber(down, alice);
    subscriber.borrow_mut().set_partial_edge_query_host();
    client
        .send(SyncMessage::FetchRowVersions {
            requests: vec![request.clone()],
            delegated_session: None,
        })
        .unwrap();
    for _ in 0..4 {
        edge.tick().unwrap();
    }
    assert_eq!(edge.server.current_rows.borrow().routes.len(), 1);
    assert!(edge.server.detach_connection(&subscriber));
    assert!(edge.server.current_rows.borrow().routes.is_empty());
    let (mut client, down) = duplex();
    let subscriber = edge.accept_subscriber(down, alice);
    subscriber.borrow_mut().set_partial_edge_query_host();
    client
        .send(SyncMessage::FetchRowVersions {
            requests: vec![request],
            delegated_session: None,
        })
        .unwrap();
    for _ in 0..4 {
        edge.tick().unwrap();
    }
    assert_eq!(edge.server.current_rows.borrow().routes.len(), 1);
    assert!(edge.server.detach_connection(&upstream));
    for _ in 0..4 {
        edge.tick().unwrap();
    }
    assert!(edge.server.current_rows.borrow().routes.is_empty());
    while let Some(message) = client.try_recv() {
        assert!(!matches!(message, SyncMessage::RowVersionPayloads { .. }));
    }
    assert!(edge.server.detach_connection(&subscriber));
    let connection = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!()
    };
    assert!(state.pending_authority_repairs.is_empty());
}

/// The Edge's own SYSTEM local-first query repairs its retained cache after
/// reconnect. No downstream client or client-context exclusion participates.
/// This uses admitted links and the native codec, not a WebSocket server.
#[test]
fn edge_own_system_scalar_query_reconciles_after_reconnect() {
    let schema = owner_read_schema();
    let core = open_core(0xc4, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let edge = open_db(0xe4, AuthorSubject::SYSTEM, &schema);
    let target = row(0xd4);
    core.insert_with_id(
        "todos",
        target,
        cells("before", false, AuthorSubject::for_test_bytes([0xa4; 16])),
    )
    .unwrap();
    let (up, down) = link(AuthorSubject::SYSTEM, 0xe4, 0xc4, true);
    let upstream = block_on(edge.connect_upstream(up));
    let _downstream = core.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let query = Query::from("todos").filter(eq(col("done"), lit(false)));
    let mut stream = prepared_subscribe(&edge, &query, ReadOpts::default()).unwrap();
    let mut snapshot = RelationSnapshot::default();
    for _ in 0..32 {
        core.tick().unwrap();
        edge.tick().unwrap();
        while let Some(event) = stream.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
    }
    assert_eq!(snapshot.root_count, 1);
    assert!(block_on(edge.detach_connection_async(&upstream)).unwrap());
    core.update(
        "todos",
        target,
        cells(
            "after reconnect",
            true,
            AuthorSubject::for_test_bytes([0xa4; 16]),
        ),
    )
    .unwrap();
    assert_eq!(snapshot.root_count, 1);
    let (up, down) = link(AuthorSubject::SYSTEM, 0xe4, 0xc4, true);
    let _upstream = block_on(edge.connect_upstream(up));
    let _downstream = core.accept_subscriber_with_trust(
        down,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    for _ in 0..64 {
        core.tick().unwrap();
        edge.tick().unwrap();
        while let Some(event) = stream.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
    }
    assert_eq!(
        snapshot.root_count, 0,
        "Edge own query removes stale scalar match"
    );
    let cached = prepared_all(
        &edge,
        &Query::from("todos"),
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    );
    assert_eq!(
        cached.len(),
        1,
        "SYSTEM cache remains readable, not revoked"
    );
    assert_eq!(cached[0].row_uuid(), target);
    assert_eq!(
        cached[0].cell(&schema.tables[0], "title"),
        Some(Value::String("after reconnect".into()))
    );
}

struct HoldCurrentRowsReceipts {
    inner: Box<dyn Transport>,
    hold: Rc<Cell<bool>>,
    withheld: Rc<RefCell<VecDeque<SyncMessage>>>,
}
impl Transport for HoldCurrentRowsReceipts {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        if !self.hold.get() {
            if let Some(message) = self.withheld.borrow_mut().pop_front() {
                return Some(message);
            }
        }
        let message = self.inner.try_recv()?;
        if self.hold.get() && matches!(message, SyncMessage::CurrentRowsReceipt(_)) {
            self.withheld.borrow_mut().push_back(message);
            None
        } else {
            Some(message)
        }
    }
    fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
        self.inner.connection_session_context()
    }
    fn permits_delegated_sessions(&self) -> bool {
        self.inner.permits_delegated_sessions()
    }
}

/// Internal transport scheduling is needed to pause precisely after Core has
/// evaluated the settled row but before its receipt reaches a new local edit.
#[test]
fn scalar_unavailability_receipt_preserves_inflight_local_edit() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("owner", &["claims", "sub"]))
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True),
                ),
        ),
    );
    let alice = AuthorSubject::for_test_bytes([0xa5; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb5; 16]);
    let core = open_core(0xc5, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let client = open_db(0xe5, alice, &schema);
    client
        .node
        .node
        .borrow_mut()
        .set_session_claims(alice, test_provider_claims(alice));
    let target = row(0xd5);
    core.insert_with_id("todos", target, cells("before", false, alice))
        .unwrap();
    let (up, down) = link(alice, 0xe5, 0xc5, false);
    let hold = Rc::new(Cell::new(true));
    let withheld = Rc::new(RefCell::new(VecDeque::new()));
    let _up = block_on(client.connect_upstream(Box::new(HoldCurrentRowsReceipts {
        inner: up,
        hold: Rc::clone(&hold),
        withheld: Rc::clone(&withheld),
    })));
    let _down = core.accept_subscriber(down, alice);
    let mut stream = prepared_subscribe(
        &client,
        &Query::from("todos").filter(eq(col("done"), lit(false))),
        ReadOpts::default(),
    )
    .unwrap();
    let mut snapshot = RelationSnapshot::default();
    for _ in 0..32 {
        core.tick().unwrap();
        client.tick().unwrap();
        while let Some(event) = stream.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
    }
    assert_eq!(snapshot.root_count, 1);
    core.update("todos", target, cells("not readable", false, bob))
        .unwrap();
    for _ in 0..32 {
        core.tick().unwrap();
        client.tick().unwrap();
        if !withheld.borrow().is_empty() {
            break;
        }
    }
    assert!(withheld.borrow().iter().any(|message| matches!(message,
        SyncMessage::CurrentRowsReceipt(receipt) if receipt.outcomes == [CurrentRowOutcome::CurrentUnavailable])));
    let write = block_on(client.update(
        "todos",
        target,
        BTreeMap::from([("title".into(), Value::String("local edit".into()))]),
        Default::default(),
    ))
    .unwrap();
    hold.set(false);
    // Core remains paused: the new optimistic write has no remote fate yet.
    for _ in 0..8 {
        client.tick().unwrap();
    }
    assert!(
        !client.node.current_rows.borrow().floors.is_empty(),
        "held receipt passed exact context and local-cut validation"
    );
    {
        let node = client.node.node.borrow();
        let scope = node.local_read_policy_binding(alice).unwrap();
        let table = node
            .local_availability_table_id(schema.version_id(), "todos")
            .unwrap();
        assert!(
            node.is_local_row_unavailable(&scope, table, target),
            "settled denial applies immediately even while the edit is pending"
        );
    }
    let rows = prepared_all(
        &client,
        &Query::from("todos"),
        ReadOpts {
            propagation: Propagation::LocalOnly,
            ..ReadOpts::default()
        },
    );
    assert_eq!(
        rows.len(),
        1,
        "settled-row denial must not hide the pending edit"
    );
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String("local edit".into()))
    );
    assert!(matches!(
        block_on(write.write_state()).unwrap().fate,
        Fate::Pending
    ));
}
