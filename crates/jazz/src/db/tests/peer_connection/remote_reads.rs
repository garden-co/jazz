//! The test uses the public serialized-read API. The authenticated feature
//! context is installed internally because it is a transport admission fact.
use super::*;

struct RemoteReadWire {
    inner: Box<dyn Transport>,
}

impl Transport for RemoteReadWire {
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
            context.negotiated_features |= crate::wire::FEATURE_REMOTE_READ_RESULTS;
            context
        })
    }
}

fn link(
    identity: AuthorSubject,
    client: u8,
    server: u8,
) -> (Box<dyn Transport>, Box<dyn Transport>) {
    let (up, down) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([client; 16]),
        1,
        NodeUuid::from_bytes([server; 16]),
        1,
    );
    (
        Box::new(RemoteReadWire { inner: up }),
        Box::new(RemoteReadWire { inner: down }),
    )
}

// The serialized result variant proves that an unbounded read used the
// authority route; the public decoded rows alone cannot distinguish it from
// the slower receiver coverage path.
#[test]
fn unbounded_global_read_proxies_authorized_rows_through_relay() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xab; 16]);
    let bob = AuthorSubject::for_test_bytes([0xbb; 16]);
    let core = open_core(0xcb, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let visible_a = row(0xdb);
    let visible_b = row(0xdc);
    core.insert_with_id("todos", visible_a, cells("first", false, alice))
        .unwrap();
    core.insert_with_id("todos", visible_b, cells("second", false, alice))
        .unwrap();
    core.insert_with_id("todos", row(0xdd), cells("hidden", false, bob))
        .unwrap();

    let query = Query::from("todos");
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = {
        let owner = core.node();
        let mut owner = owner.borrow_mut();
        let mut scoped = owner.scoped_active_session_claims(alice, test_provider_claims(alice));
        let rows = block_on(scoped.query_rows_with_prepared_plan_for_identity(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            alice,
        ))
        .unwrap();
        assert_eq!(row_ids(&rows), vec![visible_a, visible_b]);
        crate::binding_codec::encode_rows(&rows).unwrap()
    };

    let relay = open_db(0xeb, alice, &schema);
    relay.set_relay_authority_session_owner_for_test();
    let client = open_db(0xfb, alice, &schema);
    client
        .node
        .node
        .borrow_mut()
        .set_session_claims(alice, test_provider_claims(alice));
    let (relay_up, core_down) = link(alice, 0xeb, 0xcb);
    let _relay_up = block_on(relay.connect_upstream(relay_up));
    let _core_down = core.accept_scope_isolated_relay_subscriber(
        core_down,
        alice,
        test_provider_claims(alice),
        1,
    );
    let (client_up, relay_down) = link(alice, 0xfb, 0xeb);
    let _client_up = block_on(client.connect_upstream(client_up));
    let _relay_down =
        relay.accept_subscriber_with_claims(relay_down, alice, test_provider_claims(alice));
    for _ in 0..6 {
        client.tick().unwrap();
        relay.tick().unwrap();
        core.tick().unwrap();
    }

    let bytes = postcard::to_allocvec(&query).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            result_only: true,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    let result = (0..32)
        .find_map(|_| {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            relay.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("unbounded authority read settled");
    let SerializedReadResult::EncodedRows(actual) = result else {
        panic!("expected the authority result without receiver coverage");
    };
    assert_eq!(actual, expected);
    assert_eq!(client.query_coverage_attachment_counts_for_test(), (0, 0));
}

#[test]
fn bounded_global_read_returns_only_authorized_rows_without_receiver_coverage() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa6; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb6; 16]);
    let core = open_core(0xc6, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let visible = row(0xd6);
    let hidden = row(0xd7);
    core.insert_with_id("todos", visible, cells("visible", false, alice))
        .unwrap();
    core.insert_with_id("todos", hidden, cells("hidden", false, bob))
        .unwrap();

    // Independently evaluate the same public query under the admitted claims.
    let query = Query::from("todos").limit(100);
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = {
        let owner = core.node();
        let mut owner = owner.borrow_mut();
        let mut scoped = owner.scoped_active_session_claims(alice, test_provider_claims(alice));
        let rows = block_on(scoped.query_rows_with_prepared_plan_for_identity(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            alice,
        ))
        .unwrap();
        assert_eq!(row_ids(&rows), vec![visible]);
        crate::binding_codec::encode_rows(&rows).unwrap()
    };

    let client = open_db(0xe6, alice, &schema);
    let (up, down) = link(alice, 0xe6, 0xc6);
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }

    let bytes = postcard::to_allocvec(&query).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            result_only: true,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    let result = (0..32)
        .find_map(|_| {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("remote read settled");
    let SerializedReadResult::EncodedRows(actual) = result else {
        panic!("expected the authority result without local coverage");
    };
    assert_eq!(actual, expected);
    assert_eq!(client.query_coverage_attachment_counts_for_test(), (0, 0));

    // The ordinary Global read still waits for receiver coverage and returns
    // locally materialized rows. Result-only delivery is an explicit choice.
    drop(read);
    let mut ordinary = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let result = (0..64)
        .find_map(|_| {
            if let Poll::Ready(result) = ordinary.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("ordinary Global read settled through receiver coverage");
    let SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary Global read must materialize local rows");
    };
    assert_eq!(row_ids(&rows), vec![visible]);
}

#[test]
fn bounded_global_read_proxies_through_scope_isolated_relay() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa7; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb7; 16]);
    let core = open_core(0xc7, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let visible = row(0xd8);
    core.insert_with_id("todos", visible, cells("visible", false, alice))
        .unwrap();
    core.insert_with_id("todos", row(0xd9), cells("hidden", false, bob))
        .unwrap();

    let query = Query::from("todos").limit(100);
    let shape = query.validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let expected = {
        let owner = core.node();
        let mut owner = owner.borrow_mut();
        let mut scoped = owner.scoped_active_session_claims(alice, test_provider_claims(alice));
        let rows = block_on(scoped.query_rows_with_prepared_plan_for_identity(
            &shape,
            &binding,
            DurabilityTier::Global,
            None,
            alice,
        ))
        .unwrap();
        assert_eq!(row_ids(&rows), vec![visible]);
        crate::binding_codec::encode_rows(&rows).unwrap()
    };

    let relay = open_db(0xe7, alice, &schema);
    relay.set_relay_authority_session_owner_for_test();
    let client = open_db(0xf7, alice, &schema);
    client
        .node
        .node
        .borrow_mut()
        .set_session_claims(alice, test_provider_claims(alice));
    let (relay_up, core_down) = link(alice, 0xe7, 0xc7);
    let _relay_up = block_on(relay.connect_upstream(relay_up));
    let _core_down = core.accept_scope_isolated_relay_subscriber(
        core_down,
        alice,
        test_provider_claims(alice),
        1,
    );
    let (client_up, relay_down) = link(alice, 0xf7, 0xe7);
    let _client_up = block_on(client.connect_upstream(client_up));
    let _relay_down =
        relay.accept_subscriber_with_claims(relay_down, alice, test_provider_claims(alice));
    for _ in 0..6 {
        client.tick().unwrap();
        relay.tick().unwrap();
        core.tick().unwrap();
    }

    let bytes = postcard::to_allocvec(&query).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            result_only: true,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    let result = (0..32)
        .find_map(|_| {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            relay.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("relayed remote read settled");
    let SerializedReadResult::EncodedRows(actual) = result else {
        panic!("expected relayed authority result without local coverage");
    };
    assert_eq!(actual, expected);
    assert_eq!(client.query_coverage_attachment_counts_for_test(), (0, 0));
}

#[test]
fn bounded_global_read_falls_back_when_peer_lacks_remote_read_feature() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa8; 16]);
    let core = open_core(0xc8, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let visible = row(0xda);
    core.insert_with_id("todos", visible, cells("visible", false, alice))
        .unwrap();
    let client = open_db(0xe8, alice, &schema);
    let (up, down) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xe8; 16]),
        1,
        NodeUuid::from_bytes([0xc8; 16]),
        1,
    );
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let bytes = postcard::to_allocvec(&Query::from("todos").limit(100)).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Deferred,
            result_only: true,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    let result = (0..48)
        .find_map(|_| {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("legacy coverage read settled");
    let SerializedReadResult::Rows(rows) = result else {
        panic!("old peer must use the local coverage read");
    };
    assert_eq!(row_ids(&rows), vec![visible]);
}

// Claim admission is a transport fact, so this test changes it internally
// while the public read is pending and checks the user-visible result route.
#[test]
fn bounded_global_read_discards_result_after_session_claim_revision_changes() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa9; 16]);
    let core = open_core(0xc9, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let visible = row(0xdb);
    core.insert_with_id("todos", visible, cells("visible", false, alice))
        .unwrap();
    let client = open_db(0xe9, alice, &schema);
    let (up, down) = link(alice, 0xe9, 0xc9);
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }

    let bytes = postcard::to_allocvec(&Query::from("todos").limit(100)).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            result_only: true,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    assert!(read.as_mut().poll(&mut context).is_pending());
    client.tick().unwrap();
    core.tick().unwrap();
    client
        .node
        .node
        .borrow_mut()
        .set_session_claims(alice, BTreeMap::new());
    client
        .node
        .node
        .borrow_mut()
        .set_session_claims(alice, test_provider_claims(alice));

    let result = (0..64)
        .find_map(|_| {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("coverage fallback settled after claim change");
    let SerializedReadResult::Rows(rows) = result else {
        panic!("stale authority result must be discarded");
    };
    assert_eq!(row_ids(&rows), vec![visible]);
}

#[test]
fn immediate_global_read_with_local_write_uses_coverage_fallback() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xaa; 16]);
    let core = open_core(0xca, AuthorSubject::SYSTEM, &schema);
    core.server.enable_authoritative_scalar_exit_refresh();
    let saved = row(0xdc);
    let pending = row(0xdd);
    core.insert_with_id("todos", saved, cells("saved", false, alice))
        .unwrap();
    let client = open_db(0xea, alice, &schema);
    let (up, down) = link(alice, 0xea, 0xca);
    let _up = block_on(client.connect_upstream(up));
    let _down = core.accept_subscriber(down, alice);
    for _ in 0..4 {
        client.tick().unwrap();
        core.tick().unwrap();
    }
    let pending_write = block_on(client.insert(
        "todos",
        cells("pending", false, alice),
        InsertOptions {
            row_id: Some(pending),
            ..Default::default()
        },
    ))
    .unwrap();
    client.tick().unwrap();
    block_on(pending_write.wait(DurabilityTier::Local)).unwrap();

    let bytes = postcard::to_allocvec(&Query::from("todos").limit(100)).unwrap();
    let mut read = Box::pin(client.all_serialized_query(
        &bytes,
        ReadOpts {
            tier: DurabilityTier::Global,
            local_updates: LocalUpdates::Immediate,
            result_only: true,
            ..ReadOpts::default()
        },
        None,
        None,
        None,
        true,
        || false,
        |attachment| client.detach_query(attachment),
    ));
    let mut context = Context::from_waker(Waker::noop());
    let result = (0..64)
        .find_map(|_| {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return Some(result.unwrap());
            }
            client.tick().unwrap();
            core.tick().unwrap();
            None
        })
        .expect("local-write coverage read settled");
    let SerializedReadResult::Rows(rows) = result else {
        panic!("pending local write must use the coverage read");
    };
    // The unconfirmed local row is not in the Global result yet.
    assert_eq!(row_ids(&rows), vec![saved]);
}
