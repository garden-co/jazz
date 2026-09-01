//! Shape-registration ownership, retention, and hostile-cardinality boundaries.

use super::*;
use std::collections::BTreeMap;

const EXPECTED_MAX_SHAPE_REGISTRATIONS_PER_PEER: usize = 1024;
const EXPECTED_MAX_RETAINED_PEER_SHAPES: usize = 1024;
const EXISTING_SINGLE_PEER_ACTIVE_QUERY_COUNT: usize = 1_000;

fn distinct_shape(schema: &JazzSchema, index: usize) -> ValidatedQuery {
    Query::from("todos")
        .filter(eq(col("title"), param(format!("shape_{index}"))))
        .validate(schema)
        .unwrap()
}

fn register_shape_message(shape: &ValidatedQuery) -> SyncMessage {
    SyncMessage::RegisterShape {
        shape_id: shape.shape_id(),
        ast: ShapeAst::from_validated(shape),
        opts: RegisterShapeOptions::default(),
    }
}
fn subscribe_message(shape: &ValidatedQuery, usage_seed: u8) -> (SyncMessage, SubscriptionKey) {
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: BindingId(uuid::Uuid::from_bytes([usage_seed; 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    (
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: vec![Value::String("shared".to_owned())],
            known_state: None,
            delegated_session: None,
        }),
        subscription,
    )
}

fn shape_unsubscribe(shape: &ValidatedQuery) -> SyncMessage {
    SyncMessage::Unsubscribe {
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: BindingId(uuid::Uuid::nil()),
            read_view: RegisterShapeOptions::default().read_view_key(),
        },
    }
}

// This is intentionally an internal hostile-wire test: public query APIs do
// not expose `BindingSource`, but a peer can still encode the private enum
// variant on the wire.  The receiving topology, rather than an untrusted
// caller's registration options, owns that internal authority-result source.
#[test]
fn session_peer_cannot_choose_relay_authority_binding_source() {
    let schema = schema();
    let server = open_core(0x69, AuthorSubject::SYSTEM, &schema);
    let shape = distinct_shape(&schema, 69);
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server
        .server
        .accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x6a; 16]));
    let mut opts = RegisterShapeOptions::default();
    opts.binding_source = crate::protocol::BindingSource::RelayAuthoritySession;

    client_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();

    assert!(matches!(
        client_transport.try_recv(),
        Some(SyncMessage::SubscribeRejected {
            reason: SubscribeRejectReason::UnsupportedShapeCapability { detail },
            ..
        }) if detail.contains("authenticated SYSTEM trusted-backend relay")
    ));
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(shape.shape_id())
            .is_none(),
        "a rejected caller-owned internal source must not reach shared shape retention"
    );
}

// A relay's authenticated transport capability deliberately carries no
// application principal. This internal admission test proves that ordinary
// query traffic cannot regain the old SYSTEM fallback by merely omitting the
// delegated-session field.
#[test]
fn unbound_relay_cannot_subscribe_as_system() {
    let schema = schema();
    let server = open_core(0x6e, AuthorSubject::SYSTEM, &schema);
    let shape = distinct_shape(&schema, 71);
    let binding = shape
        .bind(BTreeMap::from([(
            String::from("shape_71"),
            Value::String("shared".into()),
        )]))
        .unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_relay_subscriber(server_transport);
    relay_transport
        .send(register_shape_message(&shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    relay_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: vec![Value::String("shared".to_owned())],
            known_state: None,
            delegated_session: None,
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();

    let connection = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!("accepted relay remains a subscriber link");
    };
    assert!(
        state.served.is_empty(),
        "unbound relay traffic must not be evaluated as SYSTEM or any other principal"
    );
}

// This remains internal because only the peer admission seam sees both raw
// wire messages and the topology-authenticated relay capability. It proves
// that raw relay frames cannot mutate globally cached session claims or select
// a policy subject; any delegated binding is request-local and host-admitted.
#[test]
fn relay_cannot_seed_or_consume_delegated_claims() {
    let schema = schema();
    let server = open_core(0x6b, AuthorSubject::SYSTEM, &schema);
    let delegated = AuthorSubject::for_test_bytes([0x6d; 16]);
    let shape = distinct_shape(&schema, 70);
    let binding = shape
        .bind(BTreeMap::from([(
            String::from("shape_70"),
            Value::String("shared".into()),
        )]))
        .unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_relay_subscriber(server_transport);
    let hostile_claims = BTreeMap::from([("hostile".to_owned(), Value::Bool(true))]);

    relay_transport
        .send(SyncMessage::SessionClaims {
            identity: delegated,
            claims: hostile_claims.clone(),
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert_ne!(
        server.node().borrow().session_claims_for(delegated),
        hostile_claims,
        "a subjectless relay must not seed a delegated session map"
    );

    relay_transport
        .send(register_shape_message(&shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    let delegated_session = crate::protocol::DelegatedSessionBinding {
        identity: delegated,
        claims: hostile_claims,
    };
    relay_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: vec![Value::String("shared".to_owned())],
            known_state: None,
            delegated_session: Some(delegated_session.clone()),
        }))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();

    let connection = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!("accepted peer remains a subscriber link");
    };
    assert!(
        state.served.is_empty(),
        "a non-SYSTEM backend must not consume a caller-supplied delegated policy binding"
    );
    drop(connection);
    while relay_transport.try_recv().is_some() {
        // The rejected Subscribe may have raced an unrelated control flush;
        // the repair assertion below concerns only the hostile fetch.
    }

    relay_transport
        .send(SyncMessage::FetchRowVersions {
            requests: Vec::new(),
            delegated_session: Some(delegated_session),
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        !matches!(
            relay_transport.try_recv(),
            Some(SyncMessage::RowVersionPayloads { .. })
        ),
        "a non-relay backend must not consume a caller-supplied delegated repair binding"
    );
}

// This stays internal because installed query-program retention and peer ownership
// are deliberately absent from the public database API.
#[test]
fn repeated_anonymous_shape_registration_is_reclaimed_by_one_unsubscribe() {
    let schema = schema();
    let server = open_core(0x71, AuthorSubject::SYSTEM, &schema);
    let shape = distinct_shape(&schema, 0);
    let (mut client_transport, server_transport) = duplex();
    let subscriber =
        server.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x72; 16]));

    client_transport
        .send(register_shape_message(&shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    client_transport
        .send(register_shape_message(&shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(shape.shape_id())
            .is_some()
    );

    client_transport.send(shape_unsubscribe(&shape)).unwrap();
    subscriber.borrow_mut().tick().unwrap();

    assert!(
        server
            .node()
            .borrow()
            .registered_shape(shape.shape_id())
            .is_none(),
        "repeating one peer's anonymous registration must not create an unreclaimable refcount"
    );
}

// This stays internal because shared program deduplication and connection-local
// registration ownership are deliberately absent from the public database API.
#[test]
fn shared_shape_survives_first_owner_unsubscribe_and_leaves_with_last_owner() {
    let schema = schema();
    let server = open_core(0x73, AuthorSubject::SYSTEM, &schema);
    let shape = distinct_shape(&schema, 1);
    let (mut first_transport, first_server_transport) = duplex();
    let first = server.accept_subscriber(
        first_server_transport,
        AuthorSubject::for_test_bytes([0x74; 16]),
    );
    let (mut second_transport, second_server_transport) = duplex();
    let second = server.accept_subscriber(
        second_server_transport,
        AuthorSubject::for_test_bytes([0x75; 16]),
    );

    first_transport
        .send(register_shape_message(&shape))
        .unwrap();
    first.borrow_mut().tick().unwrap();
    second_transport
        .send(register_shape_message(&shape))
        .unwrap();
    second.borrow_mut().tick().unwrap();
    let (first_subscribe, first_subscription) = subscribe_message(&shape, 0x74);
    first_transport.send(first_subscribe).unwrap();
    first.borrow_mut().tick().unwrap();
    let (second_subscribe, second_subscription) = subscribe_message(&shape, 0x75);
    second_transport.send(second_subscribe).unwrap();
    second.borrow_mut().tick().unwrap();

    first_transport
        .send(SyncMessage::Unsubscribe {
            subscription: first_subscription,
        })
        .unwrap();
    first.borrow_mut().tick().unwrap();
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(shape.shape_id())
            .is_some(),
        "one peer's unsubscribe must not drop a shared shape still owned by another peer"
    );

    second_transport
        .send(SyncMessage::Unsubscribe {
            subscription: second_subscription,
        })
        .unwrap();
    second.borrow_mut().tick().unwrap();
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(shape.shape_id())
            .is_none(),
        "the final peer owner must reclaim the shared installed shape"
    );
}
// This stays internal because parked registration retention is intentionally
// invisible until its missing catalogue lineage arrives.
#[test]
fn unsubscribed_parked_shape_does_not_install_when_its_catalogue_arrives() {
    let base = schema();
    let evolved = build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("todos")
                    .column("title", PublicColumnType::Text)
                    .column("done", PublicColumnType::Boolean)
                    .column("owner", PublicColumnType::Uuid),
            )
            .table(PublicTableSchemaBuilder::new("notes").column("body", PublicColumnType::Text)),
    );
    let evolved_version = SchemaVersion::new(evolved.clone());
    let shape = Query::from("notes")
        .filter(eq(col("body"), param("parked_body")))
        .validate(&evolved)
        .unwrap();
    let server = open_core(0x79, AuthorSubject::SYSTEM, &base);
    let (mut client_transport, server_transport) = duplex();
    let subscriber =
        server.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x7a; 16]));

    client_transport
        .send(register_shape_message(&shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(shape.shape_id())
            .is_none()
    );

    client_transport.send(shape_unsubscribe(&shape)).unwrap();
    subscriber.borrow_mut().tick().unwrap();

    let publication = server
        .node()
        .borrow()
        .author_schema_lineage_publication(
            evolved_version.clone(),
            MigrationLens::new(
                base.version_id(),
                evolved_version.id,
                vec![TableLens {
                    source_table: "todos".to_owned(),
                    target_table: "todos".to_owned(),
                    ops: vec![],
                }],
            )
            .expect("valid migration lens"),
            ["notes"],
            Vec::<String>::new(),
        )
        .unwrap();
    let node = server.node();
    let mut node = node.borrow_mut();
    let catalogue_seq = node.active_catalogue_seq().saturating_add(1);
    node.apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
        author: AuthorSubject::SYSTEM,
        catalogue_seq,
        publication: Box::new(publication),
    })
    .unwrap();

    assert!(
        node.registered_shape(shape.shape_id()).is_none(),
        "catalogue arrival must not resurrect a parked shape after its final owner unsubscribed"
    );
}

// This stays internal because abrupt peer teardown has no public receipt for the
// process-local registered-shape catalogue it must reclaim.
#[test]
fn disconnect_reclaims_all_shapes_owned_by_the_departed_peer() {
    let schema = schema();
    let server = open_core(0x76, AuthorSubject::SYSTEM, &schema);
    let first_shape = distinct_shape(&schema, 2);
    let second_shape = distinct_shape(&schema, 3);
    let (mut client_transport, server_transport) = duplex();
    let subscriber =
        server.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x77; 16]));

    client_transport
        .send(register_shape_message(&first_shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    client_transport
        .send(register_shape_message(&second_shape))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();

    assert!(server.server.detach_connection(&subscriber));

    let node = server.node();
    let node = node.borrow();
    assert!(node.registered_shape(first_shape.shape_id()).is_none());
    assert!(node.registered_shape(second_shape.shape_id()).is_none());
}
// This stays internal because the registration announcement cache is a wire
// optimization. The observable contract is that a same-cycle unsubscribe and
// reattach still sends the registration before the replacement subscription.
#[test]
fn same_cycle_reattach_reannounces_shape_after_unsubscribe() {
    let schema = schema();
    let client = open_db(0x7b, AuthorSubject::for_test_bytes([0x7b; 16]), &schema);
    let query = Query::from("todos").filter(eq(col("title"), lit("reattach")));
    let prepared = prepared(&client, &query);
    let (client_transport, _server_transport, client_sent, _) = duplex_with_taps();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let first = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let first_subscription = first.subscription();
    client.tick().unwrap();
    assert!(client_sent.borrow().iter().any(
        |message| matches!(message, SyncMessage::RegisterShape { shape_id, .. } if *shape_id == prepared.shape().shape_id())
    ));
    client_sent.borrow_mut().clear();

    client.detach_query(first);
    let replacement = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    let replacement_subscription = replacement.subscription();
    client.tick().unwrap();

    let sent = client_sent.borrow();
    let unsubscribe = sent
        .iter()
        .position(|message| {
            matches!(
                message,
                SyncMessage::Unsubscribe { subscription }
                    if *subscription == first_subscription
            )
        })
        .expect("the detached usage must unsubscribe");
    let register = sent
        .iter()
        .position(|message| {
            matches!(
                message,
                SyncMessage::RegisterShape { shape_id, .. }
                    if *shape_id == prepared.shape().shape_id()
            )
        })
        .expect("the replacement usage must reannounce its reclaimed shape");
    let subscribe = sent
        .iter()
        .position(|message| {
            matches!(
                message,
                SyncMessage::Subscribe(subscribe)
                    if subscribe.subscription == replacement_subscription
            )
        })
        .expect("the replacement usage must subscribe");
    assert!(
        unsubscribe < register && register < subscribe,
        "unsubscribe must invalidate the announcement before the replacement subscribe is emitted"
    );
}

// This stays internal because cardinality admission is a hostile-wire boundary;
// public query APIs cannot forge an over-limit registration stream. The
// 1,000-registration checkpoint covers the retained-shape cardinality required
// by the existing 1,000-active-query single-peer topology.
#[test]
fn shape_registration_cardinality_is_checked_before_peer_or_global_retention() {
    let schema = schema();
    let server = open_core(0x78, AuthorSubject::SYSTEM, &schema);
    let (mut client_transport, server_transport) = duplex();
    let subscriber =
        server.accept_subscriber(server_transport, AuthorSubject::for_test_bytes([0x80; 16]));

    for shape_index in 0..EXISTING_SINGLE_PEER_ACTIVE_QUERY_COUNT {
        let shape = distinct_shape(&schema, shape_index);
        client_transport
            .send(register_shape_message(&shape))
            .unwrap();
        subscriber.borrow_mut().tick().unwrap();
    }
    let retained = {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("accepted peer must be a subscriber");
        };
        state.shape_registrations.len()
    };
    assert_eq!(
        retained, EXISTING_SINGLE_PEER_ACTIVE_QUERY_COUNT,
        "one peer must retain enough shapes for the existing active-query topology"
    );

    for shape_index in
        EXISTING_SINGLE_PEER_ACTIVE_QUERY_COUNT..EXPECTED_MAX_SHAPE_REGISTRATIONS_PER_PEER
    {
        let shape = distinct_shape(&schema, shape_index);
        client_transport
            .send(register_shape_message(&shape))
            .unwrap();
        subscriber.borrow_mut().tick().unwrap();
    }
    let retained = {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("accepted peer must be a subscriber");
        };
        state.shape_registrations.len()
    };
    assert_eq!(retained, EXPECTED_MAX_SHAPE_REGISTRATIONS_PER_PEER);

    let peer_extra = distinct_shape(&schema, EXPECTED_MAX_SHAPE_REGISTRATIONS_PER_PEER);
    client_transport
        .send(register_shape_message(&peer_extra))
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(matches!(
        client_transport.try_recv(),
        Some(SyncMessage::SubscribeRejected { subscription, .. })
            if subscription.shape_id == peer_extra.shape_id()
    ));
    let retained = {
        let subscriber = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &subscriber.link else {
            panic!("accepted peer must be a subscriber");
        };
        state.shape_registrations.len()
    };
    assert_eq!(
        retained, EXPECTED_MAX_SHAPE_REGISTRATIONS_PER_PEER,
        "the 1,025th retained shape must be rejected"
    );
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(peer_extra.shape_id())
            .is_none()
    );

    let global_extra = distinct_shape(&schema, EXPECTED_MAX_RETAINED_PEER_SHAPES + 1);
    let (mut extra_transport, extra_server_transport) = duplex();
    let extra_peer = server.accept_subscriber(
        extra_server_transport,
        AuthorSubject::for_test_bytes([0x99; 16]),
    );
    extra_transport
        .send(register_shape_message(&global_extra))
        .unwrap();
    extra_peer.borrow_mut().tick().unwrap();
    assert!(matches!(
        extra_transport.try_recv(),
        Some(SyncMessage::SubscribeRejected { subscription, .. })
            if subscription.shape_id == global_extra.shape_id()
    ));
    let retained = {
        let extra_peer = extra_peer.borrow();
        let ConnectionLink::Subscriber(state) = &extra_peer.link else {
            panic!("accepted peer must be a subscriber");
        };
        state.shape_registrations.len()
    };
    assert_eq!(
        retained, 0,
        "the over-limit shape must not reach peer retention"
    );
    assert!(
        server
            .node()
            .borrow()
            .registered_shape(global_extra.shape_id())
            .is_none(),
        "the over-limit shape must not reach global retention"
    );
}
