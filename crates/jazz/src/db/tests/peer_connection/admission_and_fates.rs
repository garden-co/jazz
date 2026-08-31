//! Link admission, authority selection, permission advice, and routed fates.

use super::*;
use crate::db::peer_connection::{
    ConnectionLink, PendingRowVersionFetch, PendingSubscriberControlResponse,
    coverage_group_subscription_key, dispatch_admitted_subscriber_message,
};
use crate::node::SKEW_TOLERANCE_MS;

#[test]
fn authenticated_client_upload_uses_authority_clock_for_forward_skew() {
    let identity = AuthorSubject::for_test_bytes([0xc1; 16]);
    let schema = schema();
    let client = open_core(0xc1, identity, &schema);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let authority_now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let future_ms = authority_now_ms + SKEW_TOLERANCE_MS + 10_000;
    let (tx_id, unit) = client
        .node()
        .borrow_mut()
        .commit_mergeable_unit_settled(
            MergeableCommit::new("todos", RowUuid::from_bytes([0xf1; 16]), future_ms)
                .made_by(identity)
                .cells(cells("future", false, identity)),
        )
        .unwrap();
    let before = server.node().borrow().committed_global_time();
    let (mut client_transport, server_transport) = duplex();
    let subscriber = server.accept_subscriber(server_transport, identity);

    client_transport.send(unit).unwrap();
    let mut response = None;
    for _ in 0..3 {
        subscriber.borrow_mut().tick().unwrap();
        while let Some(message) = client_transport.try_recv() {
            if matches!(message, SyncMessage::FateUpdate { .. }) {
                response = Some(message);
            }
        }
    }
    let Some(SyncMessage::FateUpdate {
        fate, global_time, ..
    }) = response
    else {
        panic!("authority must return a fate");
    };
    assert_eq!(
        fate,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
    assert_eq!(global_time, None);
    assert_eq!(server.node().borrow().committed_global_time(), before);
    assert_eq!(
        server
            .node()
            .borrow_mut()
            .transaction_state(tx_id)
            .unwrap()
            .0,
        Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
    );
}

/// This stays at the peer/transport seam because public client APIs cannot
/// deliberately hold one accepted wire frame while rejecting the next logical
/// message. It proves the ownership boundary: a fate rejected by a bounded
/// transport remains in the connection-owned FIFO until the transport accepts
/// it, rather than disappearing with the synchronous tick turn.
#[test]
fn downstream_fate_retries_after_bounded_transport_backpressure() {
    let identity = AuthorSubject::for_test_bytes([0xc2; 16]);
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let subscriber = server.accept_subscriber(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        identity,
    );
    let fate = SyncMessage::FateUpdate {
        tx_id: TxId::new(TxTime::from(2), NodeUuid::from_bytes([0xc2; 16])),
        fate: Fate::Accepted,
        global_time: None,
        durability: Some(DurabilityTier::Edge),
    };
    subscriber
        .borrow()
        .downstream_fates
        .borrow_mut()
        .push(fate.clone());

    subscriber
        .borrow_mut()
        .tick()
        .expect("backpressure retains the fate and schedules a retry");
    assert_eq!(
        subscriber.borrow().downstream_fates.borrow().as_slice(),
        std::slice::from_ref(&fate),
        "a rejected wire admission leaves the exact fate at its semantic producer"
    );
    assert!(outbound.borrow().is_empty());

    subscriber
        .borrow_mut()
        .tick()
        .expect("later capacity accepts the retained fate");
    assert!(subscriber.borrow().downstream_fates.borrow().is_empty());
    assert!(matches!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    assert_eq!(outbound.borrow_mut().pop_front(), Some(fate));
    assert!(outbound.borrow().is_empty());
}

/// The ordinary-wire chunk responder is a legacy path below the public chunk
/// API. It needs the same bounded ownership rule as fates: a rejected byte
/// admission retains one response batch and does not consume another inbound
/// request until that batch is accepted.
#[test]
fn ordinary_wire_chunk_response_retries_after_bounded_transport_backpressure() {
    let identity = AuthorSubject::for_test_bytes([0xc3; 16]);
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let subscriber = server.accept_subscriber(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        identity,
    );
    let batch = ChunkResponseBatch {
        responses: vec![ChunkResponseEntry {
            request_id: 3,
            result: ChunkResponse::Unavailable,
        }],
    };
    subscriber.borrow_mut().pending_chunk_response = Some(batch.clone());

    subscriber
        .borrow_mut()
        .tick()
        .expect("backpressure retains the ordinary-wire chunk response");
    assert_eq!(
        subscriber.borrow().pending_chunk_response,
        Some(batch.clone())
    );
    assert!(outbound.borrow().is_empty());

    subscriber
        .borrow_mut()
        .tick()
        .expect("later capacity accepts the retained chunk response");
    assert!(subscriber.borrow().pending_chunk_response.is_none());
    assert_eq!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::ChunkResponseBatch(batch))
    );
    assert!(outbound.borrow().is_empty());
}

/// Missing-version repair is an upstream one-shot request, not a recomputable
/// subscription update. A bounded transport must therefore retain it locally
/// and arrange its own retry instead of relying on an unrelated reconnect or
/// inbound wakeup to make the repair possible.
#[test]
fn upstream_row_version_fetch_retries_after_bounded_transport_backpressure() {
    let identity = AuthorSubject::for_test_bytes([0xc3; 16]);
    let schema = schema();
    let client = open_db(0xc3, identity, &schema);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let upstream =
        crate::db::block_on(client.connect_upstream(Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        })));
    let request = RowVersionRef::new(
        "todos",
        RowUuid::from_bytes([0xc3; 16]),
        TxId::new(TxTime::from(3), NodeUuid::from_bytes([0xc3; 16])),
    );
    {
        let mut connection = upstream.borrow_mut();
        let ConnectionLink::Upstream(state) = &mut connection.link else {
            panic!("client connection must be upstream");
        };
        state
            .pending_row_version_fetches
            .push_back(PendingRowVersionFetch {
                requests: vec![request.clone()],
                policy_binding: (AuthorSubject::SYSTEM, BTreeMap::new()),
            });
    }

    upstream
        .borrow_mut()
        .tick()
        .expect("backpressure retains the upstream repair fetch");
    {
        let connection = upstream.borrow();
        let ConnectionLink::Upstream(state) = &connection.link else {
            panic!("client connection must be upstream");
        };
        assert_eq!(
            state.pending_row_version_fetches.front(),
            Some(&PendingRowVersionFetch {
                requests: vec![request.clone()],
                policy_binding: (AuthorSubject::SYSTEM, BTreeMap::new()),
            }),
            "a rejected byte admission retains the exact upstream repair request"
        );
    }
    assert!(outbound.borrow().is_empty());

    upstream
        .borrow_mut()
        .tick()
        .expect("scheduled retry accepts the upstream repair fetch");
    {
        let connection = upstream.borrow();
        let ConnectionLink::Upstream(state) = &connection.link else {
            panic!("client connection must be upstream");
        };
        assert!(state.pending_row_version_fetches.is_empty());
    }
    assert_eq!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::FetchRowVersions {
            requests: vec![request],
            delegated_session: None,
        })
    );
    assert!(outbound.borrow().is_empty());
}

/// Subscription rejection follows the same ownership rule. A malformed or
/// unsupported one-shot registration must not turn into a permanently pending
/// caller when the first byte admission is temporarily full.
#[test]
fn subscriber_control_reply_retries_after_bounded_transport_backpressure() {
    let identity = AuthorSubject::for_test_bytes([0xc4; 16]);
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let subscriber = server.accept_subscriber(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        identity,
    );
    let rejection = SyncMessage::SubscribeRejected {
        subscription: SubscriptionKey {
            shape_id: ShapeId(uuid::Uuid::from_bytes([4; 16])),
            binding_id: BindingId(uuid::Uuid::from_bytes([4; 16])),
            read_view: ReadViewKey::default(),
        },
        reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
    };
    subscriber.borrow_mut().pending_control_responses.extend([
        PendingSubscriberControlResponse::Direct(rejection.clone()),
        PendingSubscriberControlResponse::Direct(rejection.clone()),
    ]);

    subscriber
        .borrow_mut()
        .tick()
        .expect("backpressure retains the subscriber control reply");
    assert_eq!(
        subscriber
            .borrow()
            .pending_control_responses
            .iter()
            .map(|response| response.message().clone())
            .collect::<Vec<_>>(),
        vec![rejection.clone(), rejection.clone()],
        "a stalled link keeps every already-bounded control obligation in FIFO order"
    );
    assert!(outbound.borrow().is_empty());

    subscriber
        .borrow_mut()
        .tick()
        .expect("later capacity accepts every retained FIFO control reply");
    assert!(subscriber.borrow().pending_control_responses.is_empty());
    assert_eq!(outbound.borrow_mut().pop_front(), Some(rejection));
    assert!(matches!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::SubscribeRejected { .. })
    ));
    assert!(outbound.borrow().is_empty());
}

/// A permanently stalled link may retain only the control obligations already
/// implied by its live registrations/rejections. This stays at the transport
/// seam because a public client cannot deliberately hold an accepted frame
/// forever without also hiding the scheduler wake that the test needs to
/// inspect.
#[test]
fn subscriber_control_replies_stay_bounded_during_permanent_backpressure() {
    struct PermanentlyBackpressuredTransport {
        sends: Rc<Cell<usize>>,
    }

    impl Transport for PermanentlyBackpressuredTransport {
        fn send(&mut self, _message: SyncMessage) -> Result<(), TransportError> {
            self.sends.set(self.sends.get() + 1);
            Err(TransportError::Backpressure)
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            None
        }
    }

    let identity = AuthorSubject::for_test_bytes([0xc5; 16]);
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let sends = Rc::new(Cell::new(0));
    let subscriber = server.accept_subscriber(
        Box::new(PermanentlyBackpressuredTransport {
            sends: Rc::clone(&sends),
        }),
        identity,
    );
    let rejection = SyncMessage::SubscribeRejected {
        subscription: SubscriptionKey {
            shape_id: ShapeId(uuid::Uuid::from_bytes([5; 16])),
            binding_id: BindingId(uuid::Uuid::from_bytes([5; 16])),
            read_view: ReadViewKey::default(),
        },
        reason: SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission,
    };
    subscriber.borrow_mut().pending_control_responses.extend([
        PendingSubscriberControlResponse::Direct(rejection.clone()),
        PendingSubscriberControlResponse::Direct(rejection.clone()),
    ]);

    for _ in 0..4 {
        subscriber
            .borrow_mut()
            .tick()
            .expect("backpressure is a deferred retry, not a fatal connection error");
        assert_eq!(
            subscriber
                .borrow()
                .pending_control_responses
                .iter()
                .map(|response| response.message().clone())
                .collect::<Vec<_>>(),
            vec![rejection.clone(), rejection.clone()],
            "retries retain the same bounded FIFO without accumulating copies"
        );
    }
    assert_eq!(
        sends.get(),
        4,
        "one logical control reply is retried per tick"
    );
}

/// Repair payloads retain the normal sync-context send path. This matters on
/// trusted links where `send_with_sync_context` may first announce a catalogue
/// snapshot; the row-version response itself must still remain pending until
/// the adapter accepts it.
#[test]
fn row_version_repair_reply_retries_with_sync_context_after_backpressure() {
    let identity = AuthorSubject::for_test_bytes([0xc6; 16]);
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let subscriber = server.accept_subscriber(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        identity,
    );
    let response = SyncMessage::RowVersionPayloads {
        version_bundles: Vec::new(),
    };
    subscriber.borrow_mut().pending_control_responses.push_back(
        PendingSubscriberControlResponse::WithSyncContext(response.clone()),
    );

    subscriber
        .borrow_mut()
        .tick()
        .expect("bounded transport defers the repair reply");
    assert_eq!(
        subscriber
            .borrow()
            .pending_control_responses
            .front()
            .map(PendingSubscriberControlResponse::message),
        Some(&response)
    );
    subscriber
        .borrow_mut()
        .tick()
        .expect("later capacity accepts the retained repair reply");
    assert!(subscriber.borrow().pending_control_responses.is_empty());
    assert!(matches!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    assert_eq!(outbound.borrow_mut().pop_front(), Some(response));
    assert!(outbound.borrow().is_empty());
}

/// An authorization-scope intent remains owned by the requesting client until
/// its upstream wire admission succeeds, so a one-shot backpressure refusal
/// cannot strand Alice's permission preflight forever.
///
/// ```text
/// alice ──scope intent──► bounded upstream ──✗──► authority
/// alice ──retry─────────► bounded upstream ─────► authority
/// ```
#[test]
fn upstream_authorization_scope_intent_retries_after_bounded_transport_backpressure() {
    struct BackpressureOnceAdmittedTransport {
        outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
        failed: bool,
        session_context: ConnectionSessionContext,
    }

    impl Transport for BackpressureOnceAdmittedTransport {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            if !self.failed {
                self.failed = true;
                return Err(TransportError::Backpressure);
            }
            self.outbound.borrow_mut().push_back(message);
            Ok(())
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            None
        }

        fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
            Some(self.session_context)
        }
    }

    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let schema = schema();
    let client = open_db(0xc4, author, &schema);
    let (transport, _authority_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xc4; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let upstream = crate::db::block_on(client.connect_upstream(transport));
    let session_context = upstream
        .borrow()
        .transport
        .connection_session_context()
        .expect("the real admitted transport supplies an authority context");
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    upstream.borrow_mut().transport = Box::new(BackpressureOnceAdmittedTransport {
        outbound: Rc::clone(&outbound),
        failed: false,
        session_context,
    });

    let advice = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(4),
    });

    upstream
        .borrow_mut()
        .tick()
        .expect("backpressure keeps the authority intent pending");
    assert!(outbound.borrow().is_empty());
    {
        let connection = upstream.borrow();
        let ConnectionLink::Upstream(state) = &connection.link else {
            panic!("client connection must be upstream");
        };
        assert!(state.pending.iter().any(|command| matches!(
            command,
            PendingUpstreamCommand::AuthorizationScopeIntent { .. }
        )));
        assert!(
            state
                .scope_lease_manager
                .requests
                .values()
                .all(|request| !request.intent_sent)
        );
    }

    upstream
        .borrow_mut()
        .tick()
        .expect("later capacity retries the retained authority intent");
    assert!(matches!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::AuthorizationScopeIntent { .. })
    ));
    assert!(outbound.borrow().is_empty());
    {
        let connection = upstream.borrow();
        let ConnectionLink::Upstream(state) = &connection.link else {
            panic!("client connection must be upstream");
        };
        assert!(state.pending.is_empty());
        assert!(
            state
                .scope_lease_manager
                .requests
                .values()
                .all(|request| request.intent_sent)
        );
    }
    drop(advice);
}

/// A request captures its session claims before the first intent is admitted.
/// This seam test keeps that intent behind one bounded send, advances only the
/// same author's ambient claims, then reconnects to a B-context authority. The
/// old A-bound request must close conservatively; only a new B-owned request
/// may receive the successor's receipt.
#[test]
fn backpressured_scope_intent_claim_transition_closes_before_reconnect() {
    struct ScopeIntentBackpressureTransport {
        outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
        failed_scope_intent: bool,
        session_context: ConnectionSessionContext,
    }

    impl Transport for ScopeIntentBackpressureTransport {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            if matches!(message, SyncMessage::AuthorizationScopeIntent { .. })
                && !self.failed_scope_intent
            {
                self.failed_scope_intent = true;
                return Err(TransportError::Backpressure);
            }
            self.outbound.borrow_mut().push_back(message);
            Ok(())
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            None
        }

        fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
            Some(self.session_context)
        }
    }

    let schema = editor_claim_write_schema();
    let author = AuthorSubject::for_test_bytes([0xc4; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc4, author, &schema);
    let a_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("editor".to_owned()),
    )]);
    let b_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("viewer".to_owned()),
    )]);
    client.set_test_provider_claims(author, a_claims.clone());
    let (first_transport, _first_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xc4; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let upstream = crate::db::block_on(client.connect_upstream(first_transport));
    let session_context = upstream
        .borrow()
        .transport
        .connection_session_context()
        .expect("the admitted transport supplies an authority context");
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    upstream.borrow_mut().transport = Box::new(ScopeIntentBackpressureTransport {
        outbound: Rc::clone(&outbound),
        failed_scope_intent: false,
        session_context,
    });

    let advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });
    client.tick().unwrap();
    {
        let connection = upstream.borrow();
        let ConnectionLink::Upstream(state) = &connection.link else {
            panic!("client connection must be upstream");
        };
        assert!(state.pending.iter().any(|command| matches!(
            command,
            PendingUpstreamCommand::AuthorizationScopeIntent {
                session_claim_binding: Some((_, claims)),
                ..
            } if *claims == a_claims
        )));
        assert!(
            state
                .scope_lease_manager
                .requests
                .values()
                .any(|request| request.session_claim_binding.1 == a_claims),
            "allocation captures A before the first intent reaches the wire"
        );
    }
    assert!(
        !outbound
            .borrow()
            .iter()
            .any(|message| matches!(message, SyncMessage::AuthorizationScopeIntent { .. })),
        "the scope intent remains retained after its one bounded refusal"
    );

    client.set_test_provider_claims(author, b_claims.clone());
    assert!(client.detach_connection(&upstream));
    let (retry_transport, retry_server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xc4; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let retry_upstream = crate::db::block_on(client.connect_upstream(retry_transport));
    let retry_subscriber =
        server.accept_subscriber_with_claims(retry_server_transport, author, a_claims.clone());
    retry_subscriber
        .borrow_mut()
        .update_authenticated_session_claims(b_claims);
    client.tick().unwrap();
    {
        let connection = retry_upstream.borrow();
        let ConnectionLink::Upstream(state) = &connection.link else {
            panic!("replacement client connection must be upstream");
        };
        assert!(
            state.scope_lease_manager.requests.is_empty(),
            "claim transition closes the A request instead of sending B a mixed-context intent"
        );
    }
    assert_eq!(
        block_on(advice),
        PermissionAdvice::Unknown,
        "the B-context successor cannot settle the A-bound request"
    );

    let b_advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(b_advice), PermissionAdvice::Denied);
}

/// This is the same transition without a detach. The retained command must
/// remember A after its bounded send refusal and close before it can ask the
/// still-connected authority for a B-shaped proof.
#[test]
fn backpressured_scope_intent_claim_transition_closes_on_same_connection() {
    struct NullTransport;

    impl Transport for NullTransport {
        fn send(&mut self, _: SyncMessage) -> Result<(), TransportError> {
            Err(TransportError::Failed(
                "test placeholder must never send".to_owned(),
            ))
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            None
        }
    }

    struct ScopeIntentBackpressureTransport {
        inner: Box<dyn Transport>,
        failed_scope_intent: bool,
    }

    impl Transport for ScopeIntentBackpressureTransport {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            if matches!(message, SyncMessage::AuthorizationScopeIntent { .. })
                && !self.failed_scope_intent
            {
                self.failed_scope_intent = true;
                return Err(TransportError::Backpressure);
            }
            self.inner.send(message)
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            self.inner.try_recv()
        }

        fn connection_session_context(&self) -> Option<ConnectionSessionContext> {
            self.inner.connection_session_context()
        }
    }

    let schema = editor_claim_write_schema();
    let author = AuthorSubject::for_test_bytes([0xc5; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc5, author, &schema);
    let a_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("editor".to_owned()),
    )]);
    let b_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("viewer".to_owned()),
    )]);
    client.set_test_provider_claims(author, a_claims.clone());
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xc5; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber_with_claims(server_transport, author, a_claims);
    let original_transport = {
        let mut connection = upstream.borrow_mut();
        std::mem::replace(&mut connection.transport, Box::new(NullTransport))
    };
    upstream.borrow_mut().transport = Box::new(ScopeIntentBackpressureTransport {
        inner: original_transport,
        failed_scope_intent: false,
    });

    let advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });
    client.tick().unwrap();
    client.set_test_provider_claims(author, b_claims.clone());
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(b_claims);
    client.tick().unwrap();
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut advice = Box::pin(advice);
    assert_eq!(
        advice.as_mut().poll(&mut context),
        Poll::Ready(PermissionAdvice::Unknown),
        "a retained A command closes before the same connection can admit it under B"
    );
    let hydration_count = match &subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState {
            authority_scope_hydration_count,
            ..
        }) => *authority_scope_hydration_count,
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        hydration_count, 0,
        "no A request reaches the B authority, so no support shape can be disclosed"
    );

    let b_advice = client.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("candidate", false, author),
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(b_advice), PermissionAdvice::Denied);
}

/// A permission preflight is a node-owned caller obligation, not an
/// old-transport obligation. When the selected authority disconnects after
/// accepting the request, the successor must receive one fresh intent and
/// resolve the original future.
#[test]
fn scope_intent_retries_after_upstream_reconnect() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xd4; 16]);
    let client = open_db(0xd4, author, &schema);
    let (first_transport, mut first_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd4; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let first = crate::db::block_on(client.connect_upstream(first_transport));
    let advice = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(4),
    });
    client.tick().unwrap();
    assert!(matches!(
        try_recv_subscriber_payload(first_authority.as_mut()),
        Some(SyncMessage::AuthorizationScopeIntent { .. })
    ));
    assert!(client.detach_connection(&first));

    let (second_transport, mut second_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd4; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let _second = crate::db::block_on(client.connect_upstream(second_transport));
    client.tick().unwrap();
    let request_id = match try_recv_subscriber_payload(second_authority.as_mut()) {
        Some(SyncMessage::AuthorizationScopeIntent { request_id, .. }) => request_id,
        message => panic!("reconnect must retry the live scope intent, got {message:?}"),
    };
    second_authority
        .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
        .unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(advice), PermissionAdvice::Unknown);
}

#[test]
fn reconnect_replays_live_scope_waiters_once_and_drops_cancelled_ones() {
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xd5; 16]);
    let client = open_db(0xd5, author, &schema);
    let (first_transport, mut first_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd5; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let first = crate::db::block_on(client.connect_upstream(first_transport));
    let action = PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(5),
    };
    let first_live = client.request_permission_advice(action.clone());
    let second_live = client.request_permission_advice(action);
    let cancelled = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(6),
    });
    client.tick().unwrap();
    assert!(matches!(
        try_recv_subscriber_payload(first_authority.as_mut()),
        Some(SyncMessage::AuthorizationScopeIntent { .. })
    ));
    assert!(matches!(
        try_recv_subscriber_payload(first_authority.as_mut()),
        Some(SyncMessage::AuthorizationScopeIntent { .. })
    ));
    drop(cancelled);
    assert!(client.detach_connection(&first));

    let (second_transport, mut second_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd5; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let _second = crate::db::block_on(client.connect_upstream(second_transport));
    client.tick().unwrap();
    let request_id = match try_recv_subscriber_payload(second_authority.as_mut()) {
        Some(SyncMessage::AuthorizationScopeIntent { request_id, action }) => {
            assert_eq!(
                action,
                PermissionAdviceAction::Read {
                    table: "todos".to_owned(),
                    row: row(5),
                }
            );
            request_id
        }
        message => panic!("reconnect must replay the one live shared intent, got {message:?}"),
    };
    assert!(
        try_recv_subscriber_payload(second_authority.as_mut()).is_none(),
        "two live waiters share one retry and the dropped waiter is not replayed"
    );
    second_authority
        .send(SyncMessage::AuthorizationScopeUnavailable { request_id })
        .unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(first_live), PermissionAdvice::Unknown);
    assert_eq!(block_on(second_live), PermissionAdvice::Unknown);
}

/// An authority-scope intent may expand to a multi-frame proof sequence. Its
/// semantic producer queues that sequence before returning to inbound work, so
/// bounded wire admission must preserve both order and exact multiplicity.
#[test]
fn authorization_scope_replies_retry_fifo_after_backpressure() {
    let identity = AuthorSubject::for_test_bytes([0xc7; 16]);
    let schema = schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let subscriber = server.accept_subscriber(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        identity,
    );
    let first = SyncMessage::AuthorizationScopeUnavailable {
        request_id: PermissionAdviceRequestId([7; 16]),
    };
    let second = SyncMessage::AuthorizationScopeUnavailable {
        request_id: PermissionAdviceRequestId([8; 16]),
    };
    subscriber.borrow_mut().pending_control_responses.extend([
        PendingSubscriberControlResponse::Direct(first.clone()),
        PendingSubscriberControlResponse::Direct(second.clone()),
    ]);

    subscriber
        .borrow_mut()
        .tick()
        .expect("backpressure retains the whole authority-scope reply sequence");
    assert_eq!(
        subscriber
            .borrow()
            .pending_control_responses
            .iter()
            .map(PendingSubscriberControlResponse::message)
            .cloned()
            .collect::<Vec<_>>(),
        vec![first.clone(), second.clone()]
    );
    subscriber
        .borrow_mut()
        .tick()
        .expect("first scope reply is accepted once capacity returns");
    assert_eq!(outbound.borrow_mut().pop_front(), Some(first));
    subscriber
        .borrow_mut()
        .tick()
        .expect("second scope reply remains FIFO behind the first");
    assert_eq!(outbound.borrow_mut().pop_front(), Some(second));
    assert!(subscriber.borrow().pending_control_responses.is_empty());
}

#[test]
fn catalogue_fingerprint_change_is_eager_only_on_trusted_backend_link() {
    // This stays internal because trust is authenticated by the host at the
    // transport boundary; exposing it through a public client fixture would
    // test the HTTP/WebSocket bootstrap race rather than this hop contract.
    let base = schema();
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &base);

    let (mut edge_transport, core_edge_transport) = duplex();
    let edge_link = core.accept_subscriber_with_trust(
        core_edge_transport,
        AuthorSubject::for_test_bytes([0xe1; 16]),
        CommitUnitTrust::TrustedBackend,
    );
    let (mut client_transport, core_client_transport) = duplex();
    let client_link = core.accept_subscriber(
        core_client_transport,
        AuthorSubject::for_test_bytes([0xc1; 16]),
    );

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

    let evolved = SchemaVersion::new(build_public_db_test_schema(
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
        evolved.id,
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
    let publication = core
        .server
        .node()
        .borrow()
        .author_schema_lineage_publication(
            evolved.clone(),
            lens,
            Vec::<String>::new(),
            Vec::<String>::new(),
        )
        .expect("core authority authors evolved lineage");
    core.server
        .node()
        .borrow_mut()
        .apply_trusted_catalogue_message_settled(SyncMessage::PublishSchemaWithLens {
            author: AuthorSubject::SYSTEM,
            catalogue_seq: 1,
            publication: Box::new(publication),
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
    let identity = AuthorSubject::for_test_bytes([0x71; 16]);
    let schema = schema();
    let client = open_db(0x72, identity, &schema);
    let server = open_core(0x73, AuthorSubject::SYSTEM, &schema);
    let client_node = NodeUuid::from_bytes([0x72; 16]);
    let server_node = NodeUuid::from_bytes([0x73; 16]);
    let (client_transport, server_transport) =
        duplex_with_admitted_session_context(identity, client_node, 41, server_node, 97);
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, identity);
    assert_eq!(upstream.borrow().connection_epoch, 41);
    assert_eq!(subscriber.borrow().connection_epoch, 97);

    let expected = AuthorityContext {
        authority: *server_node.as_bytes(),
        link: identity,
        connection_id: 41,
        connection_epoch: 97,
        claims_revision: 0,
        policy_epoch: 0,
        authorization_progress: 0,
        settled_through: 0,
    };
    let receipt = AuthorizationScopeReceipt {
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
        settled_through: GlobalTime(0),
        authorization_progress: 0,
    };
    assert!(authorization_scope_receipt_matches_transport_context(
        &receipt,
        expected,
        Some(GlobalTime(0)),
    ));
    assert!(
        !authorization_scope_receipt_matches_transport_context(
            &AuthorizationScopeReceipt {
                authority: *client_node.as_bytes(),
                authority_epoch: 41,
                ..receipt.clone()
            },
            expected,
            Some(GlobalTime(0)),
        ),
        "a receipt from the opposite duplex endpoint must not cross-wire"
    );

    let (reconnected_client, reconnected_server) =
        duplex_with_admitted_session_context(identity, client_node, 42, server_node, 98);
    let reconnect = crate::db::block_on(client.connect_upstream(reconnected_client));
    let resumed = server.accept_subscriber(reconnected_server, identity);
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
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let mallory = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let owned = server
        .insert("todos", cells("secret", false, alice))
        .unwrap()
        .row_uuid();
    let alice_client = open_db(0xa1, alice, &schema);
    alice_client.set_test_provider_claims(alice, test_provider_claims(alice));
    let (alice_transport, alice_server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _alice_upstream = crate::db::block_on(alice_client.connect_upstream(alice_transport));
    let _alice_subscriber = server.accept_subscriber(alice_server_transport, alice);
    let alice_advice = alice_client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: owned,
    });

    let mallory_client = open_db(0xb2, mallory, &schema);
    mallory_client.set_test_provider_claims(mallory, test_provider_claims(mallory));
    let (mallory_transport, mallory_server_transport) = duplex_with_admitted_session_context(
        mallory,
        NodeUuid::from_bytes([0xb2; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let _mallory_upstream = crate::db::block_on(mallory_client.connect_upstream(mallory_transport));
    let _mallory_subscriber = server.accept_subscriber(mallory_server_transport, mallory);
    let mallory_advice = mallory_client.request_permission_advice(PermissionAdviceAction::Read {
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
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let allowed = server
        .insert("todos", cells("owned", false, alice))
        .unwrap()
        .row_uuid();
    let denied = server
        .insert(
            "todos",
            cells("other", false, AuthorSubject::for_test_bytes([0xb2; 16])),
        )
        .unwrap()
        .row_uuid();
    let client = open_db(0xa1, alice, &schema);
    client.set_test_provider_claims(alice, test_provider_claims(alice));
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, alice);

    let first = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: allowed,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(first), PermissionAdvice::Allowed);

    let second = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: denied,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(second), PermissionAdvice::Denied);

    let hydration_count = match &subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState {
            authority_scope_hydration_count,
            ..
        }) => *authority_scope_hydration_count,
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        hydration_count, 1,
        "candidate rows must share the compiled authority support hydration"
    );
}

/// This stays at the peer/transport seam because the public advice future
/// cannot hold an authority's completed proof between its wire receipt and
/// the local callback. It proves that the request owns the claims it observed
/// when it was issued: advancing the same author's ambient claims must retire
/// the old receipt, ignore its A-only support, and make the caller issue a
/// fresh B-bound request rather than combining those contexts.
#[test]
fn scope_receipt_claim_transition_ignores_late_a_support_and_requires_fresh_b_request() {
    let schema = owner_read_schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let replacement_subject = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let target = server
        .insert("todos", cells("owned-by-a", false, author))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa1, author, &schema);
    let a_claims = test_provider_claims(author);
    let b_claims = test_provider_claims(replacement_subject);
    client.set_test_provider_claims(author, a_claims);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, author);

    let cancelled = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(0xa2),
    });
    let live = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();

    // The authority has now queued both A-bound proofs, but the client has
    // deliberately not consumed either receipt yet.
    drop(cancelled);
    client.set_test_provider_claims(author, b_claims.clone());
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(b_claims);
    client.tick().unwrap();

    let mut retried_live = false;
    loop {
        let Some(message) = try_recv_subscriber_payload(subscriber.borrow_mut().transport.as_mut())
        else {
            break;
        };
        if let SyncMessage::AuthorizationScopeIntent { request_id, action } = message {
            assert_eq!(
                action,
                PermissionAdviceAction::Read {
                    table: "todos".to_owned(),
                    row: target,
                },
                "the cancelled request's late A receipt must not revive or retry it"
            );
            let _ = request_id;
            retried_live = true;
        }
    }
    assert!(
        !retried_live,
        "neither the cancelled nor the claim-transitioned A request may retry under B"
    );
    assert_eq!(
        block_on(live),
        PermissionAdvice::Unknown,
        "a B-context receipt cannot settle the retired A request"
    );
    assert!(
        prepared_read(&client, &Query::from("todos")).is_empty(),
        "late A-scoped support must not materialize for the B session"
    );

    let b_request = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        block_on(b_request),
        PermissionAdvice::Denied,
        "a deliberately fresh B request receives only B-shaped authorization"
    );
}

/// An authority-owned authorization support view retains the exact admitted
/// session snapshot while alice's claims change and the view is rehydrated.
///
/// ```text
/// alice ──scope intent──► authority ──support view (alice + claims)──► alice
///                                      │
///                                      └──claim revision──► fresh bound view
/// ```
///
/// This stays at the peer/transport seam because the opaque support
/// subscription is allocated by the authority rather than exposed by a public
/// client API. It proves the allocation records its immutable policy binding
/// before owner-loop maintenance can serve the view.
#[test]
fn authority_claim_revision_invalidates_cached_scope_and_rehydrates() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let target = server
        .insert("todos", cells("owned", false, alice))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa1, alice, &schema);
    client.set_test_provider_claims(alice, test_provider_claims(alice));
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, alice);

    let first = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(first), PermissionAdvice::Allowed);

    let refreshed_claims = BTreeMap::from([
        (
            crate::query::provider_claim_key("sub"),
            Value::Uuid(alice.test_uuid()),
        ),
        ("fresh".to_owned(), Value::Bool(true)),
    ]);
    // The client needs its own authenticated snapshot to evaluate the
    // authority-supplied support rows. The authority separately receives the
    // same refresh at its trusted connection-admission boundary; it must not
    // trust the client's queued SessionClaims frame.
    client.set_test_provider_claims(alice, refreshed_claims.clone());
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(refreshed_claims);
    server.tick().unwrap();
    client.tick().unwrap();

    let refreshed = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(refreshed), PermissionAdvice::Allowed);

    let advanced_claims = BTreeMap::from([
        (
            crate::query::provider_claim_key("sub"),
            Value::Uuid(alice.test_uuid()),
        ),
        ("fresh".to_owned(), Value::Bool(false)),
    ]);
    client.set_test_provider_claims(alice, advanced_claims.clone());
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(advanced_claims);
    server.tick().unwrap();
    client.tick().unwrap();
    let advanced = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: target,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(advanced), PermissionAdvice::Allowed);

    let hydration_count = match &subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState {
            authority_scope_hydration_count,
            ..
        }) => *authority_scope_hydration_count,
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        hydration_count, 3,
        "each 0→1→2 authority claim transition must reject stale evidence and rehydrate"
    );
}

/// A host-authenticated claim refresh replaces a direct subscription's policy
/// snapshot, without borrowing a same-subject sibling's snapshot.
///
/// ```text
/// alice/A ──direct view──► Core ──A-owned rows
/// alice/B ──direct view──► Core ──B-owned rows
/// alice/A refreshes to none ─────► Core ──no rows
/// ```
///
/// The server's legacy author map intentionally ends at B in this setup. The
/// refreshed A link must use its own new snapshot, while the independent B
/// link remains visible. Deleting the direct-origin replacement in
/// `rebind_subscriber_views_after_claim_change` leaves A on its stale rows and
/// makes this test fail.
#[test]
fn direct_subscription_claim_refresh_replaces_membership_without_touching_same_subject_sibling() {
    let schema = owner_read_schema();
    let session_subject = AuthorSubject::for_test_bytes([0xa1; 16]);
    let a_owner = AuthorSubject::for_test_bytes([0xb1; 16]);
    let b_owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let a_row = server
        .insert("todos", cells("owned by A", false, a_owner))
        .unwrap()
        .row_uuid();
    let b_row = server
        .insert("todos", cells("owned by B", false, b_owner))
        .unwrap()
        .row_uuid();

    let a_claims = test_provider_claims(a_owner);
    let b_claims = test_provider_claims(b_owner);
    let no_access_claims = test_provider_claims(AuthorSubject::for_test_bytes([0xb3; 16]));
    let a_client = open_db(0xa1, session_subject, &schema);
    let b_client = open_db(0xa2, session_subject, &schema);
    a_client.set_test_provider_claims(session_subject, a_claims.clone());
    b_client.set_test_provider_claims(session_subject, b_claims.clone());
    let (a_transport, a_server_transport) = duplex();
    let a_upstream = crate::db::block_on(a_client.connect_upstream(a_transport));
    let a_subscriber =
        server.accept_subscriber_with_claims(a_server_transport, session_subject, a_claims);
    let (b_transport, b_server_transport) = duplex();
    let b_upstream = crate::db::block_on(b_client.connect_upstream(b_transport));
    let _b_subscriber =
        server.accept_subscriber_with_claims(b_server_transport, session_subject, b_claims);

    let query = Query::from("todos");
    let a_prepared = prepared(&a_client, &query);
    let b_prepared = prepared(&b_client, &query);
    let a_attachment = a_client
        .attach_query_with_opts(&a_prepared, global_subscribe_opts())
        .unwrap();
    let b_attachment = b_client
        .attach_query_with_opts(&b_prepared, global_subscribe_opts())
        .unwrap();
    for _ in 0..64 {
        a_client.tick().unwrap();
        b_client.tick().unwrap();
        server.tick().unwrap();
        a_upstream.borrow_mut().tick().unwrap();
        b_upstream.borrow_mut().tick().unwrap();
        if a_client.query_attachment_is_covered(&a_attachment)
            && b_client.query_attachment_is_covered(&b_attachment)
        {
            break;
        }
    }
    assert!(a_client.query_attachment_is_covered(&a_attachment));
    assert!(b_client.query_attachment_is_covered(&b_attachment));
    assert_eq!(
        row_ids(&prepared_all(&a_client, &query, global_subscribe_opts())),
        vec![a_row]
    );
    assert_eq!(
        row_ids(&prepared_all(&b_client, &query, global_subscribe_opts())),
        vec![b_row]
    );

    // Keep the consumer and trusted serving link in the same newly admitted
    // state. The B sibling remains live under B while A becomes unprivileged.
    a_client.set_test_provider_claims(session_subject, no_access_claims.clone());
    a_subscriber
        .borrow_mut()
        .update_authenticated_session_claims(no_access_claims);
    for _ in 0..64 {
        a_client.tick().unwrap();
        server.tick().unwrap();
        a_subscriber.borrow_mut().tick().unwrap();
        a_upstream.borrow_mut().tick().unwrap();
        if prepared_all(&a_client, &query, global_subscribe_opts()).is_empty() {
            break;
        }
    }
    assert!(
        prepared_all(&a_client, &query, global_subscribe_opts()).is_empty(),
        "the refreshed direct subscriber must lose A's stale membership"
    );
    assert_eq!(
        row_ids(&prepared_all(&b_client, &query, global_subscribe_opts())),
        vec![b_row],
        "one same-subject connection cannot rewrite its sibling's binding"
    );
}

/// Whole-table current-row serving is also a maintained, policy-bound usage
/// site. Refreshing direct admission must discard its retained receiver and
/// emit a fresh reset, rather than letting its normal delta path retain the
/// old session's rows.
#[test]
fn direct_current_rows_claim_refresh_reopens_under_new_binding() {
    let schema = owner_read_schema();
    let session_subject = AuthorSubject::for_test_bytes([0xa1; 16]);
    let allowed_owner = AuthorSubject::for_test_bytes([0xb1; 16]);
    let denied_owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    server
        .insert("todos", cells("A only", false, allowed_owner))
        .unwrap();
    let client = open_db(0xa1, session_subject, &schema);
    let allowed_claims =
        BTreeMap::from([("sub".to_owned(), Value::Uuid(allowed_owner.test_uuid()))]);
    let denied_claims = BTreeMap::from([("sub".to_owned(), Value::Uuid(denied_owner.test_uuid()))]);
    client.set_test_provider_claims(session_subject, allowed_claims.clone());
    let (client_transport, server_transport, _client_sent, server_sent) = duplex_with_taps();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber =
        server.accept_subscriber_with_claims(server_transport, session_subject, allowed_claims);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let _attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    subscriber.borrow_mut().serve_current_rows("todos").unwrap();
    client.tick().unwrap();

    client.set_test_provider_claims(session_subject, denied_claims.clone());
    let expected_denied_claims = denied_claims.clone();
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(denied_claims);
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        server_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(update)
                    if update.reset_result_set
                        && update.result_member_adds.is_empty()
                        && update.result_member_removes.is_empty()
            )
        }),
        "claim refresh must publish an empty current-row reset"
    );
    let ConnectionLink::Subscriber(state) = &subscriber.borrow().link else {
        unreachable!("accepted client is served by a subscriber link")
    };
    let subscription = server
        .node()
        .borrow()
        .whole_table_subscription_key("todos")
        .unwrap();
    let served = state
        .served_current_rows
        .get(&subscription)
        .expect("current-row usage stays registered");
    assert_eq!(served.policy_binding.1, expected_denied_claims);
    assert_eq!(
        served.policy_binding_origin,
        CoveragePolicyBindingOrigin::DirectAdmitted
    );
}

/// A delegated usage site is immutable even when its trusted relay transport
/// refreshes its own admitted snapshot. This deliberately delegates SYSTEM:
/// provenance comes from the admitted wire form, not identity equality.
#[test]
fn delegated_subscription_binding_survives_relay_claim_refresh() {
    let schema = owner_read_schema();
    let delegated_identity = AuthorSubject::SYSTEM;
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let delegated_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::Uuid(AuthorSubject::for_test_bytes([0xb1; 16]).test_uuid()),
    )]);
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_subscriber_with_claims_and_trust(
        server_transport,
        AuthorSubject::SYSTEM,
        BTreeMap::new(),
        CommitUnitTrust::TrustedBackend,
    );
    relay_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    relay_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: Some(crate::protocol::DelegatedSessionBinding {
                identity: delegated_identity,
                claims: delegated_claims.clone(),
            }),
        }))
        .unwrap();
    for _ in 0..8 {
        subscriber.borrow_mut().tick().unwrap();
    }
    let coverage = {
        let connection = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!("the core connection serves the trusted relay")
        };
        state.served[&subscription].clone()
    };
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(BTreeMap::from([(
            "relay_refresh".to_owned(),
            Value::Bool(true),
        )]));
    subscriber.borrow_mut().tick().unwrap();
    let connection = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!("the core connection remains a subscriber link")
    };
    let group = &state.coverage_groups[&coverage];
    assert_eq!(
        group.policy_binding_origin,
        CoveragePolicyBindingOrigin::Delegated
    );
    assert_eq!(
        group.policy_binding,
        (delegated_identity, delegated_claims.clone())
    );
    assert_eq!(
        state.peer.subscription_policy_binding(subscription),
        Some((delegated_identity, delegated_claims)),
        "refreshing the relay transport must not retarget a delegated usage site"
    );
}

/// Closing a subscriber must retire the group-owned maintained receiver, not
/// merely its concrete wire usage. Direct coverage uses the ordinary binding
/// key, while a trusted relay's delegated coverage is policy-partitioned; both
/// ownership forms must leave no Groove work behind after normal detach.
#[test]
fn subscriber_disconnect_retires_direct_and_delegated_coverage_receivers() {
    let direct_schema = schema();
    let direct_identity = AuthorSubject::for_test_bytes([0xc1; 16]);
    let direct_server = open_core(0x5e, AuthorSubject::SYSTEM, &direct_schema);
    let direct_client = open_db(0xc1, direct_identity, &direct_schema);
    let direct_baseline = direct_server
        .node()
        .borrow()
        .runtime_stats_for_test()
        .active_subscriptions;
    let (direct_client_transport, direct_server_transport) = duplex();
    let _direct_upstream =
        crate::db::block_on(direct_client.connect_upstream(direct_client_transport));
    let direct_subscriber =
        direct_server.accept_subscriber(direct_server_transport, direct_identity);
    let direct_query = Query::from("todos");
    let direct_prepared = prepared(&direct_client, &direct_query);
    let direct_attachment = direct_client
        .attach_query_with_opts(&direct_prepared, global_subscribe_opts())
        .unwrap();
    for _ in 0..8 {
        direct_client.tick().unwrap();
        direct_server.tick().unwrap();
        direct_client.tick().unwrap();
    }
    let direct_maintained = {
        let connection = direct_subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!("direct client is served by a subscriber link")
        };
        let coverage = &state.served[&direct_attachment.subscription()];
        let maintained = coverage_group_subscription_key(coverage);
        assert!(
            coverage.policy_binding.is_none(),
            "direct coverage is not policy-partitioned"
        );
        assert_eq!(
            maintained,
            SubscriptionKey {
                shape_id: coverage.shape_id,
                binding_id: coverage.binding_id,
                read_view: coverage.opts.read_view_key(),
            },
            "ordinary direct coverage uses the unpartitioned maintained key"
        );
        assert!(state.peer.has_maintained_subscription(maintained));
        maintained
    };
    assert_eq!(
        direct_server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        direct_baseline + 1,
        "the direct coverage group owns one maintained Groove receiver"
    );
    assert!(direct_server.server.detach_connection(&direct_subscriber));
    assert!(matches!(
        &direct_subscriber.borrow().link,
        ConnectionLink::Subscriber(state)
            if !state.peer.has_maintained_subscription(direct_maintained)
    ));
    assert_eq!(
        direct_server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        direct_baseline,
        "direct detach must retire maintained receiver {direct_maintained:?}"
    );

    let delegated_schema = owner_read_schema();
    let delegated_identity = AuthorSubject::SYSTEM;
    let delegated_server = open_core(0x6e, AuthorSubject::SYSTEM, &delegated_schema);
    let delegated_baseline = delegated_server
        .node()
        .borrow()
        .runtime_stats_for_test()
        .active_subscriptions;
    let shape = Query::from("todos").validate(&delegated_schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let delegated_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::Uuid(AuthorSubject::for_test_bytes([0xb1; 16]).test_uuid()),
    )]);
    let (mut relay_transport, delegated_server_transport) = duplex();
    let delegated_subscriber = delegated_server
        .server
        .accept_subscriber_with_claims_and_trust(
            delegated_server_transport,
            AuthorSubject::SYSTEM,
            BTreeMap::new(),
            CommitUnitTrust::TrustedBackend,
        );
    relay_transport
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts: RegisterShapeOptions::default(),
        })
        .unwrap();
    relay_transport
        .send(SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription,
            values: Vec::new(),
            known_state: None,
            delegated_session: Some(crate::protocol::DelegatedSessionBinding {
                identity: delegated_identity,
                claims: delegated_claims,
            }),
        }))
        .unwrap();
    for _ in 0..8 {
        delegated_subscriber.borrow_mut().tick().unwrap();
    }
    let delegated_maintained = {
        let connection = delegated_subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!("trusted relay is served by a subscriber link")
        };
        let coverage = &state.served[&subscription];
        let maintained = coverage_group_subscription_key(coverage);
        assert_ne!(
            maintained, subscription,
            "delegated policy coverage must use an isolated maintained key"
        );
        assert!(state.peer.has_maintained_subscription(maintained));
        maintained
    };
    assert_eq!(
        delegated_server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        delegated_baseline + 1,
        "the delegated coverage group owns one maintained Groove receiver"
    );
    assert!(
        delegated_server
            .server
            .detach_connection(&delegated_subscriber)
    );
    assert!(matches!(
        &delegated_subscriber.borrow().link,
        ConnectionLink::Subscriber(state)
            if !state.peer.has_maintained_subscription(delegated_maintained)
    ));
    assert_eq!(
        delegated_server
            .node()
            .borrow()
            .runtime_stats_for_test()
            .active_subscriptions,
        delegated_baseline,
        "delegated detach must retire maintained receiver {delegated_maintained:?}"
    );
}

/// A direct session served by an Edge must replace its propagated, delegated
/// Core usage site on refresh. Rebinding only the Edge-local evaluator makes a
/// broader session permanently miss Core-only rows; retaining the old handle
/// also leaves the old policy-bearing Core receiver resident.
#[test]
fn direct_claim_refresh_replaces_relay_upstream_usage_and_remote_membership() {
    let schema = owner_read_schema();
    let session_subject = AuthorSubject::for_test_bytes([0xa1; 16]);
    let allowed_owner = AuthorSubject::for_test_bytes([0xb1; 16]);
    let denied_owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let remote_row = core
        .insert("todos", cells("only at core", false, allowed_owner))
        .unwrap()
        .row_uuid();
    let edge = open_db(0xe1, AuthorSubject::SYSTEM, &schema);
    edge.set_relay_authority_session_owner();
    let client = open_db(0xc1, session_subject, &schema);
    let allowed_claims = test_provider_claims(allowed_owner);
    let denied_claims = test_provider_claims(denied_owner);
    client.set_test_provider_claims(session_subject, allowed_claims.clone());

    let (edge_transport, core_transport) = duplex();
    let _edge_upstream = crate::db::block_on(edge.connect_upstream(edge_transport));
    let core_edge = core.accept_subscriber_with_trust(
        core_transport,
        AuthorSubject::SYSTEM,
        CommitUnitTrust::TrustedBackend,
    );
    let (client_transport, edge_client_transport, _client_sent, edge_sent) = duplex_with_taps();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let edge_client =
        edge.accept_subscriber_with_claims(edge_client_transport, session_subject, allowed_claims);

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    for _ in 0..96 {
        client.tick().unwrap();
        edge.tick().unwrap();
        core.tick().unwrap();
        edge.tick().unwrap();
        client.tick().unwrap();
        if client.query_attachment_is_covered(&attachment)
            && row_ids(&prepared_all(&client, &query, global_subscribe_opts())) == vec![remote_row]
        {
            break;
        }
    }
    assert_eq!(
        row_ids(&prepared_all(&client, &query, global_subscribe_opts())),
        vec![remote_row]
    );
    let (downstream_subscription, old_upstream_subscription, old_maintained_subscription) = {
        let connection = edge_client.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!("edge serves the direct client")
        };
        let downstream_subscription = attachment.subscription();
        let coverage = &state.served[&downstream_subscription];
        (
            downstream_subscription,
            state.coverage_groups[coverage].upstream_subscription,
            coverage_group_subscription_key(coverage),
        )
    };
    assert!(matches!(
        &core_edge.borrow().link,
        ConnectionLink::Subscriber(state) if state.served.contains_key(&old_upstream_subscription)
    ));
    assert!(matches!(
        &edge_client.borrow().link,
        ConnectionLink::Subscriber(state) if state.peer.has_maintained_subscription(old_maintained_subscription)
    ));

    client.set_test_provider_claims(session_subject, denied_claims.clone());
    edge_client
        .borrow_mut()
        .update_authenticated_session_claims(denied_claims);
    let mut saw_fresh_downstream_reset = false;
    for _ in 0..96 {
        client.tick().unwrap();
        edge.tick().unwrap();
        core.tick().unwrap();
        edge.tick().unwrap();
        saw_fresh_downstream_reset |= edge_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(update)
                    if update.subscription == downstream_subscription
                        && update.reset_result_set
                        && update.result_member_adds.is_empty()
                        && update.result_member_removes.is_empty()
            )
        });
        client.tick().unwrap();
        if saw_fresh_downstream_reset
            && !matches!(
                &core_edge.borrow().link,
                ConnectionLink::Subscriber(state) if state.served.contains_key(&old_upstream_subscription)
            )
        {
            break;
        }
    }
    let connection = edge_client.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!("edge keeps serving the direct client")
    };
    let coverage = &state.served[&downstream_subscription];
    let fresh_upstream_subscription = state.coverage_groups[coverage].upstream_subscription;
    drop(connection);
    let core_fresh = match &core_edge.borrow().link {
        ConnectionLink::Subscriber(state) => (
            state
                .peer
                .subscription_policy_binding(fresh_upstream_subscription),
            state
                .peer
                .subscription_result_sets(fresh_upstream_subscription),
        ),
        ConnectionLink::Upstream(_) => (None, None),
    };
    assert_ne!(fresh_upstream_subscription, old_upstream_subscription);
    assert!(
        saw_fresh_downstream_reset,
        "the refreshed remote policy must publish a new empty membership reset"
    );
    assert!(matches!(
        &core_edge.borrow().link,
        ConnectionLink::Subscriber(state)
            if !state.served.contains_key(&old_upstream_subscription)
                && state.served.contains_key(&fresh_upstream_subscription)
    ));
    assert!(
        matches!(
            &edge_client.borrow().link,
            ConnectionLink::Subscriber(state)
                if !state.peer.has_maintained_subscription(old_maintained_subscription)
        ),
        "claim refresh must retire the old policy-bound maintained receiver"
    );
    assert_eq!(
        core_fresh.1,
        Some(BTreeSet::new()),
        "the fresh Core usage must have an empty B-bound result set"
    );
}

#[test]
fn terminal_core_write_fates_prove_exact_insert_update_and_delete_actions() {
    let schema = owner_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    // A Core may also maintain an upstream relay; that topology fact must not
    // turn its client ingress into Edge routing or bypass local proof.
    let (core_upstream, _upstream_peer) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0x5e; 16]),
        9,
        NodeUuid::from_bytes([0xc0; 16]),
        9,
    );
    let _core_upstream = crate::db::block_on(server.server.connect_upstream(core_upstream));
    let client = open_db(0xa1, alice, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber = server.accept_subscriber(server_transport, alice);

    let inserted = client
        .insert("todos", cells("owned", false, alice), Default::default())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    // The previous-row policy may allow Alice, but the update-check candidate
    // switches ownership to Bob and must be denied by the terminal core.
    let changed_owner = client
        .update(
            "todos",
            inserted.row_uuid(),
            BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.test_uuid()))]),
            Default::default(),
        )
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(matches!(
        changed_owner.write_state().unwrap().fate,
        Fate::Rejected(_)
    ));

    let deleted = client
        .delete("todos", inserted.row_uuid(), Default::default())
        .unwrap();
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert!(matches!(
        deleted.write_state().unwrap().fate,
        Fate::Accepted
    ));

    let proofs = match &subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState { peer, .. }) => {
            peer.terminal_authority_scope_proof_count()
        }
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        proofs, 3,
        "production terminal fate admission must execute one exact aggregate proof per operation"
    );
}

/// A terminal support receiver belongs to one admitted link, even if another
/// live link authenticates the same author with different claims before that
/// receiver is first proved.
///
/// ```text
/// alice/A link ──admitted──► Core ──terminal proof──► A-bound support
///                                  ▲
/// alice/B link ──binds B───────────┘
/// ```
///
/// This targets the opaque terminal-support allocation rather than a public
/// subscription: its canonical query key is intentionally shared, while its
/// policy snapshot must not be selected from the node's author-keyed legacy
/// cache. Replacing the explicit A snapshot below with `session_claims_for`
/// makes the final assertion observe B and fail.
#[test]
fn terminal_commit_support_keeps_same_author_sibling_claim_snapshot() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let a_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("editor".to_owned()),
    )]);
    let b_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("viewer".to_owned()),
    )]);
    let (_a_transport, a_server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let a_subscriber =
        server.accept_subscriber_with_claims(a_server_transport, alice, a_claims.clone());
    let (_b_transport, b_server_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa2; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let b_subscriber =
        server.accept_subscriber_with_claims(b_server_transport, alice, b_claims.clone());
    b_subscriber
        .borrow_mut()
        .tick()
        .expect("the sibling link records its legacy compatibility claims");
    assert_eq!(
        server.node().borrow().session_claims_for(alice),
        b_claims,
        "this reproduces the author-keyed cache overwrite that terminal support must ignore"
    );

    let client = open_db(0xa1, alice, &schema);
    client.set_test_provider_claims(alice, a_claims.clone());
    let candidate_cells = cells("same-author sibling snapshot", false, alice);
    let write = client
        .insert("todos", candidate_cells.clone(), Default::default())
        .expect("A can prepare its editor-authorized write");
    let SyncMessage::CommitUnit { tx, versions } = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .expect("prepared write retains a commit unit")
    else {
        panic!("prepared mergeable write must produce one commit unit");
    };
    let scope = server
        .node()
        .borrow()
        .authorization_support_scope(
            alice,
            &PermissionAdviceAction::Insert {
                table: "todos".to_owned(),
                cells: candidate_cells,
            },
        )
        .expect("editor policy has a support clause");
    let (shape, binding) = scope
        .subscriptions
        .into_iter()
        .next()
        .expect("editor policy produces one support subscription");
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: scope.options.read_view_key(),
    };

    {
        let mut a_connection = a_subscriber.borrow_mut();
        let ConnectionLink::Subscriber(a_state) = &mut a_connection.link else {
            unreachable!("A is an admitted subscriber link");
        };
        crate::db::block_on(a_state.peer.prove_terminal_commit_authorization(
            &mut server.node().borrow_mut(),
            alice,
            a_state.session_claims.clone(),
            &versions,
            tx.tx_id,
        ))
        .expect("A terminal proof remains valid after B updates the legacy cache");
        assert_eq!(
            a_state.peer.subscription_policy_binding(subscription),
            Some((alice, a_claims.clone())),
            "the maintained terminal support receiver retains A rather than B's sibling snapshot"
        );
    }

    // 0→1→2 authenticated refreshes reuse the same canonical support key,
    // but each must replace its maintained receiver before terminal proof.
    a_subscriber
        .borrow_mut()
        .update_authenticated_session_claims(b_claims.clone());
    {
        let mut a_connection = a_subscriber.borrow_mut();
        let ConnectionLink::Subscriber(a_state) = &mut a_connection.link else {
            unreachable!("A remains an admitted subscriber link");
        };
        crate::db::block_on(a_state.peer.prove_terminal_commit_authorization(
            &mut server.node().borrow_mut(),
            alice,
            a_state.session_claims.clone(),
            &versions,
            tx.tx_id,
        ))
        .expect("a refreshed terminal proof replaces the stale support receiver");
        assert_eq!(
            a_state.peer.subscription_policy_binding(subscription),
            Some((alice, b_claims)),
            "terminal support reuse is keyed by exact immutable claims, not just its query key"
        );
    }
    a_subscriber
        .borrow_mut()
        .update_authenticated_session_claims(a_claims.clone());
    {
        let mut a_connection = a_subscriber.borrow_mut();
        let ConnectionLink::Subscriber(a_state) = &mut a_connection.link else {
            unreachable!("A remains an admitted subscriber link");
        };
        crate::db::block_on(a_state.peer.prove_terminal_commit_authorization(
            &mut server.node().borrow_mut(),
            alice,
            a_state.session_claims.clone(),
            &versions,
            tx.tx_id,
        ))
        .expect("the next refreshed terminal proof replaces the stale support receiver");
        assert_eq!(
            a_state.peer.subscription_policy_binding(subscription),
            Some((alice, a_claims)),
            "each claim revision receives a fresh terminal support receiver"
        );
    }
}

/// Edge client ingress uses the same action-specific authority proof as a
/// terminal Core, but exposes only Edge durability until an admitted upstream
/// later reports Global.  In particular, this exercises the production
/// connection loop rather than calling `PeerState`'s focused proof helpers.
#[test]
fn edge_client_ingress_proves_actions_before_one_routed_edge_fate() {
    let schema = owner_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb2; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, alice, &schema);
    let (client_transport, edge_transport, _client_sent, edge_sent) = duplex_with_taps();
    let client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            alice,
            test_provider_claims(alice),
            CommitUnitTrust::Session,
        );

    let inserted = client
        .insert(
            "todos",
            cells("edge-owned", false, alice),
            Default::default(),
        )
        .unwrap();
    let inserted_tx = inserted.mergeable_tx_id();
    let sibling = client
        .insert(
            "todos",
            cells("edge-owned-sibling", false, alice),
            Default::default(),
        )
        .unwrap();
    let sibling_tx = sibling.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();

    let edge_acceptances = edge_sent
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::FateUpdate {
                    tx_id: candidate,
                    fate: Fate::Accepted,
                    durability: Some(DurabilityTier::Edge),
                    ..
                } if *candidate == inserted_tx || *candidate == sibling_tx
            )
        })
        .count();
    assert_eq!(
        edge_acceptances, 2,
        "each queued commit makes fair progress to one routed edge fate"
    );
    client.tick().unwrap();
    assert_eq!(inserted.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        inserted.write_state().unwrap().durability,
        DurabilityTier::Edge
    );
    assert_eq!(sibling.write_state().unwrap().fate, Fate::Accepted);
    assert_eq!(
        sibling.write_state().unwrap().durability,
        DurabilityTier::Edge
    );
    assert!(
        [inserted_tx, sibling_tx].into_iter().all(|tx_id| edge
            .server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id)),
        "only edge-admitted commits enter the Core upload outbox"
    );

    // A transport retry cannot re-publish the Edge acceptance.  The retained
    // route is still needed for Core's later terminal fate, but remembers its
    // per-client edge acknowledgement.
    client_upstream
        .borrow_mut()
        .transport
        .send(
            client
                .node
                .node
                .borrow_mut()
                .commit_unit_for(inserted_tx)
                .unwrap(),
        )
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge_sent.borrow().is_empty(),
        "a retransmit must not create a second edge acknowledgement"
    );

    // The exact update action includes the candidate patch. Alice is allowed
    // by the old-row policy, but changing owner to Bob fails the update check;
    // it produces one routed rejection instead of a synthetic Edge success.
    let denied = client
        .update(
            "todos",
            inserted.row_uuid(),
            BTreeMap::from([("owner".to_owned(), Value::Uuid(bob.test_uuid()))]),
            Default::default(),
        )
        .unwrap();
    let denied_tx = denied.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();
    let rejections = edge_sent
        .borrow()
        .iter()
        .filter(|message| {
            matches!(
                message,
                SyncMessage::FateUpdate {
                    tx_id,
                    fate: Fate::Rejected(_),
                    ..
                } if *tx_id == denied_tx
            )
        })
        .count();
    assert_eq!(
        rejections, 1,
        "denial is routed once through the edge route"
    );
    assert!(
        !edge
            .server
            .edge_fate_routes
            .borrow()
            .contains_key(&denied_tx),
        "a locally denied upload cannot leave a Core-fate route behind"
    );
    assert!(
        !edge
            .server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == denied_tx),
        "a denied upload must not bypass edge authorization through Core replication"
    );
}

/// An edge route is server-owned rather than connection-owned: a reconnecting
/// client may retransmit its exact unit and receive the already-known edge
/// acceptance, but another connection cannot replace a still-live obligation
/// with different bytes for the same transaction id.
///
/// This stays at the served-peer seam because two authenticated subscriber
/// connections and their in-memory downstream queues are the boundary where
/// the otherwise durable transaction identity is deliberately absent while an
/// edge fate route is live.
#[test]
fn edge_fate_route_identity_is_shared_across_client_connections() {
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client = open_db(0xa1, alice, &schema);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("identity".to_owned()))]),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    let SyncMessage::CommitUnit { tx, versions } = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(tx_id)
        .unwrap()
    else {
        panic!("client mergeable write must retain its commit unit");
    };

    let node = edge.node();
    let mut first = PeerState::edge_client(alice);
    let mut reconnect = PeerState::edge_client(alice);
    let authority: Rc<RefCell<Option<AuthorityContext>>> = Rc::new(RefCell::new(None));
    let local_routes: LocalFateRoutes = Rc::new(RefCell::new(BTreeMap::new()));
    let first_fates = Rc::new(RefCell::new(Vec::new()));
    let reconnect_fates = Rc::new(RefCell::new(Vec::new()));
    let context = CommitUnitIngestContext {
        identity: alice,
        trust: CommitUnitTrust::Session,
        edge_authority: true,
    };

    let first_outcome = crate::db::block_on(dispatch_admitted_subscriber_message(
        &node,
        &mut first,
        false,
        context,
        (alice, BTreeMap::new()),
        &authority,
        &edge.server.edge_fate_routes,
        &local_routes,
        &first_fates,
        1,
        SyncMessage::CommitUnit {
            tx: tx.clone(),
            versions: versions.clone(),
        },
    ))
    .expect("first client-link upload is admitted");
    assert!(first_outcome.value.is_empty());
    assert!(matches!(
        first_fates.borrow().as_slice(),
        [SyncMessage::FateUpdate {
            tx_id: candidate,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Edge),
            ..
        }] if *candidate == tx_id
    ));

    let mut conflicting = tx.clone();
    conflicting.n_total_writes = conflicting.n_total_writes.saturating_add(1);
    assert!(
        crate::db::block_on(dispatch_admitted_subscriber_message(
            &node,
            &mut reconnect,
            false,
            context,
            (alice, BTreeMap::new()),
            &authority,
            &edge.server.edge_fate_routes,
            &local_routes,
            &reconnect_fates,
            1,
            SyncMessage::CommitUnit {
                tx: conflicting,
                versions: versions.clone(),
            },
        ))
        .is_err()
    );
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id].routes.len(),
        1,
        "a conflicting second connection is rejected before it gains a fate route"
    );
    assert!(reconnect_fates.borrow().is_empty());

    let retry_outcome = crate::db::block_on(dispatch_admitted_subscriber_message(
        &node,
        &mut reconnect,
        false,
        context,
        (alice, BTreeMap::new()),
        &authority,
        &edge.server.edge_fate_routes,
        &local_routes,
        &reconnect_fates,
        2,
        SyncMessage::CommitUnit { tx, versions },
    ))
    .expect("an exact reconnect retransmit reuses the route obligation");
    assert!(retry_outcome.value.is_empty());
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id].routes.len(),
        2,
        "the reconnect gets one route without replacing the original obligation"
    );
    assert_eq!(
        first_fates.borrow().len(),
        1,
        "an exact reconnect retransmit must not duplicate the old session's edge fate"
    );
    assert!(matches!(
        reconnect_fates.borrow().as_slice(),
        [SyncMessage::FateUpdate {
            tx_id: candidate,
            fate: Fate::Accepted,
            durability: Some(DurabilityTier::Edge),
            ..
        }] if *candidate == tx_id
    ));
}

#[test]
fn concurrent_upstreams_keep_selected_owner_until_detach_handoff() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let (a_transport, _a_peer) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let a = crate::db::block_on(edge.server.connect_upstream(a_transport));
    let first = *edge.server.admitted_upstream_authority.borrow();
    let (b_transport, _b_peer) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        11,
        NodeUuid::from_bytes([0xb2; 16]),
        21,
    );
    let _b = crate::db::block_on(edge.server.connect_upstream(b_transport));
    assert_eq!(
        *edge.server.admitted_upstream_authority.borrow(),
        first,
        "a concurrent admitted upstream must not steal existing route ownership"
    );
    assert_eq!(edge.server.admitted_upstream_authorities.borrow().len(), 2);
    let tx_id = edge
        .node()
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x91), 1).cells(cells("handoff", false, identity)),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } =
        edge.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        panic!("settled mergeable write must retain its commit unit");
    };
    let queue = Rc::new(RefCell::new(Vec::new()));
    edge.server.edge_fate_routes.borrow_mut().insert(
        tx_id,
        EdgeFateObligation {
            identity: EdgeFateCommitIdentity::new(&tx, &versions),
            routes: vec![EdgeFateRoute {
                authority: Some(first.unwrap()),
                queue: Rc::downgrade(&queue),
                edge_acknowledged: false,
            }],
        },
    );
    assert!(edge.server.detach_connection(&a));
    assert_ne!(
        *edge.server.admitted_upstream_authority.borrow(),
        first,
        "detaching the selected owner must deterministically hand off future routes"
    );
    let handoff = edge.server.admitted_upstream_authority.borrow().unwrap();
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id].routes[0].authority,
        Some(handoff),
        "an Edge-Accepted caller route must follow the selected handoff rather than vanish"
    );
}

#[test]
fn edge_route_capacity_rejects_instead_of_reporting_edge_acceptance() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let (upstream, _authority) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xe0; 16]),
        1,
        NodeUuid::from_bytes([0xc0; 16]),
        1,
    );
    let _upstream = crate::db::block_on(edge.server.connect_upstream(upstream));
    let selected = edge
        .server
        .admitted_upstream_authority
        .borrow()
        .expect("admitted upstream");

    let client = open_db(0xa1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0xe0; 16]),
        2,
    );
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            identity,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );
    let write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("bounded".to_owned()))]),
            Default::default(),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(write.mergeable_tx_id())
        .unwrap()
    else {
        panic!("client mergeable write must retain its commit unit");
    };
    let queue = Rc::new(RefCell::new(Vec::new()));
    edge.server.edge_fate_routes.borrow_mut().insert(
        write.mergeable_tx_id(),
        EdgeFateObligation {
            identity: EdgeFateCommitIdentity::new(&tx, &versions),
            routes: (0..MAX_EDGE_FATE_ROUTES_PER_TX)
                .map(|_| EdgeFateRoute {
                    authority: Some(selected),
                    queue: Rc::downgrade(&queue),
                    edge_acknowledged: false,
                })
                .collect(),
        },
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
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let core_node = NodeUuid::from_bytes([0xc0; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);

    // The upstream endpoint is the authority that is allowed to discharge a
    // downstream Edge-accepted write. The client endpoint is deliberately a
    // different admitted session, so it cannot supply that authority context.
    let (edge_upstream_transport, core_transport) =
        duplex_with_admitted_session_context(AuthorSubject::SYSTEM, edge_node, 41, core_node, 97);
    let edge_upstream = crate::db::block_on(edge.server.connect_upstream(edge_upstream_transport));
    let core = open_core(0xc0, AuthorSubject::SYSTEM, &schema);
    let core_session = core.accept_subscriber(core_transport, AuthorSubject::SYSTEM);

    let client = open_db(0xa1, alice, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        11,
        edge_node,
        13,
    );
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            alice,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );

    let write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("routed".to_owned()))]),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();

    let expected_authority = AuthorityContext {
        authority: *core_node.as_bytes(),
        link: AuthorSubject::SYSTEM,
        connection_id: 41,
        connection_epoch: 97,
        claims_revision: 0,
        policy_epoch: 0,
        authorization_progress: 0,
        settled_through: 0,
    };
    let routes = edge.server.edge_fate_routes.borrow();
    let routes_for_tx = routes.get(&tx_id).expect("edge must park the upload route");
    assert_eq!(routes_for_tx.routes.len(), 1);
    assert_eq!(routes_for_tx.routes[0].authority, Some(expected_authority));
    drop(routes);

    // Scope receipts advance authorization metadata on the same physical
    // connection. They must not turn that admitted link into a different fate
    // authority: FateUpdate carries no receipt generation of its own.
    {
        let mut edge_upstream = edge_upstream.borrow_mut();
        let ConnectionLink::Upstream(UpstreamConnectionState {
            expected_scope_authority,
            ..
        }) = &mut edge_upstream.link
        else {
            panic!("edge upstream must retain its admitted authority context");
        };
        let authority_context = expected_scope_authority
            .as_mut()
            .expect("admitted authority context");
        authority_context.claims_revision = 3;
        authority_context.policy_epoch = 5;
        authority_context.authorization_progress = 7;
        authority_context.settled_through = 11;
    }

    let fate = SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Accepted,
        global_time: Some(GlobalTime(17)),
        durability: Some(DurabilityTier::Global),
    };

    // Receipt metadata is intentionally not a fate-route discriminator, but
    // every physical link discriminator still is. A FateUpdate from a
    // different epoch, local connection, authority, or admitted subject must
    // remain unable to discharge Alice's parked route.
    let advanced_context = {
        let edge_upstream = edge_upstream.borrow();
        let ConnectionLink::Upstream(UpstreamConnectionState {
            expected_scope_authority,
            ..
        }) = &edge_upstream.link
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
            link: AuthorSubject::for_test_bytes([0xb2; 16]),
            ..advanced_context
        },
    ] {
        {
            let mut edge_upstream = edge_upstream.borrow_mut();
            let ConnectionLink::Upstream(UpstreamConnectionState {
                expected_scope_authority,
                ..
            }) = &mut edge_upstream.link
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
                global_time: None,
                durability: None,
            })
            .unwrap();
        edge_upstream.borrow_mut().tick().unwrap();
        assert!(
            edge_client.borrow().downstream_fates.borrow().is_empty(),
            "a physically distinct authority context must not reach Alice"
        );
        assert_eq!(
            edge.node()
                .borrow_mut()
                .transaction_state_settled(tx_id)
                .unwrap(),
            (Fate::Accepted, None, DurabilityTier::Edge),
            "a rejected fate from a different physical link must not alter the edge-local admission"
        );
    }
    {
        let mut edge_upstream = edge_upstream.borrow_mut();
        let ConnectionLink::Upstream(UpstreamConnectionState {
            expected_scope_authority,
            ..
        }) = &mut edge_upstream.link
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
        std::slice::from_ref(&fate),
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
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let (a_transport, mut a_peer) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xe0; 16]),
        1,
        NodeUuid::from_bytes([0xa2; 16]),
        1,
    );
    let _a = crate::db::block_on(edge.server.connect_upstream(a_transport));
    let selected = edge.server.admitted_upstream_authority.borrow().unwrap();
    let (b_transport, mut b_peer) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xe0; 16]),
        2,
        NodeUuid::from_bytes([0xb2; 16]),
        2,
    );
    let _b = crate::db::block_on(edge.server.connect_upstream(b_transport));
    let tx_id = edge
        .node()
        .borrow_mut()
        .commit_mergeable_settled(
            MergeableCommit::new("todos", row(0x44), 1).cells(cells("pending", false, identity)),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } =
        edge.node().borrow_mut().commit_unit_for(tx_id).unwrap()
    else {
        panic!("settled mergeable write must retain its commit unit");
    };
    let downstream = Rc::new(RefCell::new(Vec::new()));
    edge.server.edge_fate_routes.borrow_mut().insert(
        tx_id,
        EdgeFateObligation {
            identity: EdgeFateCommitIdentity::new(&tx, &versions),
            routes: vec![EdgeFateRoute {
                authority: Some(selected),
                queue: Rc::downgrade(&downstream),
                edge_acknowledged: false,
            }],
        },
    );
    b_peer
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.server.tick().unwrap();
    assert!(matches!(
        edge.node()
            .borrow_mut()
            .transaction_state_settled(tx_id)
            .unwrap()
            .0,
        Fate::Pending
    ));
    assert!(downstream.borrow().is_empty());
    a_peer
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.server.tick().unwrap();
    assert!(matches!(
        edge.node()
            .borrow_mut()
            .transaction_state_settled(tx_id)
            .unwrap()
            .0,
        Fate::Accepted
    ));
    assert_eq!(downstream.borrow().len(), 1);
}

#[test]
fn edge_fate_handoff_redrives_real_downstream_write_and_ignores_old_authority() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let authority_a = open_core(0xa2, AuthorSubject::SYSTEM, &schema);
    let authority_b = open_core(0xb2, AuthorSubject::SYSTEM, &schema);
    let edge_node = NodeUuid::from_bytes([0xe0; 16]);

    let (edge_a_transport, a_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let edge_a = crate::db::block_on(edge.server.connect_upstream(edge_a_transport));
    let a = authority_a.accept_subscriber(a_transport, identity);
    let (edge_b_transport, b_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        11,
        NodeUuid::from_bytes([0xb2; 16]),
        21,
    );
    let edge_b = crate::db::block_on(edge.server.connect_upstream(edge_b_transport));
    let _b = authority_b.accept_subscriber(b_transport, identity);

    let client = open_db(0xc1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xc1; 16]),
        1,
        edge_node,
        2,
    );
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            identity,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );

    let write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("handoff".to_owned()))]),
            Default::default(),
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
        let edge_b = edge_b.borrow();
        let ConnectionLink::Upstream(UpstreamConnectionState { uploaded, .. }) = &edge_b.link
        else {
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
        let edge_b = edge_b.borrow();
        let ConnectionLink::Upstream(UpstreamConnectionState { uploaded, .. }) = &edge_b.link
        else {
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
            global_time: None,
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
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let authority_a = open_core(0xa2, AuthorSubject::SYSTEM, &schema);
    let edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let (edge_a_transport, a_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let edge_a = crate::db::block_on(edge.server.connect_upstream(edge_a_transport));
    let _a = authority_a.accept_subscriber(a_transport, identity);

    let client = open_db(0xc1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xc1; 16]),
        1,
        edge_node,
        2,
    );
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            identity,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );
    let write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("parked".to_owned()))]),
            Default::default(),
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
        edge.server.edge_fate_routes.borrow()[&write.mergeable_tx_id()].routes[0].authority,
        None,
        "a route whose authority disconnected remains parked without stale authority claims"
    );

    let authority_c = open_core(0xc2, AuthorSubject::SYSTEM, &schema);
    let (edge_c_transport, c_transport) = duplex_with_admitted_session_context(
        identity,
        edge_node,
        12,
        NodeUuid::from_bytes([0xc2; 16]),
        22,
    );
    let _edge_c = crate::db::block_on(edge.server.connect_upstream(edge_c_transport));
    let _c = authority_c.accept_subscriber(c_transport, identity);
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
    let schema = schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let core_node = NodeUuid::from_bytes([0xc0; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, alice, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        alice,
        NodeUuid::from_bytes([0xa1; 16]),
        11,
        edge_node,
        13,
    );
    let client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            alice,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );

    let write = client
        .insert(
            "todos",
            cells("startup race", false, alice),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    let canonical = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(tx_id)
        .unwrap();
    let mut reconstructed = canonical.clone();
    let SyncMessage::CommitUnit { versions, .. } = &mut reconstructed else {
        unreachable!("commit_unit_for returns a CommitUnit");
    };
    versions.clear();
    edge.server.outbox.borrow_mut().push(PendingUpload {
        tx_id,
        unit: Some(reconstructed),
    });
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge
    );
    assert_eq!(
        edge.server.edge_fate_routes.borrow()[&tx_id].routes[0].authority,
        None,
        "an offline-ready edge retains the downstream obligation without inventing authority"
    );
    let outbox = edge.server.outbox.borrow();
    let retained = outbox
        .iter()
        .find(|pending| pending.tx_id == tx_id)
        .expect("accepted Edge write remains queued for its future Core");
    assert_eq!(
        retained.unit.as_ref(),
        Some(&canonical),
        "the exact inbound unit must replace an earlier same-tx reconstruction"
    );
    drop(outbox);
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
        edge.server.edge_fate_routes.borrow()[&tx_id].routes.len(),
        1,
        "a retransmitted pre-admission unit must reuse the same downstream route"
    );

    let (edge_upstream_transport, core_transport) =
        duplex_with_admitted_session_context(AuthorSubject::SYSTEM, edge_node, 41, core_node, 97);
    let _edge_upstream = crate::db::block_on(edge.server.connect_upstream(edge_upstream_transport));
    assert!(
        edge.server.edge_fate_routes.borrow()[&tx_id].routes[0]
            .authority
            .is_some(),
        "the first authenticated authority binds the parked route"
    );
    let core = open_core(0xc0, AuthorSubject::SYSTEM, &schema);
    let core_session = core.accept_subscriber(core_transport, AuthorSubject::SYSTEM);
    edge.tick().unwrap();
    let uploaded = std::iter::from_fn(|| core_session.borrow_mut().transport.try_recv())
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
            global_time: Some(GlobalTime(1)),
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
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let edge = open_core(0xe0, AuthorSubject::SYSTEM, &schema);
    let edge_node = NodeUuid::from_bytes([0xe0; 16]);
    let authority_node = NodeUuid::from_bytes([0xa2; 16]);
    let old_authority = open_core(0xa2, AuthorSubject::SYSTEM, &schema);
    let current_authority = open_core(0xa2, AuthorSubject::SYSTEM, &schema);

    let (edge_old_transport, old_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 10, authority_node, 20);
    let _edge_old = crate::db::block_on(edge.server.connect_upstream(edge_old_transport));
    let old = old_authority.accept_subscriber(old_transport, identity);
    let (edge_current_transport, current_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 11, authority_node, 21);
    let _edge_current = crate::db::block_on(edge.server.connect_upstream(edge_current_transport));
    let current = current_authority.accept_subscriber(current_transport, identity);

    let client = open_db(0xc1, identity, &schema);
    let (client_transport, edge_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xc1; 16]),
        1,
        edge_node,
        2,
    );
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            identity,
            BTreeMap::new(),
            CommitUnitTrust::Session,
        );
    let write = client
        .insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String("epoch".to_owned()))]),
            Default::default(),
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
    let current_context = edge.server.admitted_upstream_authorities.borrow()[1];
    *edge.server.admitted_upstream_authority.borrow_mut() = Some(current_context);
    edge.server
        .edge_fate_routes
        .borrow_mut()
        .get_mut(&write.mergeable_tx_id())
        .expect("routed edge write")
        .routes[0]
        .authority = Some(current_context);
    old.borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id: write.mergeable_tx_id(),
            fate: Fate::Rejected(RejectionReason::MalformedCommit("old session".to_owned())),
            global_time: None,
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
            global_time: Some(GlobalTime(1)),
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
fn outbox_release_requires_current_admitted_authority_receipt() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa4; 16]);
    let edge_node = NodeUuid::from_bytes([0xe4; 16]);
    let authority_node = NodeUuid::from_bytes([0xa4; 16]);
    let edge = open_core(0xe4, AuthorSubject::SYSTEM, &schema);
    let current_authority = open_core(0xa4, AuthorSubject::SYSTEM, &schema);
    let (edge_current_transport, current_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 11, authority_node, 21);
    let edge_current = block_on(edge.server.connect_upstream(edge_current_transport));
    let current = current_authority.accept_subscriber(current_transport, identity);
    let old_authority = open_core(0xa4, AuthorSubject::SYSTEM, &schema);
    let (edge_old_transport, old_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 10, authority_node, 20);
    let edge_old = block_on(edge.server.connect_upstream(edge_old_transport));
    let old = old_authority.accept_subscriber(old_transport, identity);

    let client = open_db(0xc4, identity, &schema);
    let (client_transport, edge_transport) = duplex();
    let _client_upstream = block_on(client.connect_upstream(client_transport));
    let _edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            identity,
            BTreeMap::new(),
            CommitUnitTrust::TrustedBackend,
        );
    let write = client
        .insert(
            "todos",
            cells("authority receipt", false, identity),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(
        write.write_state().unwrap().durability,
        DurabilityTier::Edge,
        "an Edge trusted-backend session must not assign Global durability locally"
    );
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "local authority acceptance must retain the future upstream upload"
    );

    // The superseded connection advertises the same authority node UUID, so
    // physical admission epoch -- not just the node identity -- must guard
    // outbox release.
    old.borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "a direct terminal receipt from a superseded authority must not release the upload"
    );

    // These are real admitted authority frames, but neither is a terminal
    // Global acceptance: both must leave the canonical upload replayable.
    current
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Pending,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "a Pending/Global receipt without time must not release the upload"
    );

    current
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "an Accepted/Global receipt without time must not release the upload"
    );

    // Disconnect every previous authority. The new admitted session must be
    // sent the canonical unit again; if either nonterminal receipt had pruned
    // it, this reconnect would have no upload to retransmit.
    assert!(edge.server.detach_connection(&edge_current));
    assert!(edge.server.detach_connection(&edge_old));
    let reconnected_authority = open_core(0xa4, AuthorSubject::SYSTEM, &schema);
    let (edge_reconnected_transport, reconnected_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 12, authority_node, 22);
    let _edge_reconnected = block_on(edge.server.connect_upstream(edge_reconnected_transport));
    let reconnected = reconnected_authority.accept_subscriber(reconnected_transport, identity);
    edge.tick().unwrap();
    assert!(
        std::iter::from_fn(|| reconnected.borrow_mut().transport.try_recv()).any(
            |message| matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id)
        ),
        "the retained canonical upload must be retransmitted after authority reconnect"
    );

    reconnected
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(2)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .all(|pending| pending.tx_id != tx_id),
        "the current authority's time-bearing Global acceptance releases the upload"
    );
}

/// A routed Edge upload has no direct-receipt compatibility path: the frame
/// must identify the selected authenticated authority session before it can
/// settle or prune the upload.
#[test]
fn featureless_upstream_cannot_release_routed_edge_outbox() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa5; 16]);
    let edge_node = NodeUuid::from_bytes([0xe5; 16]);
    let authority_node = NodeUuid::from_bytes([0xa5; 16]);
    let edge = open_core(0xe5, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xc5, identity, &schema);
    let (client_transport, edge_transport) = duplex();
    let _client_upstream = block_on(client.connect_upstream(client_transport));
    let _edge_client = edge
        .server
        .accept_edge_authority_subscriber_with_claims_and_trust(
            edge_transport,
            identity,
            BTreeMap::new(),
            CommitUnitTrust::TrustedBackend,
        );
    let write = client
        .insert(
            "todos",
            cells("featureless receipt", false, identity),
            Default::default(),
        )
        .unwrap();
    let tx_id = write.mergeable_tx_id();
    client.tick().unwrap();
    edge.tick().unwrap();
    client.tick().unwrap();
    assert!(
        edge.server.edge_fate_routes.borrow().contains_key(&tx_id),
        "the edge-accepted client write must retain a routed authority obligation"
    );

    let (edge_featureless_transport, mut featureless_authority) = duplex();
    let _featureless = block_on(edge.server.connect_upstream(edge_featureless_transport));
    assert!(
        edge.server.admitted_upstream_authority.borrow().is_none(),
        "a duplex without session context must not become an admitted authority"
    );
    featureless_authority
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(1)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .any(|pending| pending.tx_id == tx_id),
        "a featureless authority frame must not prune a routed Edge upload"
    );

    let current_authority = open_core(0xa5, AuthorSubject::SYSTEM, &schema);
    let (edge_current_transport, current_transport) =
        duplex_with_admitted_session_context(identity, edge_node, 12, authority_node, 22);
    let _edge_current = block_on(edge.server.connect_upstream(edge_current_transport));
    let current = current_authority.accept_subscriber(current_transport, identity);
    current
        .borrow_mut()
        .transport
        .send(SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: Some(GlobalTime(2)),
            durability: Some(DurabilityTier::Global),
        })
        .unwrap();
    edge.tick().unwrap();
    assert!(
        edge.server
            .outbox
            .borrow()
            .iter()
            .all(|pending| pending.tx_id != tx_id),
        "the selected admitted authority's matching receipt releases the upload"
    );
}

#[test]
fn public_permission_advice_accepts_an_explicit_zero_clause_receipt() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa3; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let target = server
        .insert("todos", cells("public", false, identity))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa3, identity, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        identity,
        NodeUuid::from_bytes([0xa3; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, identity);
    let advice = client.request_permission_advice(PermissionAdviceAction::Read {
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
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    server.server.set_permissions_ready(false).unwrap();
    let client = open_db(0xa1, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    let advice = client.request_permission_advice(PermissionAdviceAction::Insert {
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
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let partial = open_db(0x5e, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa1, author, &schema);
    let (client_transport, partial_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = partial.accept_subscriber(partial_transport, author);
    let advice = client.request_permission_advice(PermissionAdviceAction::Insert {
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
    let policy = public_literal_eq("done", PublicValue::Boolean(false));
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(PublicTablePolicies::new().with_update(None, policy)),
        ),
    );
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let target = server
        .insert("todos", cells("target", false, author))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa1, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    let advice = client.request_permission_advice(PermissionAdviceAction::Update {
        table: "todos".to_owned(),
        row: target,
        patch: BTreeMap::from([("done".to_owned(), Value::Bool(true))]),
    });

    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();

    assert_eq!(block_on(advice), PermissionAdvice::Denied);

    let missing = client.request_permission_advice(PermissionAdviceAction::Update {
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
    let request_id = PermissionAdviceRequestId([7; 16]);
    let message = SyncMessage::PermissionAdviceResponse {
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
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);
    let client = open_db(0xa1, author, &schema);
    let (client_transport, mut authority_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));

    let cancelled = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    client.tick().unwrap();
    let cancelled_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    drop(cancelled);

    let current = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(2),
    });
    client.tick().unwrap();
    let current_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
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
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa4; 16]);
    let client = open_db(0xa4, author, &schema);
    let (client_transport, mut authority_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa4; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let action = PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    };
    let first = client.request_permission_advice(action.clone());
    let second = client.request_permission_advice(action);
    client.tick().unwrap();

    let request_id = match try_recv_subscriber_payload(authority_transport.as_mut()).unwrap() {
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
    let schema = schema();
    let author = AuthorSubject::for_test_bytes([0xa1; 16]);

    let first = open_db(0xa1, author, &schema);
    let (first_transport, mut first_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let _first_upstream = crate::db::block_on(first.connect_upstream(first_transport));
    let cancelled = first.request_permission_advice(PermissionAdviceAction::Insert {
        table: "todos".to_owned(),
        cells: cells("sensitive", false, author),
    });
    drop(cancelled);
    first.tick().unwrap();
    assert!(try_recv_subscriber_payload(first_authority.as_mut()).is_none());

    let first_live = first.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    first.tick().unwrap();
    let first_id = match try_recv_subscriber_payload(first_authority.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    drop(first_live);

    let reopened = open_db(0xa1, author, &schema);
    let (reopened_transport, mut reopened_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa1; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        2,
    );
    let _reopened_upstream = crate::db::block_on(reopened.connect_upstream(reopened_transport));
    let reopened_live = reopened.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: row(1),
    });
    reopened.tick().unwrap();
    let reopened_id = match try_recv_subscriber_payload(reopened_authority.as_mut()).unwrap() {
        SyncMessage::AuthorizationScopeIntent { request_id, .. } => request_id,
        message => panic!("expected authority scope intent, got {message:?}"),
    };
    drop(reopened_live);

    assert_ne!(first_id, reopened_id);
}
