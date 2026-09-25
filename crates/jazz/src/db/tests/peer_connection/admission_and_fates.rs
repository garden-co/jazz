//! Link admission, authority selection, permission advice, and routed fates.

use super::*;
use crate::db::peer_connection::{
    ConnectionLink, PendingRowVersionFetch, PendingSubscriberControlResponse,
    coverage_group_subscription_key,
};
use crate::node::SKEW_TOLERANCE_MS;

struct ReceivePollTransport {
    outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    accepted_send: Cell<bool>,
    receive_polls: Rc<Cell<usize>>,
    backpressure_polls: Rc<Cell<usize>>,
    failure: Option<TransportError>,
    sticky_failure: bool,
}

impl Transport for ReceivePollTransport {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.accepted_send.set(true);
        self.outbound.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        None
    }

    fn try_recv_result(&mut self) -> Result<Option<SyncMessage>, TransportError> {
        self.receive_polls
            .set(self.receive_polls.get().saturating_add(1));
        if !self.accepted_send.get() {
            return Ok(None);
        }
        assert!(
            !self.outbound.borrow().is_empty(),
            "receive polling must flush an accepted outbound backlog"
        );
        let failure = if self.sticky_failure {
            self.failure.clone()
        } else {
            self.failure.take()
        };
        if matches!(failure, Some(TransportError::Backpressure)) {
            self.backpressure_polls
                .set(self.backpressure_polls.get().saturating_add(1));
        }
        match failure {
            Some(error) => Err(error),
            None => Ok(None),
        }
    }
}

#[test]
fn receive_poll_backpressure_defers_schema_admission_and_failed_is_terminal() {
    use groove::storage::TestStorage;

    let make_schema = |extra: bool| {
        let builder = PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid),
        );
        build_public_db_test_schema(if extra {
            builder.table(
                PublicTableSchemaBuilder::new("controls").column("value", PublicColumnType::Text),
            )
        } else {
            builder
        })
    };
    let base_schema = make_schema(false);
    let schema = make_schema(true);
    let open_pending_client = |node: u8| {
        let families = schema.column_families();
        let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
        let (storage, _) = TestStorage::controlled(&refs);
        let identity = DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::for_test_bytes([node; 16]),
        };
        let seeded = block_on(Db::open(DbConfig::new(
            base_schema.clone(),
            storage.clone(),
            identity,
        )))
        .unwrap();
        block_on(seeded.close()).unwrap();
        block_on(Db::open(DbConfig::new(schema.clone(), storage, identity))).unwrap()
    };
    let client = open_pending_client(0xd1);
    let receive_polls = Rc::new(Cell::new(0));
    let backpressure_polls = Rc::new(Cell::new(0));
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let mut transport = ReceivePollTransport {
        outbound: Rc::clone(&outbound),
        accepted_send: Cell::new(false),
        receive_polls: Rc::clone(&receive_polls),
        backpressure_polls: Rc::clone(&backpressure_polls),
        failure: Some(TransportError::Backpressure),
        sticky_failure: false,
    };
    transport
        .send(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0xd1; 16]),
            claims: BTreeMap::new(),
        })
        .unwrap();
    let _handle = block_on(client.connect_upstream_for_test(Box::new(transport)));

    block_on(client.tick()).expect("receive backpressure is deferred for retry");
    let pending = client
        .prepare_query(&Query::from("todos"))
        .expect_err("schema admission must remain pending after recoverable backpressure");
    assert_eq!(pending.code, ErrorCode::Schema);
    block_on(client.tick()).expect("the connection remains retryable");
    assert_eq!(backpressure_polls.get(), 1);
    assert!(receive_polls.get() >= 1);

    let failed_client = open_pending_client(0xd2);
    let failed_polls = Rc::new(Cell::new(0));
    let failed_outbound = Rc::new(RefCell::new(VecDeque::new()));
    let mut failed_transport = ReceivePollTransport {
        outbound: Rc::clone(&failed_outbound),
        accepted_send: Cell::new(false),
        receive_polls: Rc::clone(&failed_polls),
        backpressure_polls: Rc::new(Cell::new(0)),
        failure: Some(TransportError::Failed("wire closed".to_owned())),
        sticky_failure: true,
    };
    failed_transport
        .send(SyncMessage::SessionClaims {
            identity: AuthorSubject::for_test_bytes([0xd2; 16]),
            claims: BTreeMap::new(),
        })
        .unwrap();
    let _failed_handle =
        block_on(failed_client.connect_upstream_for_test(Box::new(failed_transport)));
    let error = match block_on(failed_client.tick()) {
        Err(error) => error,
        Ok(_) => block_on(failed_client.tick()).expect_err("permanent receive failure is terminal"),
    };
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(failed_polls.get() >= 1);
}

fn finish_catalogue_bootstrap_before_control_backpressure(
    subscriber: &Rc<LocalMutex<PeerConnection<RocksDbStorage>>>,
    outbound: &Rc<RefCell<VecDeque<SyncMessage>>>,
) {
    subscriber.borrow_mut().transport = Box::new(BackpressureOnceTransport {
        outbound: Rc::clone(outbound),
        failed: true,
    });
    subscriber.borrow_mut().tick().unwrap();
    assert!(matches!(
        outbound.borrow_mut().pop_front(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    assert!(outbound.borrow().is_empty());
    // Rearm the first-send fault specifically for the original control test.
    subscriber.borrow_mut().transport = Box::new(BackpressureOnceTransport {
        outbound: Rc::clone(outbound),
        failed: false,
    });
}

// Internal contention is deliberately planted: the public boundary is async
// detach completion and continued local writes, but callers cannot hold these
// owner guards deterministically through the public API.
#[test]
fn async_peer_detach_waits_for_connection_and_node_without_losing_local_work() {
    let author = AuthorSubject::for_test_bytes([0xd4; 16]);
    let client = open_db(0xd4, author, &schema());
    let (transport, _authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd4; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let connection = block_on(client.connect_upstream(transport));
    let mut detach = Box::pin(client.detach_connection_async(&connection));
    let held_connection = block_on(connection.lock());
    assert!(
        std::future::Future::poll(
            detach.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    drop(held_connection);
    let held_node = block_on(client.node.node.lock());
    assert!(
        std::future::Future::poll(
            detach.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    drop(held_node);
    assert!(block_on(detach).unwrap());
    assert!(!block_on(client.detach_connection_async(&connection)).unwrap());
    let write = client
        .insert(
            "todos",
            doctest_support::todo_cells("after detach", false),
            Default::default(),
        )
        .unwrap();
    assert!(block_on(write.wait(DurabilityTier::Local)).is_ok());
}

// Internal scheduling receipt: public callers cannot deliberately suspend
// the successor's peer owner while detaching the selected authority.
#[test]
fn review_async_detach_waits_for_surviving_authority_owner() {
    let author = AuthorSubject::for_test_bytes([0xd5; 16]);
    let client = open_db(0xd5, author, &schema());
    let (first_transport, _first_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd5; 16]),
        1,
        NodeUuid::from_bytes([0x5d; 16]),
        1,
    );
    let first = block_on(client.connect_upstream(first_transport));
    let (second_transport, _second_authority) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd5; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let second = block_on(client.connect_upstream(second_transport));
    let held_successor = block_on(second.lock());
    let mut detach = Box::pin(client.detach_connection_async(&first));
    assert!(
        std::future::Future::poll(
            detach.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    drop(held_successor);
    assert!(block_on(detach).unwrap());
    assert!(!block_on(client.detach_connection_async(&first)).unwrap());
    assert!(block_on(client.detach_connection_async(&second)).unwrap());
}

#[test]
fn async_peer_detach_concurrent_cancellation_releases_inventory() {
    let author = AuthorSubject::for_test_bytes([0xd6; 16]);
    let client = open_db(0xd6, author, &schema());
    let (a, _a_wire) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd6; 16]),
        1,
        NodeUuid::from_bytes([0x5d; 16]),
        1,
    );
    let first = block_on(client.connect_upstream(a));
    let (b, _b_wire) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xd6; 16]),
        2,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let second = block_on(client.connect_upstream(b));
    let held = block_on(second.lock());
    let mut detach_first = Box::pin(client.detach_connection_async(&first));
    let mut detach_second = Box::pin(client.detach_connection_async(&second));
    let mut cx = std::task::Context::from_waker(std::task::Waker::noop());
    assert!(std::future::Future::poll(detach_first.as_mut(), &mut cx).is_pending());
    assert!(std::future::Future::poll(detach_second.as_mut(), &mut cx).is_pending());
    // Cancelling A must release its earlier peer guard, allowing B to finish.
    drop(detach_first);
    let write = client
        .insert(
            "todos",
            doctest_support::todo_cells("while detach waits", false),
            Default::default(),
        )
        .unwrap();
    assert!(block_on(write.wait(DurabilityTier::Local)).is_ok());
    drop(held);
    assert!(block_on(detach_second).unwrap());
    assert!(block_on(client.detach_connection_async(&first)).unwrap());
    assert!(client.node.connections.borrow().is_empty());
}

#[test]
fn strict_upstream_install_waits_for_existing_peer_and_cancels_without_admission() {
    let author = AuthorSubject::for_test_bytes([0xdb; 16]);
    let db = open_db(0xdb, author, &schema());
    let transport = |epoch| {
        duplex_with_admitted_session_context(
            author,
            NodeUuid::from_bytes([0xdb; 16]),
            epoch,
            NodeUuid::from_bytes([0x5d; 16]),
            epoch,
        )
    };
    let (first_transport, _first_wire) = transport(1);
    let first = block_on(db.connect_upstream(first_transport));
    let selected = *db.node.admitted_upstream_authority.borrow();
    let held = block_on(first.lock());
    let (next_transport, _next_wire) = transport(2);
    let mut install = Box::pin(db.try_connect_upstream(next_transport));
    assert!(
        std::future::Future::poll(
            install.as_mut(),
            &mut std::task::Context::from_waker(std::task::Waker::noop())
        )
        .is_pending()
    );
    assert_eq!(*db.node.admitted_upstream_authority.borrow(), selected);
    assert_eq!(db.node.connections.borrow().len(), 1);
    drop(install);
    let write = db
        .insert(
            "todos",
            cells("local during cancelled install", false, author),
            Default::default(),
        )
        .unwrap();
    assert!(block_on(write.wait(DurabilityTier::Local)).is_ok());
    drop(held);
    let (next_transport, _retry_wire) = transport(3);
    assert!(block_on(db.try_connect_upstream(next_transport)).is_ok());
    assert_eq!(db.node.connections.borrow().len(), 2);
    assert_eq!(*db.node.admitted_upstream_authority.borrow(), selected);
}

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
        durability: Some(DurabilityTier::Global),
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
    finish_catalogue_bootstrap_before_control_backpressure(&subscriber, &outbound);
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
                requests: VecDeque::from([request.clone()]),
                sent_count: 0,
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
                requests: VecDeque::from([request.clone()]),
                sent_count: 0,
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
        assert_eq!(
            state
                .pending_row_version_fetches
                .front()
                .unwrap()
                .sent_count,
            1,
            "the accepted batch remains owned until its reply arrives"
        );
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
    finish_catalogue_bootstrap_before_control_backpressure(&subscriber, &outbound);
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
        Some(SyncMessage::AuthorizationScopeIntent {
            request_id, action, ..
        }) => {
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
    finish_catalogue_bootstrap_before_control_backpressure(&subscriber, &outbound);
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

// The exact bounded-adapter refusal and retained control queue are internal
// transport state; the public API only observes the eventual receipt.
#[test]
fn queued_sibling_authorization_receipt_survives_backpressure_exactly_once() {
    let identity = AuthorSubject::for_test_bytes([0xc8; 16]);
    let server = open_core(0x5f, AuthorSubject::SYSTEM, &schema());
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    let subscriber = server.accept_subscriber(
        Box::new(BackpressureOnceTransport {
            outbound: Rc::clone(&outbound),
            failed: false,
        }),
        identity,
    );
    let subscription = SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x31; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x32; 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let response = SyncMessage::AuthorizationScopeReceipt {
        subscription,
        receipt: AuthorizationScopeReceipt {
            key: AuthorizationSupportScopeKey {
                support_shape_digest: [0x41; 32],
                subject: identity,
                claims_digest: [0x42; 32],
                policy_digest: [0x43; 32],
            },
            authority: [0x5f; 16],
            link: identity,
            authority_epoch: 1,
            claims_revision: 2,
            policy_epoch: 3,
            settled_through: GlobalTime(4),
            authorization_progress: 5,
        },
    };
    subscriber
        .borrow_mut()
        .pending_control_responses
        .push_back(PendingSubscriberControlResponse::Direct(response.clone()));

    subscriber
        .borrow_mut()
        .tick()
        .expect("the refused sibling receipt remains queued");
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
        .expect("capacity retry accepts the sibling receipt");
    subscriber
        .borrow_mut()
        .tick()
        .expect("a later tick must not duplicate the accepted receipt");
    assert_eq!(
        outbound
            .borrow()
            .iter()
            .filter(|message| *message == &response)
            .count(),
        1
    );
    assert!(subscriber.borrow().pending_control_responses.is_empty());
}

// Fate-observer routing is connection-internal and cannot be inspected through
// the public query API, so exercise the same subscriber send helper used by
// canonical sibling fanout.
#[test]
fn canonical_sibling_pending_carrier_registers_a_fate_observer() {
    let identity = AuthorSubject::for_test_bytes([0xc9; 16]);
    let server = open_core(0x60, AuthorSubject::SYSTEM, &schema());
    let node = server.node();
    let mut peer = PeerState::new();
    let (_receiver, mut transport) = duplex();
    let local_fate_routes = Rc::new(RefCell::new(BTreeMap::new()));
    let downstream_fates = Rc::new(RefCell::new(Vec::new()));
    let tx_id = TxId::new(TxTime(7), NodeUuid::from_bytes([0x61; 16]));
    let tx = Transaction {
        tx_id,
        kind: TxKind::Mergeable,
        n_total_writes: 0,
        made_by: identity,
        permission_subject: None,
        base_snapshot: None,
        row_read_set: None,
        absent_read_set: None,
        predicate_read_set: None,
        user_metadata_json: None,
        contribution_merge: None,
    };
    let subscription = SubscriptionKey {
        shape_id: ShapeId(uuid::Uuid::from_bytes([0x62; 16])),
        binding_id: BindingId(uuid::Uuid::from_bytes([0x63; 16])),
        read_view: RegisterShapeOptions::default().read_view_key(),
    };
    let update = SyncMessage::ViewUpdate(crate::protocol::ViewUpdatePayload {
        subscription,
        settled_through: GlobalTime(0),

        version_carriers: vec![VersionCarrier::Bundle(VersionBundle {
            tx,
            versions: Vec::new(),
            scope: VersionBundleScope::ViewScoped,
            fate: Fate::Pending,
            global_time: None,
            durability: DurabilityTier::Local,
        })],
        peer_payload_inventory: PeerPayloadInventory::default(),
        supporting_rows: crate::protocol::SupportingRowsUpdate::snapshot(Vec::new()),
    });

    send_subscriber_with_sync_context(
        &node,
        &mut peer,
        transport.as_mut(),
        &local_fate_routes,
        &downstream_fates,
        update,
    )
    .expect("canonical sibling update is accepted");

    let routes = local_fate_routes.borrow();
    let route = routes
        .get(&tx_id)
        .and_then(|routes| routes.first())
        .expect("pending carrier registers its transaction fate route");
    assert!(Rc::ptr_eq(
        &route
            .queue
            .upgrade()
            .expect("downstream fate queue remains live"),
        &downstream_fates,
    ));
}

#[test]
fn catalogue_bootstrap_is_eager_but_later_idle_updates_remain_trusted_only() {
    // This stays internal because trust is authenticated by the host at the
    // transport boundary; exposing it through a public client fixture would
    // test the HTTP/WebSocket bootstrap race rather than this hop contract.
    let base = schema();
    let core = open_core(0x5e, AuthorSubject::SYSTEM, &base);

    let (mut backend_transport, core_backend_transport) = duplex();
    let backend_link = core.accept_subscriber_with_trust(
        core_backend_transport,
        AuthorSubject::for_test_bytes([0xe1; 16]),
        CommitUnitTrust::TrustedBackend,
    );
    let (mut client_transport, core_client_transport) = duplex();
    let client_link = core.accept_subscriber(
        core_client_transport,
        AuthorSubject::for_test_bytes([0xc1; 16]),
    );

    backend_link.borrow_mut().tick().unwrap();
    assert!(matches!(
        backend_transport.try_recv(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    assert!(backend_transport.try_recv().is_none());
    backend_link.borrow_mut().tick().unwrap();
    assert!(
        backend_transport.try_recv().is_none(),
        "an unchanged catalogue fingerprint must not resend its snapshot"
    );
    client_link.borrow_mut().tick().unwrap();
    assert!(
        matches!(
            client_transport.try_recv(),
            Some(SyncMessage::CatalogueSnapshot(_))
        ),
        "admitted sessions receive the initial catalogue before query compilation"
    );
    assert!(client_transport.try_recv().is_none());

    // A resumed connection receives an actual snapshot even with unchanged A;
    // a peer requesting absent B needs that explicit authoritative outcome.
    let cursor = client_link.borrow_mut().take_resume_cursor().unwrap();
    let (mut client_transport, core_client_transport) = duplex();
    let client_link = core.accept_subscriber_with_resume(
        core_client_transport,
        AuthorSubject::for_test_bytes([0xc1; 16]),
        cursor,
    );
    client_link.borrow_mut().tick().unwrap();
    let Some(SyncMessage::CatalogueSnapshot(snapshot)) = client_transport.try_recv() else {
        panic!("resumed unchanged authority must send its catalogue");
    };
    assert_eq!(snapshot.current_write_schema.schema, base.version_id());

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

    backend_link.borrow_mut().tick().unwrap();
    let Some(SyncMessage::CatalogueSnapshot(snapshot)) = backend_transport.try_recv() else {
        panic!("trusted backend must receive the changed catalogue before any subscription");
    };
    assert!(
        snapshot
            .schemas
            .iter()
            .any(|schema| schema.id == evolved.id),
        "changed snapshot carries the newly published schema"
    );
    assert!(backend_transport.try_recv().is_none());

    client_link.borrow_mut().tick().unwrap();
    assert!(
        client_transport.try_recv().is_none(),
        "later idle changes remain request-driven on ordinary session links"
    );
    let cursor = client_link.borrow_mut().take_resume_cursor().unwrap();
    let (mut resumed_transport, core_resumed_transport) = duplex();
    let resumed = core.accept_subscriber_with_resume(
        core_resumed_transport,
        AuthorSubject::for_test_bytes([0xc1; 16]),
        cursor,
    );
    resumed.borrow_mut().tick().unwrap();
    let Some(SyncMessage::CatalogueSnapshot(snapshot)) = resumed_transport.try_recv() else {
        panic!("resumed session must receive published B before querying");
    };
    assert!(
        snapshot
            .schemas
            .iter()
            .any(|schema| schema.id == evolved.id)
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
        expected.link,
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
            expected.link,
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
/// A trusted backend can reconnect with concurrent, distinct tenant bindings
/// for the same delegated identity; each answer must use only its own support
/// receipt.
///
/// backend ──editor tenant scope──► reconnect ──support view + receipt──► Allowed
/// backend ──viewer tenant scope──► reconnect ──support view + receipt──► Denied
fn backend_permission_advice_keeps_concurrent_delegated_claim_scopes_separate_after_reconnect() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("tenant", PublicColumnType::Text)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(public_session_eq("tenant", &["claims", "tenant"])),
                ),
        ),
    );
    let delegated = AuthorSubject::for_test_bytes([0xc7; 16]);
    let backend = open_db(0xbe, AuthorSubject::SYSTEM, &schema);
    let authority = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let owned = authority
        .insert(
            "todos",
            BTreeMap::from([
                ("title".to_owned(), Value::String("owned".to_owned())),
                ("done".to_owned(), Value::Bool(false)),
                ("tenant".to_owned(), Value::String("editor".to_owned())),
            ]),
        )
        .unwrap()
        .row_uuid();
    let (backend_transport, authority_transport) = duplex_with_admitted_session_context(
        AuthorSubject::SYSTEM,
        NodeUuid::from_bytes([0xbe; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let upstream = crate::db::block_on(backend.connect_upstream(backend_transport));
    let _subscriber = authority.server.accept_subscriber_with_claims_and_trust(
        authority_transport,
        AuthorSubject::SYSTEM,
        BTreeMap::new(),
        CommitUnitTrust::TrustedBackend,
    );
    let mut editor = test_provider_claims(delegated);
    editor.insert(
        crate::query::provider_claim_key("tenant"),
        Value::String("editor".to_owned()),
    );
    let mut viewer = test_provider_claims(delegated);
    viewer.insert(
        crate::query::provider_claim_key("tenant"),
        Value::String("viewer".to_owned()),
    );
    let action = PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: owned,
    };
    let allowed = backend.request_permission_advice_with_delegated_session(
        action.clone(),
        crate::protocol::DelegatedSessionBinding {
            identity: delegated,
            claims: editor,
        },
    );
    let denied = backend.request_permission_advice_with_delegated_session(
        action,
        crate::protocol::DelegatedSessionBinding {
            identity: delegated,
            claims: viewer,
        },
    );
    // The first admitted backend link sends both intents but disappears
    // before the authority can hydrate either support scope.
    backend.tick().unwrap();
    assert!(backend.detach_connection(&upstream));
    let (reconnected_backend_transport, reconnected_authority_transport) =
        duplex_with_admitted_session_context(
            AuthorSubject::SYSTEM,
            NodeUuid::from_bytes([0xbe; 16]),
            2,
            NodeUuid::from_bytes([0x5e; 16]),
            2,
        );
    let _reconnected_upstream =
        crate::db::block_on(backend.connect_upstream(reconnected_backend_transport));
    let _reconnected_subscriber = authority.server.accept_subscriber_with_claims_and_trust(
        reconnected_authority_transport,
        AuthorSubject::SYSTEM,
        BTreeMap::new(),
        CommitUnitTrust::TrustedBackend,
    );
    for _ in 0..16 {
        backend.tick().unwrap();
        authority.tick().unwrap();
    }
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    let mut allowed = Box::pin(allowed);
    let mut denied = Box::pin(denied);
    assert_eq!(
        allowed.as_mut().poll(&mut context),
        Poll::Ready(PermissionAdvice::Allowed),
        "backend must receive and apply the editor-bound authority receipt"
    );
    assert_eq!(
        denied.as_mut().poll(&mut context),
        Poll::Ready(PermissionAdvice::Denied),
        "viewer binding must not coalesce with the editor support scope"
    );
}

#[test]
fn ordinary_session_link_rejects_forged_delegated_permission_advice_intent() {
    // Internal protocol-admission test: a raw intent is the only way to plant
    // a forged delegation field; public callers cannot manufacture it.
    let schema = owner_read_schema();
    let ordinary = AuthorSubject::for_test_bytes([0xa0; 16]);
    let forged = AuthorSubject::for_test_bytes([0xb0; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let (mut client_transport, server_transport) = duplex_with_admitted_session_context(
        ordinary,
        NodeUuid::from_bytes([0xa0; 16]),
        1,
        NodeUuid::from_bytes([0x5e; 16]),
        1,
    );
    let subscriber = server.accept_subscriber(server_transport, ordinary);
    // Finish authenticated startup before exercising the control under test.
    subscriber.borrow_mut().tick().unwrap();
    assert!(matches!(
        client_transport.try_recv(),
        Some(SyncMessage::CatalogueSnapshot(_))
    ));
    assert!(client_transport.try_recv().is_none());
    client_transport
        .send(SyncMessage::AuthorizationScopeIntent {
            request_id: PermissionAdviceRequestId([0xa1; 16]),
            action: PermissionAdviceAction::Read {
                table: "todos".to_owned(),
                row: row(1),
            },
            delegated_session: Some(crate::protocol::DelegatedSessionBinding {
                identity: forged,
                claims: BTreeMap::new(),
            }),
        })
        .unwrap();
    subscriber.borrow_mut().tick().unwrap();
    assert!(
        client_transport.try_recv().is_none(),
        "an ordinary session link cannot turn a forged delegation into authority advice"
    );
}

#[test]
fn permission_advice_uses_authenticated_link_identity_without_mutating() {
    // INV-SYNC-45: exercise the complete-snapshot receiver contract.
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

/// Internal: the hydration count is not observable through public advice.
/// Each advice proof is seeded with its target row (#3468), so distinct rows
/// hydrate separate one-row scopes, while asking about the same row again
/// before any write reuses its cached scope.
#[test]
fn advice_scopes_hydrate_once_per_row_and_reuse_until_a_write() {
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

    let again = client.request_permission_advice(PermissionAdviceAction::Read {
        table: "todos".to_owned(),
        row: allowed,
    });
    client.tick().unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    assert_eq!(block_on(again), PermissionAdvice::Allowed);

    let hydration_count = match &subscriber.borrow().link {
        ConnectionLink::Subscriber(SubscriberConnectionState {
            authority_scope_hydration_count,
            ..
        }) => *authority_scope_hydration_count,
        ConnectionLink::Upstream(_) => unreachable!("server link is a subscriber"),
    };
    assert_eq!(
        hydration_count, 2,
        "each row hydrates its own one-row scope once; a repeat ask reuses it"
    );
}

/// Internal: advice answers are identical whether the proof reads one row or
/// the whole table, so only the storage counter shows the difference. After
/// a write invalidates the cached scope, the next ask must re-prove only its
/// own row (#3468), not rehydrate every row the policy could match.
#[test]
fn advice_after_a_write_reads_only_the_target_row() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let mut target = None;
    for n in 0..80 {
        let row = server
            .insert("todos", cells(&format!("owned {n}"), false, alice))
            .unwrap()
            .row_uuid();
        if n == 40 {
            target = Some(row);
        }
    }
    let target = target.unwrap();
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
    let _subscriber = server.accept_subscriber(server_transport, alice);
    let ask = |row| {
        let advice = client.request_permission_advice(PermissionAdviceAction::Read {
            table: "todos".to_owned(),
            row,
        });
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        block_on(advice)
    };
    assert_eq!(ask(target), PermissionAdvice::Allowed);

    server
        .insert("todos", cells("invalidates the cached scope", false, alice))
        .unwrap();
    server.tick().unwrap();
    client.tick().unwrap();
    server.server.node.borrow().reset_storage_read_metrics();
    assert_eq!(ask(target), PermissionAdvice::Allowed);
    let metrics = server.server.node.borrow().take_storage_read_metrics();
    assert!(
        metrics.total.reads <= 12,
        "advice after a write must re-prove one row, not all 81: {metrics:?}"
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
        if let SyncMessage::AuthorizationScopeIntent {
            request_id, action, ..
        } = message
        {
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

/// A whole-table maintained usage refreshes its direct admission by replacing
/// the retained receiver with a fresh reset, rather than letting its normal
/// delta path retain the old session's rows.
#[test]
fn direct_whole_table_claim_refresh_reopens_under_new_binding() {
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
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    subscriber.borrow_mut().tick().unwrap();
    client.tick().unwrap();

    // Discard the opening reset. The refresh must independently publish a
    // replacement empty closure rather than letting this assertion pass on
    // the original allowed snapshot.
    server_sent.borrow_mut().clear();
    client.set_test_provider_claims(session_subject, denied_claims.clone());
    let expected_denied_claims = denied_claims.clone();
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(denied_claims);
    subscriber.borrow_mut().tick().unwrap();
    let sent = server_sent.borrow();
    let refreshed = sent
        .iter()
        .filter_map(|message| match message {
            SyncMessage::ViewUpdate(update) => Some(update),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert!(
        !refreshed.is_empty(),
        "claim refresh must publish a replacement ViewUpdate, got {sent:?}"
    );
    assert_eq!(
        refreshed.len(),
        1,
        "claim refresh must not duplicate one live ViewUpdate transition"
    );
    for update in refreshed {
        assert_eq!(update.subscription, attachment.subscription());
        assert!(update.version_carriers.is_empty());

        assert!(
            update
                .peer_payload_inventory
                .authorization_progress
                .is_some()
                && !update.peer_payload_inventory.opening_pending,
            "the empty reset must carry its settled authorization receipt"
        );
        assert!(update.supporting_rows.added_rows().is_empty());
    }
    drop(sent);
    let ConnectionLink::Subscriber(state) = &subscriber.borrow().link else {
        unreachable!("accepted client is served by a subscriber link")
    };
    let coverage = state
        .served
        .get(&attachment.subscription())
        .expect("ordinary usage stays registered");
    let group = state
        .coverage_groups
        .get(coverage)
        .expect("ordinary usage retains its coverage group");
    assert_eq!(group.policy_binding.1, expected_denied_claims);
    assert_eq!(
        group.policy_binding_origin,
        CoveragePolicyBindingOrigin::DirectAdmitted
    );
}

/// Full-diff fallback counters (#3292). The serving link's counter is the
/// same one the server shell reports as `subscription_full_diff_fallbacks`;
/// it is read from the link because no client API exposes a server's
/// recompute strategy.
///
/// alice opens todos ─► initial reset          (not a fallback)
/// server inserts a row ─► incremental delta  (not a fallback)
/// alice's claims change ─► retire + reopen    (one query reopen)
#[test]
fn claim_refresh_counts_one_full_diff_fallback_and_incremental_deltas_count_none() {
    let schema = owner_read_schema();
    let session_subject = AuthorSubject::for_test_bytes([0xa1; 16]);
    let allowed_owner = AuthorSubject::for_test_bytes([0xb1; 16]);
    let denied_owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let first = server
        .insert("todos", cells("first", false, allowed_owner))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa1, session_subject, &schema);
    let allowed_claims = test_provider_claims(allowed_owner);
    let denied_claims = test_provider_claims(denied_owner);
    client.set_test_provider_claims(session_subject, allowed_claims.clone());
    let (client_transport, server_transport) = duplex();
    let upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber =
        server.accept_subscriber_with_claims(server_transport, session_subject, allowed_claims);
    let query = Query::from("todos");
    let tick_all = || {
        for _ in 0..32 {
            client.tick().unwrap();
            server.tick().unwrap();
            subscriber.borrow_mut().tick().unwrap();
            upstream.borrow_mut().tick().unwrap();
        }
    };
    let prepared = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    tick_all();
    assert!(client.query_attachment_is_covered(&attachment));
    assert_eq!(
        row_ids(&prepared_all(&client, &query, global_subscribe_opts())),
        vec![first]
    );
    assert_eq!(
        subscriber.borrow().full_diff_fallbacks(),
        Default::default(),
        "opening a maintained view is ordinary hydration"
    );

    let second = server
        .insert("todos", cells("second", false, allowed_owner))
        .unwrap()
        .row_uuid();
    tick_all();
    let mut expected = vec![first, second];
    expected.sort();
    let mut visible = row_ids(&prepared_all(&client, &query, global_subscribe_opts()));
    visible.sort();
    assert_eq!(visible, expected);
    assert_eq!(
        subscriber.borrow().full_diff_fallbacks(),
        Default::default(),
        "an incremental delta is not a fallback"
    );

    client.set_test_provider_claims(session_subject, denied_claims.clone());
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(denied_claims);
    tick_all();
    assert!(
        prepared_all(&client, &query, global_subscribe_opts()).is_empty(),
        "the refreshed subscription must lose the old claims' rows"
    );
    let fallbacks = subscriber.borrow().full_diff_fallbacks();
    assert_eq!(fallbacks.query_reopens, 1, "{fallbacks:?}");
    assert_eq!(fallbacks.total(), 1, "{fallbacks:?}");
}

/// The maintained-group cursor consumes each accepted replacement reset once.
/// The public admission contract normally gives one connection one exact
/// `SubscriptionKey` per coverage group; this white-box pair exercises the
/// shared group path anyway so a future coalescing caller cannot replay the
/// first accepted reset when the second send is backpressured.
#[test]
fn claim_refresh_retries_only_the_unsent_group_member_after_backpressure() {
    struct SecondViewUpdateBackpressureTransport {
        outbound: Rc<RefCell<VecDeque<SyncMessage>>>,
        accepted_view_updates: usize,
        rejected_second_view_update: bool,
    }

    impl Transport for SecondViewUpdateBackpressureTransport {
        fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
            if matches!(message, SyncMessage::ViewUpdate(_)) {
                if self.accepted_view_updates == 1 && !self.rejected_second_view_update {
                    self.rejected_second_view_update = true;
                    return Err(TransportError::Backpressure);
                }
                self.accepted_view_updates += 1;
            }
            self.outbound.borrow_mut().push_back(message);
            Ok(())
        }

        fn try_recv(&mut self) -> Option<SyncMessage> {
            None
        }
    }

    let schema = owner_read_schema();
    let session_subject = AuthorSubject::for_test_bytes([0xa1; 16]);
    let allowed_owner = AuthorSubject::for_test_bytes([0xb1; 16]);
    let denied_owner = AuthorSubject::for_test_bytes([0xb2; 16]);
    let later_denied_owner = AuthorSubject::for_test_bytes([0xb3; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    server
        .insert("todos", cells("A only", false, allowed_owner))
        .unwrap();
    let client = open_db(0xa1, session_subject, &schema);
    let allowed_claims =
        BTreeMap::from([("sub".to_owned(), Value::Uuid(allowed_owner.test_uuid()))]);
    let denied_claims = BTreeMap::from([("sub".to_owned(), Value::Uuid(denied_owner.test_uuid()))]);
    let later_denied_claims = BTreeMap::from([(
        "sub".to_owned(),
        Value::Uuid(later_denied_owner.test_uuid()),
    )]);
    client.set_test_provider_claims(session_subject, allowed_claims.clone());
    let (client_transport, server_transport, _client_sent, _server_sent) = duplex_with_taps();
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let subscriber =
        server.accept_subscriber_with_claims(server_transport, session_subject, allowed_claims);

    let prepared = prepared(&client, &Query::from("todos"));
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    client.tick().unwrap();
    subscriber.borrow_mut().tick().unwrap();

    let mut second_subscription = attachment.subscription();
    second_subscription.read_view.id = uuid::Uuid::from_bytes([0xc3; 16]);
    let outbound = Rc::new(RefCell::new(VecDeque::new()));
    {
        let mut connection = subscriber.borrow_mut();
        let ConnectionLink::Subscriber(state) = &mut connection.link else {
            unreachable!("accepted client is served by a subscriber link")
        };
        let coverage = state.served[&attachment.subscription()].clone();
        state.served.insert(second_subscription, coverage.clone());
        let group = state
            .coverage_groups
            .get_mut(&coverage)
            .expect("ordinary usage retains its coverage group");
        group.subscribers.insert(second_subscription);
        group
            .pending_initial_subscribers
            .insert(second_subscription);
        connection.transport = Box::new(SecondViewUpdateBackpressureTransport {
            outbound: Rc::clone(&outbound),
            accepted_view_updates: 0,
            rejected_second_view_update: false,
        });
    }

    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(denied_claims.clone());
    let error = subscriber
        .borrow_mut()
        .tick()
        .expect_err("the second fresh reset is backpressured");
    assert_eq!(error.code, ErrorCode::Backpressure);
    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(later_denied_claims.clone());
    subscriber
        .borrow_mut()
        .tick()
        .expect("a newer claim revision reopens every group member after capacity returns");

    let outbound = outbound.borrow();
    let refreshed = outbound
        .iter()
        .filter_map(|message| match message {
            SyncMessage::ViewUpdate(update) => Some(update),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(refreshed.len(), 3);
    let progress_by_subscription = refreshed.iter().fold(
        BTreeMap::<SubscriptionKey, Vec<u64>>::new(),
        |mut progress, update| {
            progress.entry(update.subscription).or_default().push(
                update
                    .peer_payload_inventory
                    .authorization_progress
                    .expect("claim-refresh reset carries authorization progress"),
            );
            progress
        },
    );
    assert_eq!(
        progress_by_subscription[&attachment.subscription()].len(),
        2,
        "the accepted A reset is followed by exactly one newer B reset"
    );
    assert_eq!(
        progress_by_subscription[&second_subscription].len(),
        1,
        "the rejected A reset never reaches the second member"
    );
    assert!(
        progress_by_subscription[&attachment.subscription()][0]
            < progress_by_subscription[&attachment.subscription()][1]
    );
    assert_eq!(
        progress_by_subscription[&attachment.subscription()][1],
        progress_by_subscription[&second_subscription][0],
        "the newer claim revision reaches every current member"
    );
    assert!(
        refreshed
            .iter()
            .all(|update| { update.supporting_rows.added_rows().is_empty() })
    );
    let ConnectionLink::Subscriber(state) = &subscriber.borrow().link else {
        unreachable!("accepted client is served by a subscriber link")
    };
    let coverage = &state.served[&attachment.subscription()];
    let group = &state.coverage_groups[coverage];
    assert!(group.pending_initial_subscribers.is_empty());
    assert!(group.initialized);
    assert_eq!(group.policy_binding.1, later_denied_claims);
    drop(outbound);
}

/// A scope-isolated relay usage site retains the immutable session selected by
/// server admission even when the host refresh hook runs afterward. This
/// deliberately admits SYSTEM as the foreground session: provenance comes
/// from the exact admitted binding, not identity equality or the transport.
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
    let subscriber = server.server.accept_scope_isolated_relay_subscriber(
        server_transport,
        delegated_identity,
        delegated_claims.clone(),
        1,
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
/// merely its concrete wire usage. Both direct coverage and a scope-isolated
/// relay use an immutable admitted-policy key, so neither may leave Groove
/// work behind after normal detach.
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
            coverage.policy_binding.is_some(),
            "direct coverage is isolated by its admitted policy snapshot"
        );
        assert_ne!(
            maintained,
            direct_attachment.subscription(),
            "a direct coverage evaluator must not reuse its public wire usage key"
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
        .accept_scope_isolated_relay_subscriber(
            delegated_server_transport,
            delegated_identity,
            delegated_claims.clone(),
            1,
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

/// A direct session served by a relay must replace its propagated, delegated
/// Core usage site on refresh. Rebinding only the relay-local evaluator makes a
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
    let relay = open_db(0xe1, AuthorSubject::SYSTEM, &schema);
    relay.set_relay_authority_session_owner_for_test();
    let client = open_db(0xc1, session_subject, &schema);
    let allowed_claims = test_provider_claims(allowed_owner);
    let denied_claims = test_provider_claims(denied_owner);
    client.set_test_provider_claims(session_subject, allowed_claims.clone());

    let (relay_transport, core_transport) = duplex();
    let relay_upstream = crate::db::block_on(relay.connect_upstream(relay_transport));
    // The Core does not infer a user session from a trusted/backend transport.
    // This test models the production scope-relay handshake that admits the
    // exact foreground binding forwarded by the relay.
    let core_relay = core.accept_scope_isolated_relay_subscriber(
        core_transport,
        session_subject,
        allowed_claims.clone(),
        1,
    );
    let (client_transport, relay_client_transport, _client_sent, relay_sent) = duplex_with_taps();
    let _client_upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let relay_client = relay.accept_subscriber_with_claims(
        relay_client_transport,
        session_subject,
        allowed_claims.clone(),
    );

    let query = Query::from("todos");
    let prepared = prepared(&client, &query);
    let attachment = client
        .attach_query_with_opts(&prepared, global_subscribe_opts())
        .unwrap();
    for _ in 0..96 {
        client.tick().unwrap();
        relay.tick().unwrap();
        core.tick().unwrap();
        relay.tick().unwrap();
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
        let connection = relay_client.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!("relay serves the direct client")
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
        &core_relay.borrow().link,
        ConnectionLink::Subscriber(state) if state.served.contains_key(&old_upstream_subscription)
    ));
    assert!(matches!(
        &relay_client.borrow().link,
        ConnectionLink::Subscriber(state) if state.peer.has_maintained_subscription(old_maintained_subscription)
    ));
    let expected_group_source = relay
        .node
        .node()
        .borrow()
        .authority_result_key_for_subscription(old_upstream_subscription)
        .expect("scope relay installs the exact upstream authority result");
    assert!(
        matches!(
            &relay_client.borrow().link,
            ConnectionLink::Subscriber(state)
                if state
                    .peer
                    .subscription_authority_result_source(old_maintained_subscription)
                    == Some(&expected_group_source)
        ),
        "the group-owned maintained receiver must be bound to U before opening; recording U only on D reproduces the cold source=None receiver"
    );

    // The original Core capability is immutable. A direct client claim
    // refresh cannot widen or narrow it in place; production `updateAuth`
    // disconnects and re-admits the scope relay under a fresh epoch.
    let old_core_binding = match &core_relay.borrow().link {
        ConnectionLink::Subscriber(state) => state
            .peer
            .subscription_policy_binding(old_upstream_subscription),
        ConnectionLink::Upstream(_) => None,
    };
    assert_eq!(
        old_core_binding,
        Some((session_subject, allowed_claims.clone()))
    );
    assert!(relay.detach_connection(&relay_upstream));

    let (replacement_relay_transport, replacement_core_transport) = duplex();
    let _replacement_relay_upstream =
        crate::db::block_on(relay.connect_upstream(replacement_relay_transport));
    let replacement_core_relay = core.accept_scope_isolated_relay_subscriber(
        replacement_core_transport,
        session_subject,
        denied_claims.clone(),
        2,
    );
    client.set_test_provider_claims(session_subject, denied_claims.clone());
    relay_client
        .borrow_mut()
        .update_authenticated_session_claims(denied_claims);
    let mut saw_fresh_downstream_reset = false;
    for _ in 0..96 {
        client.tick().unwrap();
        relay.tick().unwrap();
        core.tick().unwrap();
        relay.tick().unwrap();
        saw_fresh_downstream_reset |= relay_sent.borrow().iter().any(|message| {
            matches!(
                message,
                SyncMessage::ViewUpdate(update)
                    if update.subscription == downstream_subscription

            )
        });
        client.tick().unwrap();
        if saw_fresh_downstream_reset
            && !matches!(
                &replacement_core_relay.borrow().link,
                ConnectionLink::Subscriber(state) if state.served.contains_key(&old_upstream_subscription)
            )
        {
            break;
        }
    }
    let connection = relay_client.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!("relay keeps serving the direct client")
    };
    let coverage = &state.served[&downstream_subscription];
    let fresh_group_subscription = coverage_group_subscription_key(coverage);
    let fresh_upstream_subscription = state.coverage_groups[coverage].upstream_subscription;
    drop(connection);
    let core_fresh = match &replacement_core_relay.borrow().link {
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
        &replacement_core_relay.borrow().link,
        ConnectionLink::Subscriber(state)
            if !state.served.contains_key(&old_upstream_subscription)
                && state.served.contains_key(&fresh_upstream_subscription)
    ));
    let fresh_authority_source = relay
        .node
        .node()
        .borrow()
        .authority_result_key_for_subscription(fresh_upstream_subscription)
        .expect("fresh upstream usage has an exact authority result");
    assert!(
        matches!(
            &relay_client.borrow().link,
            ConnectionLink::Subscriber(state)
                if fresh_group_subscription != old_maintained_subscription
                    && !state.peer.has_maintained_subscription(old_maintained_subscription)
                    && state.peer.has_maintained_subscription(fresh_group_subscription)
                    && state
                        .peer
                        .subscription_authority_result_source(fresh_group_subscription)
                        == Some(&fresh_authority_source)
        ),
        "claim refresh must retire the old policy receiver and reopen a fresh group against fresh U"
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
    // turn its client ingress into relay routing or bypass local proof.
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

/// A scope-isolated relay carries one binding selected by server admission. A
/// raw `SessionClaims` frame must neither replace that binding nor make the
/// later terminal write proof use the forged editor role.
///
/// ```text
/// viewer handshake ──scope relay──► Core
///       │ raw { role: editor }         │ terminal proof remains viewer → Rejected
///       └──────────────────────────────┘
/// ```
#[test]
fn scope_isolated_relay_terminal_write_rejects_denied_handshake_claims() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa8; 16]);
    let client = open_db(0xa8, alice, &schema);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let viewer_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("viewer".to_owned()),
    )]);
    let hostile_editor_claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("editor".to_owned()),
    )]);
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_scope_isolated_relay_subscriber(
        server_transport,
        alice,
        viewer_claims,
        7,
    );

    subscriber
        .borrow_mut()
        .update_authenticated_session_claims(hostile_editor_claims.clone());
    // Generic host refresh is valid for an ordinary Session link, but a
    // scope-isolated relay must receive a new server-issued capability on
    // reconnect rather than widen its immutable binding in place.

    relay_transport
        .send(SyncMessage::SessionClaims {
            identity: alice,
            claims: hostile_editor_claims.clone(),
        })
        .expect("hostile relay can send a raw wire frame");
    subscriber
        .borrow_mut()
        .tick()
        .expect("scope relay drops raw SessionClaims instead of adopting them");
    assert_ne!(
        server.node().borrow().session_claims_for(alice),
        hostile_editor_claims,
        "raw relay claims must not enter the node's mutable compatibility map"
    );

    let write = client
        .insert(
            "todos",
            cells("viewer must not write", false, alice),
            Default::default(),
        )
        .expect("client can stage a mergeable candidate before terminal policy proof");
    let tx_id = write.mergeable_tx_id();
    let unit = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(tx_id)
        .expect("staged candidate retains its exact commit unit");
    relay_transport
        .send(unit)
        .expect("scope relay forwards the unchanged commit unit");
    subscriber
        .borrow_mut()
        .tick()
        .expect("terminal authority processes scope relay upload");

    let fates = std::iter::from_fn(|| relay_transport.try_recv()).collect::<Vec<_>>();
    assert!(
        fates.iter().any(|message| matches!(
            message,
            SyncMessage::FateUpdate { tx_id: candidate, fate: Fate::Rejected(_), .. }
                if *candidate == tx_id
        )),
        "the terminal proof must use the immutable viewer handshake binding, got {fates:?}"
    );
    assert!(matches!(
        server
            .node()
            .borrow_mut()
            .transaction_state(tx_id)
            .expect("terminal authority retains the upload outcome")
            .0,
        Fate::Rejected(_)
    ));
}

#[test]
fn scope_relay_upload_rejects_forged_system_origin_before_persistence() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xaa; 16]);
    let client = open_db(0xaa, alice, &schema);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_scope_isolated_relay_subscriber(
        server_transport,
        alice,
        BTreeMap::new(),
        9,
    );

    let write = client
        .insert(
            "todos",
            cells("forged system provenance", false, alice),
            Default::default(),
        )
        .expect("client can stage a candidate before relay admission");
    let tx_id = write.mergeable_tx_id();
    let SyncMessage::CommitUnit { mut tx, versions } = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(tx_id)
        .expect("staged candidate retains its exact commit unit")
    else {
        unreachable!("commit_unit_for returns a commit unit")
    };
    tx.made_by = AuthorSubject::system_at(NodeUuid::from_bytes([0x6a; 16]));
    relay_transport
        .send(SyncMessage::CommitUnit { tx, versions })
        .expect("relay can send a raw forged upload");

    let error = subscriber
        .borrow_mut()
        .tick()
        .expect_err("relay provenance must match the admitted session");
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        server
            .node()
            .borrow_mut()
            .transaction_state(tx_id)
            .is_none(),
        "a rejected relay envelope must not retain forged system attribution"
    );
}

/// A scope-isolated relay cannot substitute any independently attributed
/// principal for its immutable delegated session binding.
#[test]
fn scope_relay_upload_rejects_forged_user_origin_before_persistence() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xaa; 16]);
    let bob = AuthorSubject::for_test_bytes([0xbb; 16]);
    let client = open_db(0xaa, alice, &schema);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_scope_isolated_relay_subscriber(
        server_transport,
        alice,
        BTreeMap::new(),
        9,
    );

    let write = client
        .insert(
            "todos",
            cells("forged user provenance", false, alice),
            Default::default(),
        )
        .expect("client can stage a candidate before relay admission");
    let tx_id = write.mergeable_tx_id();
    let SyncMessage::CommitUnit { mut tx, versions } = client
        .node
        .node
        .borrow_mut()
        .commit_unit_for(tx_id)
        .expect("staged candidate retains its exact commit unit")
    else {
        unreachable!("commit_unit_for returns a commit unit")
    };
    tx.made_by = bob;
    relay_transport
        .send(SyncMessage::CommitUnit { tx, versions })
        .expect("relay can send a raw forged upload");

    let error = subscriber
        .borrow_mut()
        .tick()
        .expect_err("scope relay provenance must match the admitted session");
    assert_eq!(error.code, ErrorCode::Protocol);
    assert!(
        server
            .node()
            .borrow_mut()
            .transaction_state(tx_id)
            .is_none(),
        "a rejected relay envelope must not retain forged user attribution"
    );
}

/// A scope relay may resend an authority-owned unit that is already stored.
/// Its immutable delegated binding does not become that unit's authority, and
/// it cannot rewrite the durable system origin while replaying it.
#[test]
fn scope_relay_replays_known_system_origin_without_claiming_authority() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xaa; 16]);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let write = server
        .insert("todos", cells("authority-owned", false, alice))
        .expect("system authority can create the durable unit");
    let tx_id = write.mergeable_tx_id();
    let unit = server
        .server
        .node()
        .borrow_mut()
        .commit_unit_for(tx_id)
        .expect("settled authority unit remains replayable");
    let SyncMessage::CommitUnit { mut tx, versions } = unit else {
        unreachable!("commit_unit_for returns a commit unit")
    };
    assert!(matches!(tx.made_by, AuthorSubject::SystemAt(_)));
    assert_eq!(tx.permission_subject, Some(AuthorSubject::SYSTEM));
    // This differs only in a raw untrusted permission hint. The relay must
    // redact it before duplicate identity comparison, not reject a known unit.
    tx.permission_subject = Some(alice);

    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_scope_isolated_relay_subscriber(
        server_transport,
        alice,
        BTreeMap::new(),
        9,
    );
    relay_transport
        .send(SyncMessage::CommitUnit { tx, versions })
        .expect("scope relay can retransmit the known unit");
    subscriber
        .borrow_mut()
        .tick()
        .expect("known authority replay is idempotent");

    let SyncMessage::CommitUnit { tx: stored, .. } = server
        .node()
        .borrow_mut()
        .commit_unit_for(tx_id)
        .expect("known replay does not remove the authority transaction")
    else {
        unreachable!("commit_unit_for returns a commit unit")
    };
    assert!(matches!(stored.made_by, AuthorSubject::SystemAt(_)));
    assert_ne!(stored.permission_subject, Some(alice));
}

/// Empty scope-relay claims are an admitted empty snapshot, not an invitation
/// to use default, prior, or process-global claims when terminal policy proof
/// runs.
#[test]
fn scope_isolated_relay_terminal_write_rejects_empty_handshake_claims() {
    let schema = editor_claim_write_schema();
    let alice = AuthorSubject::for_test_bytes([0xa9; 16]);
    let client = open_db(0xa9, alice, &schema);
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let (mut relay_transport, server_transport) = duplex();
    let subscriber = server.server.accept_scope_isolated_relay_subscriber(
        server_transport,
        alice,
        BTreeMap::new(),
        8,
    );
    let write = client
        .insert(
            "todos",
            cells("empty claims must not write", false, alice),
            Default::default(),
        )
        .expect("client can stage before the terminal policy decision");
    let tx_id = write.mergeable_tx_id();
    relay_transport
        .send(
            client
                .node
                .node
                .borrow_mut()
                .commit_unit_for(tx_id)
                .expect("staged candidate retains its commit unit"),
        )
        .expect("scope relay forwards the unchanged commit unit");
    subscriber
        .borrow_mut()
        .tick()
        .expect("terminal authority processes empty-claims relay upload");
    assert!(
        std::iter::from_fn(|| relay_transport.try_recv()).any(|message| matches!(
            message,
            SyncMessage::FateUpdate { tx_id: candidate, fate: Fate::Rejected(_), .. }
                if candidate == tx_id
        )),
        "an empty admitted binding must fail closed rather than inherit a policy subject"
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
        .authorization_support_scope_for_session(
            alice,
            Some(&a_claims),
            &PermissionAdviceAction::Insert {
                table: "todos".to_owned(),
                cells: candidate_cells.clone(),
            },
        )
        .expect("editor policy has a support clause");
    let (shape, binding) = scope
        .subscriptions
        .into_iter()
        .next()
        .expect("editor policy produces one support subscription");
    let a_subscription = SubscriptionKey {
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
            a_state.peer.subscription_policy_binding(a_subscription),
            Some((alice, a_claims.clone())),
            "the maintained terminal support receiver retains A rather than B's sibling snapshot"
        );
    }

    // 0→1→2 authenticated refreshes reuse the same canonical support key,
    // but each must replace its maintained receiver before terminal proof.
    a_subscriber
        .borrow_mut()
        .update_authenticated_session_claims(b_claims.clone());
    let b_scope = server
        .node()
        .borrow()
        .authorization_support_scope_for_session(
            alice,
            Some(&b_claims),
            &PermissionAdviceAction::Insert {
                table: "todos".to_owned(),
                cells: candidate_cells.clone(),
            },
        )
        .expect("viewer policy has the same support clause under its own snapshot");
    let (b_shape, b_binding) = b_scope
        .subscriptions
        .into_iter()
        .next()
        .expect("viewer policy produces one support subscription");
    let b_subscription = SubscriptionKey {
        shape_id: b_shape.shape_id(),
        binding_id: b_binding.binding_id(),
        read_view: b_scope.options.read_view_key(),
    };
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
            a_state.peer.subscription_policy_binding(b_subscription),
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
            a_state.peer.subscription_policy_binding(a_subscription),
            Some((alice, a_claims)),
            "each claim revision receives a fresh terminal support receiver"
        );
    }
}

#[test]
fn concurrent_upstreams_keep_selected_owner_until_detach_handoff() {
    let schema = schema();
    let identity = AuthorSubject::for_test_bytes([0xa1; 16]);
    let relay = open_db(0xe0, identity, &schema);
    let relay_node = NodeUuid::from_bytes([0xe0; 16]);
    let (a_transport, _a_peer) = duplex_with_admitted_session_context(
        identity,
        relay_node,
        10,
        NodeUuid::from_bytes([0xa2; 16]),
        20,
    );
    let a = crate::db::block_on(relay.node.connect_upstream(a_transport));
    let first = *relay.node.admitted_upstream_authority.borrow();
    let (b_transport, _b_peer) = duplex_with_admitted_session_context(
        identity,
        relay_node,
        11,
        NodeUuid::from_bytes([0xb2; 16]),
        21,
    );
    let _b = crate::db::block_on(relay.node.connect_upstream(b_transport));
    assert_eq!(
        *relay.node.admitted_upstream_authority.borrow(),
        first,
        "a concurrent admitted upstream must not steal existing route ownership"
    );
    assert_eq!(relay.node.admitted_upstream_authorities.borrow().len(), 2);
    assert!(relay.node.detach_connection(&a));
    assert_ne!(
        *relay.node.admitted_upstream_authority.borrow(),
        first,
        "detaching the selected owner must deterministically hand off future routes"
    );
    assert_eq!(relay.node.admitted_upstream_authorities.borrow().len(), 1);
}

#[test]
fn missing_read_policy_advice_denies_with_an_explicit_zero_clause_receipt() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid),
        ),
    );
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

    assert_eq!(block_on(advice), PermissionAdvice::Denied);
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

/// The row lookup must find an existing, readable row: only an `Allowed`
/// answer distinguishes a correct lookup from one that reports every row as
/// missing, since both a violating patch and a missing row are `Denied`.
#[test]
fn permission_advice_update_allows_a_valid_patch_to_an_existing_row() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_update(None, public_literal_eq("done", PublicValue::Boolean(false))),
                ),
        ),
    );
    let author = AuthorSubject::for_test_bytes([0xa4; 16]);
    let server = open_core(0x61, AuthorSubject::SYSTEM, &schema);
    for title in ["other-1", "other-2"] {
        server.insert("todos", cells(title, false, author)).unwrap();
    }
    let target = server
        .insert("todos", cells("target", false, author))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa4, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa4; 16]),
        1,
        NodeUuid::from_bytes([0x61; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    let mut ask = |row, patch| {
        let advice = client.request_permission_advice(PermissionAdviceAction::Update {
            table: "todos".to_owned(),
            row,
            patch,
        });
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        block_on(advice)
    };
    let rename = || BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]);

    assert_eq!(ask(target, rename()), PermissionAdvice::Allowed);
    assert_eq!(
        ask(
            target,
            BTreeMap::from([("done".to_owned(), Value::Bool(true))])
        ),
        PermissionAdvice::Denied
    );
    assert_eq!(ask(row(0xef), rename()), PermissionAdvice::Denied);
}

/// With an allow-all update policy, row existence alone decides update
/// advice: a live row is Allowed, while a row that never existed or was
/// deleted is Denied. Guards the #3386 point lookup against treating an
/// absent or deleted row as present.
#[test]
fn permission_advice_update_denies_missing_and_deleted_rows_under_allow_all_policy() {
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(
                    PublicTablePolicies::new()
                        .with_select(PublicPolicyExpr::True)
                        .with_insert(PublicPolicyExpr::True)
                        .with_update(Some(PublicPolicyExpr::True), PublicPolicyExpr::True)
                        .with_delete(PublicPolicyExpr::True),
                ),
        ),
    );
    let author = AuthorSubject::for_test_bytes([0xa5; 16]);
    let server = open_core(0x62, AuthorSubject::SYSTEM, &schema);
    let live = server
        .insert("todos", cells("live", false, author))
        .unwrap()
        .row_uuid();
    let deleted = server
        .insert("todos", cells("deleted", false, author))
        .unwrap()
        .row_uuid();
    let client = open_db(0xa5, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa5; 16]),
        1,
        NodeUuid::from_bytes([0x62; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    let ask = |row| {
        let advice = client.request_permission_advice(PermissionAdviceAction::Update {
            table: "todos".to_owned(),
            row,
            patch: BTreeMap::from([("title".to_owned(), Value::String("renamed".to_owned()))]),
        });
        client.tick().unwrap();
        server.tick().unwrap();
        client.tick().unwrap();
        block_on(advice)
    };

    assert_eq!(ask(live), PermissionAdvice::Allowed);
    assert_eq!(
        ask(deleted),
        PermissionAdvice::Allowed,
        "live before deletion"
    );
    assert_eq!(ask(row(0xee)), PermissionAdvice::Denied, "never existed");

    let _ = client.delete("todos", deleted, Default::default()).unwrap();
    for _ in 0..3 {
        client.tick().unwrap();
        server.tick().unwrap();
    }
    assert_eq!(ask(deleted), PermissionAdvice::Denied, "deleted");
    assert_eq!(
        ask(live),
        PermissionAdvice::Allowed,
        "unrelated row stays live"
    );
}

#[test]
#[ignore = "#3386: timing probe, run manually with --ignored"]
/// Server tick for one Update permission advice against a growing table,
/// next to an idle tick. Before #3386 the row-existence check decoded the
/// whole table, so the advice tick grew linearly with it.
fn probe_3386_update_advice_latency() {
    let policy = public_literal_eq("done", PublicValue::Boolean(false));
    // No update policy: an update policy's support scope is hydrated over
    // the whole table on every request and would mask the row lookup.
    let policies = PublicTablePolicies::new().with_insert(policy);
    let schema = build_public_db_test_schema(
        PublicSchemaBuilder::new().table(
            PublicTableSchemaBuilder::new("todos")
                .column("title", PublicColumnType::Text)
                .column("done", PublicColumnType::Boolean)
                .column("owner", PublicColumnType::Uuid)
                .policies(policies),
        ),
    );
    let author = AuthorSubject::for_test_bytes([0xa3; 16]);
    let server = open_core(0x60, AuthorSubject::SYSTEM, &schema);
    let client = open_db(0xa3, author, &schema);
    let (client_transport, server_transport) = duplex_with_admitted_session_context(
        author,
        NodeUuid::from_bytes([0xa3; 16]),
        1,
        NodeUuid::from_bytes([0x60; 16]),
        1,
    );
    let _upstream = crate::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, author);
    let mut inserted = 0usize;
    for size in [100usize, 1_000, 5_000] {
        while inserted < size {
            server
                .insert("todos", cells(&format!("row {inserted}"), false, author))
                .unwrap();
            inserted += 1;
        }
        let mut total = std::time::Duration::ZERO;
        const ASKS: u32 = 20;
        for _ in 0..ASKS {
            let advice = client.request_permission_advice(PermissionAdviceAction::Update {
                table: "todos".to_owned(),
                row: row(0xef),
                patch: BTreeMap::from([("done".to_owned(), Value::Bool(false))]),
            });
            client.tick().unwrap();
            let started = std::time::Instant::now();
            server.tick().unwrap();
            total += started.elapsed();
            client.tick().unwrap();
            assert_eq!(block_on(advice), PermissionAdvice::Denied);
        }
        eprintln!(
            "PROBE rows={size} server_tick_us={}",
            (total / ASKS).as_micros()
        );
        let mut idle = std::time::Duration::ZERO;
        for _ in 0..ASKS {
            client.tick().unwrap();
            let started = std::time::Instant::now();
            server.tick().unwrap();
            idle += started.elapsed();
        }
        eprintln!(
            "PROBE rows={size} idle_tick_us={}",
            (idle / ASKS).as_micros()
        );
    }
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

// Internal: the public API intentionally cannot forge raw SessionClaims or
// host-admitted transport trust. Pin both retained evaluator bindings here;
// real concurrent claim-gated reads are covered by napi.for-request.test.ts.
#[test]
fn delegated_subscription_binding_survives_backend_raw_claim_refresh() {
    let schema = owner_read_schema();
    let delegated_identity = AuthorSubject::for_test_bytes([0xb3; 16]);
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
    let subscriber = server.accept_subscriber_with_trust(
        server_transport,
        AuthorSubject::SYSTEM,
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
    let later_claims = BTreeMap::from([(
        crate::query::provider_claim_key("sub"),
        Value::Uuid(AuthorSubject::for_test_bytes([0xb2; 16]).test_uuid()),
    )]);
    relay_transport
        .send(SyncMessage::SessionClaims {
            identity: delegated_identity,
            claims: later_claims.clone(),
        })
        .unwrap();
    for _ in 0..8 {
        subscriber.borrow_mut().tick().unwrap();
    }
    assert_eq!(
        server
            .node()
            .borrow()
            .session_claims_for(delegated_identity),
        later_claims,
        "the trusted backend compatibility update must actually be admitted"
    );
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
        "refreshing the backend author claims must not retarget a delegated usage site"
    );
}

// Internal admission boundary shared by subscriptions, body repair and advice:
// provider metadata must exactly match the immutable server-authenticated scope.
#[test]
fn scoped_relay_request_rejects_mismatched_provider_claims() {
    let schema = owner_read_schema();
    let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
    let author = AuthorSubject::for_test_bytes([0xb4; 16]);
    let claims = BTreeMap::from([(
        crate::query::provider_claim_key("role"),
        Value::String("user".into()),
    )]);
    let (_client, transport) = duplex();
    let subscriber =
        server
            .server
            .accept_scope_isolated_relay_subscriber(transport, author, claims.clone(), 1);
    let connection = subscriber.borrow();
    let ConnectionLink::Subscriber(state) = &connection.link else {
        unreachable!()
    };
    for requested in [
        BTreeMap::new(),
        BTreeMap::from([(
            crate::query::provider_claim_key("role"),
            Value::String("admin".into()),
        )]),
        claims.clone(),
    ] {
        let exact = requested == claims;
        let admitted = admitted_request_policy_binding(
            state.ingest_context,
            &state.peer,
            None,
            Some(crate::protocol::DelegatedSessionBinding {
                identity: author,
                claims: requested,
            }),
        );
        assert_eq!(
            admitted.is_some(),
            exact,
            "only the exact provider claim snapshot is admitted"
        );
    }
}

// Internal: only host admission can select connection trust. Raw wire claims
// must not grant request delegation to a session, authority, or admin link.
#[test]
fn delegated_request_binding_requires_backend_client_link() {
    for trust in [
        CommitUnitTrust::Session,
        CommitUnitTrust::TrustedAuthority,
        CommitUnitTrust::TrustedAdmin,
    ] {
        let schema = owner_read_schema();
        let server = open_core(0x5e, AuthorSubject::SYSTEM, &schema);
        let shape = Query::from("todos").validate(&schema).unwrap();
        let binding = shape.bind(BTreeMap::new()).unwrap();
        let subscription = SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: RegisterShapeOptions::default().read_view_key(),
        };
        let (mut client, transport) = duplex();
        let subscriber = server.accept_subscriber_with_trust(
            transport,
            AuthorSubject::for_test_bytes([0xb4; 16]),
            trust,
        );
        client
            .send(SyncMessage::RegisterShape {
                shape_id: shape.shape_id(),
                ast: ShapeAst::from_validated(&shape),
                opts: RegisterShapeOptions::default(),
            })
            .unwrap();
        client
            .send(SyncMessage::Subscribe(Subscribe {
                shape_id: shape.shape_id(),
                subscription,
                values: Vec::new(),
                known_state: None,
                delegated_session: Some(crate::protocol::DelegatedSessionBinding {
                    identity: AuthorSubject::SYSTEM,
                    claims: BTreeMap::new(),
                }),
            }))
            .unwrap();
        for _ in 0..8 {
            subscriber.borrow_mut().tick().unwrap();
        }
        let connection = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!()
        };
        assert!(
            state.served.is_empty(),
            "{trust:?} must not admit delegated queries"
        );
    }
}

// Internal: the capability is a host-only admission event, never a wire field.
#[test]
fn authority_query_delegation_requires_explicit_host_admission() {
    for trust in [
        CommitUnitTrust::TrustedAuthority,
        CommitUnitTrust::TrustedAdmin,
    ] {
        let schema = owner_read_schema();
        let server = open_core(0x6e, AuthorSubject::SYSTEM, &schema);
        let (_, transport) = duplex();
        let subscriber =
            server.accept_subscriber_with_trust(transport, AuthorSubject::SYSTEM, trust);
        subscriber.borrow_mut().admit_authority_query_delegate();
        let connection = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!()
        };
        let binding = admitted_request_policy_binding(
            state.ingest_context,
            &state.peer,
            None,
            Some(crate::protocol::DelegatedSessionBinding {
                identity: AuthorSubject::for_test_bytes([0x73; 16]),
                claims: BTreeMap::new(),
            }),
        );
        assert_eq!(
            binding.is_some(),
            trust == CommitUnitTrust::TrustedAuthority
        );
    }
}

// Internal transport fixture: only host admission can mark subscriber trust.
// Observe raw native delivery/rejection because a client facade cannot express
// the unsupported remote propagation option or delegated transport scope.
#[derive(Clone, Copy, Debug)]
enum QueryTestClient {
    Session,
    System,
    Delegated,
}

fn remote_query_delivery(
    propagate_upstream: bool,
    tier: DurabilityTier,
    client_scope: QueryTestClient,
) -> (bool, bool) {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0x75; 16]);
    let core = open_core(0x76, AuthorSubject::SYSTEM, &schema);
    let target = row(0x77);
    core.insert_with_id(
        "todos",
        target,
        cells("unverified shared bytes", false, alice),
    )
    .unwrap();
    let forbidden = row(0x78);
    core.insert_with_id(
        "todos",
        forbidden,
        cells(
            "another reader",
            false,
            AuthorSubject::for_test_bytes([0x79; 16]),
        ),
    )
    .unwrap();
    let shape = Query::from("todos").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    let opts = RegisterShapeOptions {
        tier,
        propagate_upstream,
        ..RegisterShapeOptions::default()
    };
    let subscription = SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: opts.read_view_key(),
    };
    let (mut client, transport) = duplex();
    let delegated = matches!(client_scope, QueryTestClient::Delegated);
    let subscriber = if delegated {
        let subscriber = core.accept_subscriber_with_trust(
            transport,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedAuthority,
        );
        subscriber.borrow_mut().admit_authority_query_delegate();
        subscriber
    } else if matches!(client_scope, QueryTestClient::System) {
        core.accept_subscriber_with_trust(
            transport,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedBackend,
        )
    } else {
        core.accept_subscriber(transport, alice)
    };
    client
        .send(SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        })
        .unwrap();
    let delegated_session = delegated.then(|| crate::protocol::DelegatedSessionBinding {
        identity: alice,
        claims: test_provider_claims(alice),
    });
    let request = SyncMessage::Subscribe(Subscribe {
        shape_id: shape.shape_id(),
        subscription,
        values: Vec::new(),
        known_state: None,
        delegated_session,
    });
    if !propagate_upstream {
        client.send(request.clone()).unwrap();
    }
    client.send(request).unwrap();
    let mut emitted = false;
    let mut rejected = false;
    for _ in 0..32 {
        subscriber.borrow_mut().tick().unwrap();
        while let Some(message) = client.try_recv() {
            rejected |= matches!(message, SyncMessage::SubscribeRejected { .. });
            if let SyncMessage::ViewUpdate(view) = message {
                for carrier in view.version_carriers {
                    for bundle in carrier.bundle_refs().unwrap() {
                        if !matches!(client_scope, QueryTestClient::System) {
                            assert!(
                                !bundle
                                    .versions
                                    .iter()
                                    .any(|version| version.row_uuid() == forbidden),
                                "local Core evaluation must narrow payloads under the admitted reader"
                            );
                        }
                        emitted |= bundle
                            .versions
                            .iter()
                            .any(|version| version.row_uuid() == target);
                    }
                }
            }
        }
    }
    if rejected {
        let connection = subscriber.borrow();
        let ConnectionLink::Subscriber(state) = &connection.link else {
            unreachable!()
        };
        assert!(state.served.is_empty());
        assert!(state.coverage_groups.is_empty());
    }
    (emitted, rejected)
}

#[test]
fn remote_queries_cannot_disable_upstream_propagation() {
    for client in [
        QueryTestClient::Session,
        QueryTestClient::System,
        QueryTestClient::Delegated,
    ] {
        assert_eq!(
            remote_query_delivery(false, DurabilityTier::Global, client),
            (false, true),
            "{client:?}"
        );
    }
}

// Rust equivalent of a memory-only browser foreground: LocalOnly can read its
// own pending data but may not ask the worker (or any node) for a local view.
#[test]
fn foreground_local_only_reads_never_emit_remote_query_requests() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0x78; 16]);
    let foreground = open_db(0x79, alice, &schema);
    foreground.set_non_durable_client();
    let target = row(0x7a);
    foreground
        .insert(
            "todos",
            cells("foreground pending", false, alice),
            crate::db::InsertOptions {
                row_id: Some(target),
                identity: crate::db::WriteIdentity::Attribution(alice),
                ..Default::default()
            },
        )
        .unwrap();
    let (transport, mut remote) = duplex();
    let _upstream = block_on(foreground.connect_upstream(transport));
    let query = Query::from("todos");
    let prepared = foreground.prepare_query(&query).unwrap();
    let opts = ReadOpts {
        tier: DurabilityTier::Local,
        propagation: Propagation::LocalOnly,
        ..ReadOpts::default()
    };
    let attachment = foreground
        .attach_query_with_opts(&prepared, opts.clone())
        .unwrap();
    let second = foreground
        .attach_query_with_opts_for_identity(&prepared, opts.clone(), alice)
        .unwrap();
    let third =
        block_on(foreground.attach_query_with_opts_async(&prepared, opts.clone(), None, None))
            .unwrap();
    assert_ne!(attachment.subscription(), second.subscription());
    assert_ne!(second.subscription(), third.subscription());
    assert!(foreground.query_attachment_is_covered(&attachment));
    assert_eq!(
        prepared_all(&foreground, &query, opts.clone())
            .iter()
            .map(|r| r.row_uuid())
            .collect::<Vec<_>>(),
        vec![target]
    );
    let mut stream = prepared_subscribe(&foreground, &query, opts).unwrap();
    let mut snapshot = RelationSnapshot::default();
    for _ in 0..16 {
        foreground.tick().unwrap();
        while let Some(event) = stream.try_next_event() {
            apply_subscription_event(&mut snapshot, event);
        }
        while let Some(message) = remote.try_recv() {
            assert!(
                !matches!(
                    message,
                    SyncMessage::RegisterShape { .. } | SyncMessage::Subscribe(_)
                ),
                "LocalOnly emitted a remote query"
            );
        }
    }
    assert_eq!(snapshot.root_count, 1);
    foreground.detach_query(attachment);
    foreground.detach_query(second);
    foreground.detach_query(third);
}

// A worker's local_receiver role is still a node boundary, not an in-process
// read API. Even a trusted foreground cannot send the local-only wire option.
#[test]
fn scope_relay_remote_registration_cannot_disable_propagation() {
    let schema = owner_read_schema();
    let alice = AuthorSubject::for_test_bytes([0x7b; 16]);
    let worker = open_db(0x7c, alice, &schema);
    worker.set_relay_authority_session_owner_for_test();
    let shape = Query::from("todos").validate(&schema).unwrap();
    for (identity, trust) in [
        (alice, CommitUnitTrust::Session),
        (AuthorSubject::SYSTEM, CommitUnitTrust::TrustedBackend),
    ] {
        let (mut client, transport) = duplex();
        let subscriber = worker
            .node
            .accept_subscriber_with_trust(transport, identity, trust);
        client
            .send(SyncMessage::RegisterShape {
                shape_id: shape.shape_id(),
                ast: ShapeAst::from_validated(&shape),
                opts: RegisterShapeOptions {
                    tier: DurabilityTier::Local,
                    propagate_upstream: false,
                    ..RegisterShapeOptions::default()
                },
            })
            .unwrap();
        let mut rejected = false;
        for _ in 0..8 {
            subscriber.borrow_mut().tick().unwrap();
            while let Some(message) = client.try_recv() {
                rejected |= matches!(message, SyncMessage::SubscribeRejected { .. });
            }
        }
        assert!(rejected);
    }
}
