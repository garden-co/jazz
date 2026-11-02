//! Public accepted reads carried over normal sync transports. Provenance
//! assertions for relayed writes remain in authority_accepted_content.
use crate::common::{allow_all_policies, compile_schema, read_and_allow_all_writes};
use jazz::db::{
    AcceptedReadScope, Db, DbConfig, DbIdentity, EncodedAcceptedRead, InsertOptions, SyncMessage,
    Transport, TransportError, block_on, duplex,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, provider_claim_key};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};
use std::{
    cell::RefCell,
    collections::BTreeMap,
    future::Future,
    pin::pin,
    rc::Rc,
    task::{Context, Poll, Waker},
};

pub(super) fn schema(scoped: bool) -> JazzSchema {
    let policies = if scoped {
        read_and_allow_all_writes(PolicyExpr::And(vec![
            PolicyExpr::eq_session("owner", vec!["user".into(), "account".into()]),
            PolicyExpr::eq_session("token", vec!["claims".into(), "token".into()]),
        ]))
    } else {
        allow_all_policies()
    };
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
                    .column("token", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
}
pub(super) fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([1; 16])
}
pub(super) fn bob() -> AuthorSubject {
    AuthorSubject::for_test_bytes([2; 16])
}
pub(super) fn open(node: u8, author: AuthorSubject, authority: bool, scoped: bool) -> Db {
    let db = block_on(Db::open(DbConfig::new(
        schema(scoped),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    } else {
        db.become_replica();
    }
    db
}
pub(super) fn claims(token: &str) -> BTreeMap<String, Value> {
    [(provider_claim_key("token"), Value::String(token.into()))].into()
}
pub(super) fn seed(db: &Db, n: u8, owner: AuthorSubject, token: &str) {
    block_on(
        db.insert(
            "notes",
            [
                ("body".into(), Value::String(format!("note{n}"))),
                ("owner".into(), Value::Uuid(owner.account_id().unwrap().0)),
                ("token".into(), Value::String(token.into())),
            ]
            .into(),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n; 16])),
                ..Default::default()
            },
        ),
    )
    .unwrap();
}
pub(super) fn tick(nodes: &[&Db]) {
    for db in nodes {
        block_on(db.tick()).unwrap();
    }
}
pub(super) fn drive<T>(nodes: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    for _ in 0..100 {
        if let Poll::Ready(value) = future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            return value;
        }
        tick(nodes);
    }
    panic!("accepted read did not complete");
}
pub(super) fn bodies(read: &EncodedAcceptedRead) -> Vec<String> {
    jazz::binding_codec::decode_rows(&read.rows)
        .unwrap()
        .iter()
        .flat_map(|batch| &batch.rows)
        .map(|row| match &row.cells["body"] {
            Value::String(body) => body.clone(),
            other => panic!("{other:?}"),
        })
        .collect()
}

/// Alice's cold foreground gets the authority's complete body/evidence
/// result directly and through a relay, at the same accepted coordinate.
#[test]
fn cold_reads_cross_direct_and_relay_links() {
    for relayed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        seed(&server, 1, alice(), "a");
        seed(&server, 2, bob(), "b");
        let relay = open(8, AuthorSubject::SYSTEM, false, false);
        let client = open(7, alice(), false, false);
        let (up, down) = duplex();
        block_on(client.connect_upstream(up));
        if relayed {
            relay.accept_subscriber(down, alice()).unwrap();
            let (up, down) = duplex();
            block_on(relay.connect_upstream(up));
            server
                .accept_subscriber(down, AuthorSubject::SYSTEM)
                .unwrap();
        } else {
            server.accept_subscriber(down, alice()).unwrap();
        }
        let query = client.prepare_query(&Query::from("notes")).unwrap();
        let read = drive(
            &[&client, &relay, &server],
            client.accepted_query(&query, None, false, None),
        )
        .unwrap();
        assert_eq!(bodies(&read), ["note1", "note2"]);
        assert_eq!(read.evidence.len(), 2);
        assert_eq!(read.scope.author, alice());
    }
}

/// A trusted backend vouches for Alice and her immutable claims. The
/// authority evaluates that scope, returning none of Bob's private rows.
#[test]
fn delegated_backend_reads_are_scoped_at_the_authority() {
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server, 1, alice(), "a");
    seed(&server, 2, bob(), "b");
    let backend = open(8, AuthorSubject::SYSTEM, false, true);
    let (up, down) = duplex();
    block_on(backend.connect_upstream(up));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    let query = backend.prepare_query(&Query::from("notes")).unwrap();
    let scope = AcceptedReadScope {
        author: alice(),
        claims: claims("a"),
    };
    let read = drive(
        &[&backend, &server],
        backend.accepted_query(&query, None, false, Some(scope.clone())),
    )
    .unwrap();
    assert_eq!(bodies(&read), ["note1"]);
    assert_eq!(read.scope, scope);
}

/// Mallory cannot replace her authenticated connection identity or claims
/// by putting Alice's identity in an accepted-read request.
#[test]
fn ordinary_links_cannot_forge_delegated_scope() {
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server, 1, alice(), "a");
    seed(&server, 2, bob(), "b");
    let mallory = open(7, bob(), false, true);
    let (up, down) = duplex();
    block_on(mallory.connect_upstream(up));
    server
        .accept_subscriber_with_claims(down, bob(), Some(claims("b")))
        .unwrap();
    let query = mallory.prepare_query(&Query::from("notes")).unwrap();
    let read = drive(
        &[&mallory, &server],
        mallory.accepted_query(
            &query,
            None,
            false,
            Some(AcceptedReadScope {
                author: alice(),
                claims: claims("a"),
            }),
        ),
    )
    .unwrap();
    assert_eq!(bodies(&read), ["note2"]);
    assert_eq!(
        read.scope,
        AcceptedReadScope {
            author: bob(),
            claims: claims("b")
        }
    );
}

/// Alice's absent connection claims inherit the serving host's snapshot;
/// explicit empty or different claims stay authoritative on both hops.
#[test]
fn relay_reads_distinguish_absent_empty_and_different_connection_claims() {
    for relayed in [false, true] {
        for connection in [None, Some(BTreeMap::new()), Some(claims("other"))] {
            let server = open(9, AuthorSubject::SYSTEM, true, true);
            seed(&server, 1, alice(), "host");
            seed(&server, 2, alice(), "other");
            let relay = open(8, AuthorSubject::SYSTEM, false, true);
            let client = open(7, alice(), false, true);
            let host = if relayed { &relay } else { &server };
            host.set_identity_claims(alice(), claims("host"));
            let (up, down) = duplex();
            block_on(client.connect_upstream(up));
            host.accept_subscriber_with_claims(down, alice(), connection.clone())
                .unwrap();
            if relayed {
                let (up, down) = duplex();
                block_on(relay.connect_upstream(up));
                server
                    .accept_subscriber(down, AuthorSubject::SYSTEM)
                    .unwrap();
            }
            let query = client.prepare_query(&Query::from("notes")).unwrap();
            let read = drive(
                &[&client, &relay, &server],
                client.accepted_query(&query, None, false, None),
            )
            .unwrap();
            let expected = match &connection {
                None => vec!["note1"],
                Some(c) if c.is_empty() => vec![],
                Some(_) => vec!["note2"],
            };
            assert_eq!(bodies(&read), expected);
        }
    }
}

/// Alice's relay cannot invent authority evidence while disconnected.
/// The original pending read completes after the authority reconnects.
#[test]
fn offline_relay_waits_for_a_real_authority_result() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let relay = open(8, AuthorSubject::SYSTEM, false, false);
    let client = open(7, alice(), false, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let mut read = pin!(client.accepted_query(&query, None, false, None));
    for _ in 0..8 {
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        tick(&[&client, &relay]);
    }
    let (up, down) = duplex();
    block_on(relay.connect_upstream(up));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    assert_eq!(
        bodies(&drive(&[&client, &relay, &server], read).unwrap()),
        ["note1"]
    );
}

/// Alice reads at an earlier authority cut after Bob changes that row.
/// The returned body, source version and content settlement stay paired.
#[test]
fn remote_snapshot_does_not_take_a_later_content_version() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let client = open(7, alice(), false, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let first = drive(
        &[&client, &server],
        client.accepted_query(&query, None, false, None),
    )
    .unwrap();
    block_on(server.update(
        "notes",
        RowUuid::from_bytes([1; 16]),
        [("body".into(), Value::String("later".into()))].into(),
        Default::default(),
    ))
    .unwrap();
    let earlier = drive(
        &[&client, &server],
        client.accepted_query(&query, Some(first.through), false, None),
    )
    .unwrap();
    assert_eq!(earlier, first);
}

struct Tap {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<Vec<SyncMessage>>>,
}
impl Transport for Tap {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push(message.clone());
        self.inner.send(message)
    }
    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}
/// Alice cancels a request before its first send. The transport sees no
/// accepted read; this protocol tap is necessary to observe queued work.
#[test]
fn cancellation_removes_an_unsent_request() {
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    let client = open(7, alice(), false, false);
    let sent = Rc::new(RefCell::new(Vec::new()));
    let (up, down) = duplex();
    block_on(client.connect_upstream(Box::new(Tap {
        inner: up,
        sent: sent.clone(),
    })));
    server.accept_subscriber(down, alice()).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    {
        let mut read = pin!(client.accepted_query(&query, None, false, None));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
    }
    tick(&[&client, &server]);
    assert!(!sent.borrow().iter().any(|message| matches!(
        message,
        SyncMessage::Read {
            span: jazz::db::ReadSpan::AcceptedOnce { .. },
            ..
        }
    )));
}

/// Alice's direct and relayed reads preserve both origins after a deletion,
/// and neither a populated nor an empty query crosses Bob's unfinished hold.
/// Alice -> relay? -> Bob: read the settled prefix, then refuse a future cut.
#[test]
fn every_authority_route_preserves_the_settled_prefix_and_paired_origins() {
    use jazz::db::{
        ErrorCode,
        shard::{CommitRef, Held},
    };
    for relayed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        seed(&server, 1, alice(), "a");
        block_on(server.delete("notes", RowUuid::from_bytes([1; 16]), Default::default())).unwrap();
        let part = CommitRef {
            node: [44; 16],
            tx: 1,
        };
        let Held::At(held) = server.shard().hold(part, &[], b"unfinished part").unwrap() else {
            panic!("fresh part must be held");
        };
        seed(&server, 2, bob(), "b");
        let relay = open(8, AuthorSubject::SYSTEM, false, false);
        let client = open(7, alice(), false, false);
        let (up, down) = duplex();
        block_on(client.connect_upstream(up));
        if relayed {
            relay.accept_subscriber(down, alice()).unwrap();
            let (up, down) = duplex();
            block_on(relay.connect_upstream(up));
            server
                .accept_subscriber(down, AuthorSubject::SYSTEM)
                .unwrap();
        } else {
            server.accept_subscriber(down, alice()).unwrap();
        }
        let nodes = [&client, &relay, &server];
        let query = client.prepare_query(&Query::from("notes")).unwrap();
        let read = drive(&nodes, client.accepted_query(&query, None, true, None)).unwrap();
        assert_eq!(read.through, held - 1);
        assert_eq!(bodies(&read), ["note1"]);
        assert_ne!(
            read.evidence[0].state.transaction,
            read.evidence[0].content.transaction
        );
        assert!(read.evidence[0].state.sequence > read.evidence[0].content.sequence);
        let empty = client
            .prepare_query(&Query::from("notes").limit(0))
            .unwrap();
        let read = drive(&nodes, client.accepted_query(&empty, None, false, None)).unwrap();
        assert_eq!(read.through, held - 1);
        assert!(read.evidence.is_empty());
        for query in [&query, &empty] {
            assert_eq!(
                drive(&nodes, client.accepted_query(query, Some(held), true, None))
                    .unwrap_err()
                    .code,
                ErrorCode::NotObserved
            );
        }
    }
}

/// Alice's relay freezes inherited claims before waiting for its authority.
/// Changing the host map afterwards cannot relabel that pending request.
#[test]
fn relay_scope_is_frozen_before_reconnect() {
    let server = open(9, AuthorSubject::SYSTEM, true, true);
    seed(&server, 1, alice(), "first");
    seed(&server, 2, alice(), "later");
    let relay = open(8, AuthorSubject::SYSTEM, false, true);
    relay.set_identity_claims(alice(), claims("first"));
    let client = open(7, alice(), false, true);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let mut read = pin!(client.accepted_query(&query, None, false, None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tick(&[&client, &relay]);
    relay.set_identity_claims(alice(), claims("later"));
    let (up, down) = duplex();
    block_on(relay.connect_upstream(up));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    let result = drive(&[&client, &relay, &server], read).unwrap();
    assert_eq!(result.scope.claims, claims("first"));
    assert_eq!(bodies(&result), ["note1"]);
}

/// Alice cancels an issued read while the relay has no authority connection.
/// Its ordinary request slot and queued upstream request are both released.
#[test]
fn cancellation_releases_an_issued_relay_request() {
    let client = open(7, alice(), false, false);
    let relay = open(8, AuthorSubject::SYSTEM, false, false);
    let (up, down) = duplex();
    block_on(client.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let mut read = Box::pin(client.accepted_query(&query, None, false, None));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    tick(&[&client, &relay]);
    assert_eq!(relay.pending_once_state_for_test(), (1, 1, 1));
    drop(read);
    tick(&[&client, &relay]);
    assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
}

/// A disconnected subscriber's queued evidence cannot reach a new subscriber
/// reusing its relay slot. This exercises the core subscriber-close contract;
/// native host close/replacement is covered by the separate host bridge.
#[test]
fn subscriber_replacement_drops_old_queued_evidence() {
    let relay = open(8, AuthorSubject::SYSTEM, false, false);
    let (mut alice_link, down) = duplex();
    let old = relay.accept_subscriber(down, alice()).unwrap();
    alice_link
        .send(SyncMessage::Read {
            id: 42,
            query: Box::new(Query::from("notes")),
            binding: BTreeMap::new(),
            include_deleted: false,
            tier: jazz::db::ReadTier::Remote,
            wait: None,
            view: Box::new(jazz::protocol::ReadViewSourceSpec::Current),
            since: None,
            span: jazz::db::ReadSpan::AcceptedOnce {
                at: None,
                schema: *schema(false).version_id().as_bytes(),
                scope: None,
            },
        })
        .unwrap();
    tick(&[&relay]);
    assert_eq!(relay.pending_once_state_for_test(), (1, 1, 1));
    relay.disconnect_subscriber(old);
    assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
    let (mut bob_link, down) = duplex();
    relay.accept_subscriber(down, bob()).unwrap();
    tick(&[&relay]);
    while let Some(message) = bob_link.try_recv() {
        assert!(!matches!(
            message,
            SyncMessage::Accepted { .. } | SyncMessage::Refused { id: 42, .. }
        ));
    }
}

#[derive(Default)]
struct WakeCount(std::sync::atomic::AtomicUsize);
impl std::task::Wake for WakeCount {
    fn wake(self: std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
    fn wake_by_ref(self: &std::sync::Arc<Self>) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
#[derive(Default)]
struct DeadlineHost {
    delays: RefCell<Vec<u64>>,
}
impl jazz::db::TickScheduler for DeadlineHost {
    fn schedule_tick(&self, _: jazz::db::TickUrgency) {}
    fn schedule_tick_after(&self, delay_ms: u64) {
        self.delays.borrow_mut().push(delay_ms);
    }
}

/// Alice's quiet disconnected read schedules a host timer. One timer-driven
/// tick wakes the task at its deadline, without traffic or repeated polling.
/// This is the core scheduler contract, not NAPI/WASM timer qualification.
#[test]
fn a_deadline_tick_wakes_the_read_without_unrelated_traffic() {
    let client = open(7, alice(), false, false);
    let host = Rc::new(DeadlineHost::default());
    client.set_tick_scheduler(Some(host.clone()));
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let wakes = std::sync::Arc::new(WakeCount::default());
    let waker = Waker::from(wakes.clone());
    let mut read = Box::pin(client.accepted_query_with_timeout(&query, None, false, None, 10));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(&waker))
            .is_pending()
    );
    let delay = *host
        .delays
        .borrow()
        .last()
        .expect("a real delayed tick was requested");
    std::thread::sleep(std::time::Duration::from_millis(delay + 5));
    assert_eq!(wakes.0.load(std::sync::atomic::Ordering::SeqCst), 0);
    tick(&[&client]);
    assert!(wakes.0.load(std::sync::atomic::Ordering::SeqCst) > 0);
    let Poll::Ready(Err(error)) = read.as_mut().poll(&mut Context::from_waker(&waker)) else {
        panic!("deadline did not finish the read")
    };
    assert_eq!(error.code, jazz::db::ErrorCode::NotObserved);
    drop(read);
    host.delays.borrow_mut().clear();
    tick(&[&client]);
    assert!(host.delays.borrow().is_empty());
    assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
}

/// Alice keeps an old schema handle while Bob publishes a rename. Both
/// direct and relayed evidence retain that handle's schema and result names.
#[test]
fn accepted_reads_keep_the_requested_schema_handle_through_a_rename() {
    use jazz::protocol::{LensOp, MigrationLens, TableLens};
    for relayed in [false, true] {
        let server = open(9, AuthorSubject::SYSTEM, true, false);
        seed(&server, 1, alice(), "a");
        let client = open(7, alice(), false, false);
        let relay = open(8, AuthorSubject::SYSTEM, false, false);
        let view = block_on(client.register_schema_view(schema(false))).unwrap();
        let query = view.prepare_query(&Query::from("notes")).unwrap();
        let mut read = Box::pin(view.accepted_query(&query, None, false, None));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        let renamed = compile_schema(
            &SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("tasks")
                        .column("title", ColumnType::Text)
                        .column("owner", ColumnType::Uuid)
                        .column("token", ColumnType::Text)
                        .policies(allow_all_policies()),
                )
                .build(),
        );
        let lens = MigrationLens::new(
            schema(false).version_id(),
            renamed.version_id(),
            vec![TableLens {
                source_table: "notes".into(),
                target_table: "tasks".into(),
                ops: vec![LensOp::RenameColumn {
                    from: "body".into(),
                    to: "title".into(),
                }],
            }],
        )
        .unwrap();
        block_on(server.activate_schema_with_lenses(renamed, &[lens])).unwrap();
        let (up, down) = duplex();
        block_on(client.connect_upstream(up));
        if relayed {
            relay.accept_subscriber(down, alice()).unwrap();
            let (up, down) = duplex();
            block_on(relay.connect_upstream(up));
            server
                .accept_subscriber(down, AuthorSubject::SYSTEM)
                .unwrap();
        } else {
            server.accept_subscriber(down, alice()).unwrap();
        }
        let result = drive(&[&client, &relay, &server], read).unwrap();
        assert_eq!(result.schema, *schema(false).version_id().as_bytes());
        assert_eq!(bodies(&result), ["note1"]);
    }
}

struct CancellationHost {
    client: Rc<Db>,
    wakes: std::cell::Cell<usize>,
}
impl jazz::db::TickScheduler for CancellationHost {
    fn schedule_tick(&self, _: jazz::db::TickUrgency) {
        // Re-enter a public read-only API: a notification under Inner's
        // exclusive coverage-callback borrow would panic here.
        self.client.pending_once_state_for_test();
        self.wakes.set(self.wakes.get() + 1);
    }
    fn schedule_tick_after(&self, _: u64) {}
}

fn cancel_from_coverage_callback(issued: bool) {
    use jazz::db::{ReadOpts, ReadTier};
    let client = Rc::new(open(7, alice(), false, false));
    let relay = open(8, AuthorSubject::SYSTEM, false, false);
    let sent = Rc::new(RefCell::new(Vec::new()));
    let (up, down) = duplex();
    block_on(client.connect_upstream(Box::new(Tap {
        inner: up,
        sent: sent.clone(),
    })));
    relay.accept_subscriber(down, alice()).unwrap();
    let host = Rc::new(CancellationHost {
        client: client.clone(),
        wakes: std::cell::Cell::new(0),
    });
    client.set_tick_scheduler(Some(host.clone()));
    let query = client.prepare_query(&Query::from("notes")).unwrap();
    let mut first = Box::pin(client.accepted_query(&query, None, false, None));
    assert!(
        first
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    if issued {
        tick(&[&client, &relay]);
        assert_eq!(relay.pending_once_state_for_test(), (1, 1, 1));
    }
    let cancelled = RefCell::new(Some(first));
    let budget = || {
        drop(cancelled.borrow_mut().take());
        false
    };
    let encoded = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut sibling = Box::pin(client.all_serialized_query(
        &encoded,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        Some(&budget),
    ));
    let wakes = host.wakes.get();
    assert!(
        sibling
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    assert!(
        cancelled.borrow().is_none(),
        "the borrowed-store callback did not cancel the accepted read"
    );
    assert!(
        host.wakes.get() > wakes,
        "cleanup did not request a host drive turn"
    );
    // Only the surviving ordinary read remains in the client's queue/slots.
    assert_eq!(client.pending_once_state_for_test(), (1, 1, 1));
    tick(&[&client, &relay]);
    let server = open(9, AuthorSubject::SYSTEM, true, false);
    seed(&server, 1, alice(), "a");
    let (up, down) = duplex();
    block_on(relay.connect_upstream(up));
    server
        .accept_subscriber(down, AuthorSubject::SYSTEM)
        .unwrap();
    let result = drive(&[&client, &relay, &server], sibling).unwrap();
    let jazz::db::SerializedReadResult::Rows(rows) = result else {
        panic!("ordinary sibling did not return rows");
    };
    assert_eq!(rows.len(), 1);
    tick(&[&client, &relay, &server]);
    assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
    let sent = sent.borrow();
    assert_eq!(
        sent.iter()
            .filter(|message| matches!(
                message,
                SyncMessage::Read {
                    span: jazz::db::ReadSpan::AcceptedOnce { .. },
                    ..
                }
            ))
            .count(),
        usize::from(issued),
        "queued cancellation reached the relay"
    );
    if issued {
        let id = sent
            .iter()
            .find_map(|message| match message {
                SyncMessage::Read {
                    id,
                    span: jazz::db::ReadSpan::AcceptedOnce { .. },
                    ..
                } => Some(*id),
                _ => None,
            })
            .unwrap();
        assert!(sent.iter().any(|message| matches!(message, SyncMessage::Unsubscribe { id: cancelled } if *cancelled == id)));
    }
    client.set_tick_scheduler(None);
}

/// Alice drops an accepted read before sending it, from a coverage callback
/// holding Inner. Only her surviving ordinary read may reach Bob's relay.
#[test]
fn queued_accepted_cancellation_inside_coverage_callback() {
    cancel_from_coverage_callback(false);
}

/// Alice drops an issued accepted read from a coverage callback holding Inner.
/// Bob's disconnected relay must discard its answer slot and upstream queue,
/// and Alice's host wake must run after the borrowed callback has returned.
#[test]
fn issued_accepted_cancellation_inside_coverage_callback() {
    cancel_from_coverage_callback(true);
}
