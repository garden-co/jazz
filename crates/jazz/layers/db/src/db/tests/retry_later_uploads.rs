//! Retry-later answers to uploads (SPEC 4 §4.6, SPEC 8).
//!
//! The authority does not hold a chained write whose pending predecessor has
//! no fate there: it stores nothing and answers `RetryLater`, naming the
//! predecessor. The writer keeps the write pending, sends the predecessor
//! (when it still has it) and the write again after a backoff, in outbox
//! order, and fails the write when the predecessor is lost. These tests
//! reorder or drop a writer's uploads at the link so its chain reaches Core
//! out of order.

use super::*;
use crate::model::test_support::AllowAll;

fn music_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(PublicTableSchemaBuilder::new("tracks").column("title", PublicColumnType::Text))
            .allow_all(),
    )
}

#[derive(Clone)]
struct ManualUploadRetryClock(Rc<Cell<u64>>);

impl UploadRetryClock for ManualUploadRetryClock {
    fn now_ms(&self) -> u64 {
        self.0.get()
    }
}

fn title(value: impl Into<String>) -> RowCells {
    BTreeMap::from([("title".to_owned(), Value::String(value.into()))])
}

fn is_commit_unit_for(message: &SyncMessage, tx_id: TxId) -> bool {
    matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id)
}

/// A writer `node` with a manual upload clock, linked to a fresh Core
/// through a tap on its outbound messages, and one accepted seed row.
struct Fixture {
    core: CoreDb,
    writer: Db,
    clock: Rc<Cell<u64>>,
    uploads: Rc<RefCell<VecDeque<SyncMessage>>>,
    target: RowUuid,
    /// The writer's upstream link and Core's link serving it.
    links: (
        Rc<LocalMutex<PeerConnection>>,
        Rc<LocalMutex<PeerConnection>>,
    ),
}

impl Fixture {
    fn new(node: u8) -> Self {
        let schema = music_schema();
        let author = AuthorSubject::for_test_bytes([node; 16]);
        let core = open_core(node + 1, AuthorSubject::SYSTEM, &schema);
        let writer = open_db(node, author, &schema);
        let clock = Rc::new(Cell::new(10_000));
        writer
            .node
            .set_upload_retry_clock_for_test(Rc::new(ManualUploadRetryClock(Rc::clone(&clock))));
        writer.set_tick_scheduler(Some(Rc::new(RecordingScheduler::default())));
        let (writer_transport, core_transport, uploads) =
            duplex_with_admitted_session_context_and_client_outbound_tap(
                author,
                NodeUuid::from_bytes([node; 16]),
                1,
                NodeUuid::from_bytes([node + 1; 16]),
                1,
            );
        let upstream = crate::local_executor::block_on(writer.connect_upstream(writer_transport));
        let subscriber = core.accept_subscriber(core_transport, author);
        let seed = crate::local_executor::block_on(writer.insert(
            "tracks",
            title("seed"),
            Default::default(),
        ))
        .unwrap();
        let mut fixture = Self {
            core,
            writer,
            clock,
            uploads,
            target: seed.row_uuid(),
            links: (upstream, subscriber),
        };
        fixture.pump_until(16, |fixture| fixture.is_global(seed.tx_id));
        assert!(fixture.is_global(seed.tx_id));
        fixture
    }

    fn edit(&self, value: &str) -> TxId {
        crate::local_executor::block_on(self.writer.update(
            "tracks",
            self.target,
            title(value),
            Default::default(),
        ))
        .unwrap()
        .tx_id
    }

    fn is_global(&self, tx_id: TxId) -> bool {
        self.writer
            .write_state(tx_id)
            .is_ok_and(|state| state.durability == DurabilityTier::Global)
    }

    fn pump_until(&mut self, rounds: usize, done: impl Fn(&Self) -> bool) {
        for _ in 0..rounds {
            if done(self) {
                return;
            }
            self.writer.tick().unwrap();
            self.core.tick().unwrap();
        }
    }

    /// Tick the writer until it has put `tx_id` on the wire.
    fn upload(&self, tx_id: TxId) {
        for _ in 0..8 {
            if self
                .uploads
                .borrow()
                .iter()
                .any(|message| is_commit_unit_for(message, tx_id))
            {
                return;
            }
            self.writer.tick().unwrap();
        }
        panic!("the writer never uploaded {tx_id:?}");
    }

    fn take_upload(&self, tx_id: TxId) -> SyncMessage {
        let mut uploads = self.uploads.borrow_mut();
        let index = uploads
            .iter()
            .position(|message| is_commit_unit_for(message, tx_id))
            .expect("the upload is on the link");
        uploads.remove(index).unwrap()
    }

    fn core_state(&self, tx_id: TxId) -> Option<(Fate, Option<GlobalTime>, DurabilityTier)> {
        crate::local_executor::block_on(self.core.node().borrow_mut().transaction_state(tx_id))
    }
}

/// alice makes 260 chained edits of one row, and her first edit reaches
/// Core last. Core stores nothing for the edits that arrive before their
/// predecessors and answers each with `RetryLater`. alice stops uploading
/// at her first deferred edit, sends her chain again from there after the
/// backoff, and every edit is accepted in order. None is lost.
///
/// ```text
/// alice ══ e1, e2 … e260 (each chained on the one before)
/// link  ── e2 … e260 ──► core   RetryLater each, nothing stored
/// link  ── e1 ─────────► core   e1 accepted
/// alice ── (backoff) e2 … e260 ──► core   accepted   title=e260
/// ```
#[test]
fn chained_writes_reaching_core_out_of_order_converge_after_retry_later() {
    let mut fixture = Fixture::new(0xe1);
    let edits = (1..=260)
        .map(|index| fixture.edit(&format!("e{index}")))
        .collect::<Vec<_>>();
    fixture.upload(*edits.last().unwrap());
    // The link delivers alice's first edit last.
    let first = fixture.take_upload(edits[0]);
    fixture.uploads.borrow_mut().push_back(first);
    fixture.core.tick().unwrap();
    assert_eq!(
        fixture.core_state(edits[0]).map(|(fate, ..)| fate),
        Some(Fate::Accepted)
    );
    for tx_id in &edits[1..] {
        assert_eq!(
            fixture.core_state(*tx_id),
            None,
            "Core stored nothing for it"
        );
        assert_eq!(
            fixture.writer.write_state(*tx_id).unwrap().fate,
            Fate::Pending
        );
    }

    let edits_global = |fixture: &Fixture| edits.iter().all(|tx_id| fixture.is_global(*tx_id));
    fixture.pump_until(32, edits_global);
    fixture.clock.set(fixture.clock.get() + 60_000);
    fixture.pump_until(64, edits_global);
    assert!(edits_global(&fixture), "no edit is lost");
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&music_schema().tables[0], "title"),
        Some(Value::String("e260".to_owned()))
    );
}

/// After a `RetryLater` answer the writer's link stops at the deferred
/// write: nothing chained after it goes up before the backoff passes, and
/// then the chain goes up again from the predecessor Core named.
///
/// ```text
/// link  ── e2 ──► core   RetryLater(awaiting e1); e1 dropped on the link
/// alice    e3 made: held until the backoff passes
/// alice ── (backoff) e1, e2, e3 ──► core   accepted in order
/// ```
#[test]
fn uploads_stop_at_the_deferred_write_until_its_backoff_passes() {
    let mut fixture = Fixture::new(0xe3);
    let e1 = fixture.edit("e1");
    let e2 = fixture.edit("e2");
    fixture.upload(e2);
    // The link loses e1; Core sees e2 first.
    fixture.take_upload(e1);
    fixture.core.tick().unwrap();
    assert_eq!(fixture.core_state(e2), None);
    fixture.writer.tick().unwrap();
    let e3 = fixture.edit("e3");
    for _ in 0..4 {
        fixture.writer.tick().unwrap();
        fixture.core.tick().unwrap();
    }
    assert!(
        !fixture
            .uploads
            .borrow()
            .iter()
            .any(|message| is_commit_unit_for(message, e3)),
        "e3 does not overtake the deferred chain"
    );
    for tx_id in [e1, e2, e3] {
        assert_eq!(
            fixture.core_state(tx_id),
            None,
            "nothing went up during the backoff"
        );
    }
    fixture.clock.set(fixture.clock.get() + 60_000);
    let all_global = |fixture: &Fixture| [e1, e2, e3].iter().all(|tx| fixture.is_global(*tx));
    fixture.pump_until(32, all_global);
    assert!(all_global(&fixture));
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    assert_eq!(
        rows[0].cell(&music_schema().tables[0], "title"),
        Some(Value::String("e3".to_owned()))
    );
}

/// When the predecessor Core waits for is already settled here (here
/// refused locally, never uploaded) or unknown, it can never reach Core.
/// The chained write then fails with a surfaced "predecessor lost" error
/// instead of retrying forever, and leaves the outbox.
#[test]
fn chained_write_fails_when_its_predecessor_is_lost() {
    let mut fixture = Fixture::new(0xe5);
    let e1 = fixture.edit("e1");
    let e2 = fixture.edit("e2");
    fixture.upload(e2);
    fixture.take_upload(e1);
    // e1 settles here without reaching Core.
    crate::local_executor::block_on(fixture.writer.node.node.borrow_mut().apply_fate_update(
        e1,
        Fate::Rejected(RejectionReason::AuthorizationDenied),
        None,
        None,
    ))
    .unwrap();
    fixture
        .writer
        .node
        .outbox
        .borrow_mut()
        .retain(|pending| pending.tx_id != e1);
    let failed = |fixture: &Fixture| {
        matches!(
            fixture.writer.write_state(e2).map(|state| state.fate),
            Ok(Fate::Rejected(_))
        )
    };
    fixture.pump_until(16, failed);
    let Ok(Fate::Rejected(RejectionReason::MalformedCommit(message))) =
        fixture.writer.write_state(e2).map(|state| state.fate)
    else {
        panic!("e2 fails: {:?}", fixture.writer.write_state(e2));
    };
    assert!(message.starts_with("predecessor lost"), "{message}");
    assert!(!fixture.writer.node.outbox.borrow().contains(e2));
    assert_eq!(fixture.core_state(e2), None);
    // Later ticks do not send it again.
    fixture.uploads.borrow_mut().clear();
    fixture.clock.set(fixture.clock.get() + 60_000);
    for _ in 0..4 {
        fixture.writer.tick().unwrap();
    }
    assert!(
        !fixture
            .uploads
            .borrow()
            .iter()
            .any(|message| is_commit_unit_for(message, e2))
    );
}

/// Core's link answers alice's out-of-order edit with `RetryLater` while
/// its transport refuses the frame once. The answer waits in Core's fate
/// queue like a fate and goes out on a later tick, so alice still sends
/// her chain again; it is never lost to back-pressure.
#[test]
fn retry_later_survives_back_pressure_on_cores_link() {
    let mut fixture = Fixture::new(0xe7);
    let e1 = fixture.edit("e1");
    let e2 = fixture.edit("e2");
    fixture.upload(e2);
    fixture.take_upload(e1);
    let refused = Rc::new(Cell::new(false));
    {
        let mut link = fixture.links.1.borrow_mut();
        let inner = std::mem::replace(
            &mut link.transport,
            Box::new(BackpressureOnceTransport {
                outbound: Default::default(),
                failed: true,
            }),
        );
        link.transport = Box::new(RefuseRetryLaterOnce {
            inner,
            refused: Rc::clone(&refused),
        });
    }
    fixture.core.tick().unwrap();
    assert!(refused.get(), "the transport refused the answer once");
    assert_eq!(fixture.core_state(e2), None);
    // The refused answer is still queued at Core and reaches alice.
    let all_global = |fixture: &Fixture| [e1, e2].iter().all(|tx| fixture.is_global(*tx));
    fixture.pump_until(8, all_global);
    fixture.clock.set(fixture.clock.get() + 60_000);
    fixture.pump_until(32, all_global);
    assert!(all_global(&fixture), "alice's chain is accepted");
}

/// Core's side of a link that refuses the first `RetryLater` with
/// back-pressure and passes everything else through.
struct RefuseRetryLaterOnce {
    inner: Box<dyn Transport>,
    refused: Rc<Cell<bool>>,
}

impl Transport for RefuseRetryLaterOnce {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::RetryLater { .. }) && !self.refused.get() {
            self.refused.set(true);
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

/// alice's outbox holds her first edit behind her second (here moved
/// there by hand), and the link loses the first. Core answers the second
/// with `RetryLater`. alice moves the first edit ahead of the second and
/// sends nothing during the backoff; then the first goes up before the
/// second and both are accepted.
#[test]
fn predecessor_queued_after_its_successor_goes_up_first_on_retry() {
    let mut fixture = Fixture::new(0xe9);
    let e1 = fixture.edit("e1");
    let e2 = fixture.edit("e2");
    {
        let outbox = &fixture.writer.node.outbox;
        outbox.borrow_mut().retain(|pending| pending.tx_id != e1);
        queue_pending_upload_in(outbox, e1, None);
    }
    fixture.upload(e1);
    fixture.take_upload(e1);
    fixture.core.tick().unwrap();
    assert_eq!(fixture.core_state(e2), None);
    fixture.uploads.borrow_mut().clear();
    // Core's answer is not taken off the link here, so whatever alice sends
    // during the backoff stays visible.
    for _ in 0..4 {
        fixture.writer.tick().unwrap();
    }
    assert!(
        !fixture
            .uploads
            .borrow()
            .iter()
            .any(|message| is_commit_unit_for(message, e1) || is_commit_unit_for(message, e2)),
        "nothing of the chain goes up during the backoff"
    );
    fixture.clock.set(fixture.clock.get() + 60_000);
    fixture.upload(e2);
    let position = |tx_id| {
        fixture
            .uploads
            .borrow()
            .iter()
            .position(|message| is_commit_unit_for(message, tx_id))
    };
    assert!(
        position(e1).is_some_and(|first| position(e2).is_some_and(|second| first < second)),
        "the predecessor goes up first"
    );
    let all_global = |fixture: &Fixture| [e1, e2].iter().all(|tx| fixture.is_global(*tx));
    fixture.pump_until(16, all_global);
    assert!(all_global(&fixture));
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    assert_eq!(
        rows[0].cell(&music_schema().tables[0], "title"),
        Some(Value::String("e2".to_owned()))
    );
}

/// Two writers, alice and bob, upload through one relay to Core.
struct RelayFixture {
    core: CoreDb,
    relay: Db,
    alice: Db,
    bob: Db,
    clock: Rc<Cell<u64>>,
    /// alice's uploads to the relay, and the relay's messages to alice.
    alice_uploads: Rc<RefCell<VecDeque<SyncMessage>>>,
    to_alice: Rc<RefCell<VecDeque<SyncMessage>>>,
    /// The relay's uploads to Core.
    relay_uploads: Rc<RefCell<VecDeque<SyncMessage>>>,
    target: RowUuid,
    _links: Vec<Rc<LocalMutex<PeerConnection>>>,
}

impl RelayFixture {
    fn new(node: u8) -> Self {
        let schema = music_schema();
        let alice_author = AuthorSubject::for_test_bytes([node; 16]);
        let bob_author = AuthorSubject::for_test_bytes([node + 1; 16]);
        let core = open_core(node + 2, AuthorSubject::SYSTEM, &schema);
        let relay = open_db(node + 3, AuthorSubject::SYSTEM, &schema);
        let alice = open_db(node, alice_author, &schema);
        let bob = open_db(node + 1, bob_author, &schema);
        let clock = Rc::new(Cell::new(10_000));
        for db in [&relay, &alice, &bob] {
            db.node
                .set_upload_retry_clock_for_test(Rc::new(ManualUploadRetryClock(Rc::clone(
                    &clock,
                ))));
            db.set_tick_scheduler(Some(Rc::new(RecordingScheduler::default())));
        }
        let (relay_transport, core_transport, relay_uploads) = duplex_with_client_outbound_tap();
        let relay_upstream =
            crate::local_executor::block_on(relay.connect_upstream(relay_transport));
        let core_link = core.accept_subscriber_with_trust(
            core_transport,
            AuthorSubject::SYSTEM,
            CommitUnitTrust::TrustedBackend,
        );
        let (alice_transport, relay_alice_transport, alice_uploads, to_alice) = duplex_with_taps();
        let alice_upstream =
            crate::local_executor::block_on(alice.connect_upstream(alice_transport));
        let relay_alice = relay.accept_subscriber(relay_alice_transport, alice_author);
        let (bob_transport, relay_bob_transport) = duplex();
        let bob_upstream = crate::local_executor::block_on(bob.connect_upstream(bob_transport));
        let relay_bob = relay.accept_subscriber(relay_bob_transport, bob_author);
        let seed = crate::local_executor::block_on(alice.insert(
            "tracks",
            title("seed"),
            Default::default(),
        ))
        .unwrap();
        let mut fixture = Self {
            core,
            relay,
            alice,
            bob,
            clock,
            alice_uploads,
            to_alice,
            relay_uploads,
            target: seed.row_uuid(),
            _links: vec![
                relay_upstream,
                core_link,
                alice_upstream,
                relay_alice,
                bob_upstream,
                relay_bob,
            ],
        };
        fixture.pump_until(32, |fixture| is_global(&fixture.alice, seed.tx_id));
        assert!(is_global(&fixture.alice, seed.tx_id));
        fixture
    }

    fn alice_edit(&self, value: &str) -> TxId {
        crate::local_executor::block_on(self.alice.update(
            "tracks",
            self.target,
            title(value),
            Default::default(),
        ))
        .unwrap()
        .tx_id
    }

    fn pump(&self) {
        self.alice.tick().unwrap();
        self.bob.tick().unwrap();
        self.relay.tick().unwrap();
        self.core.tick().unwrap();
        self.relay.tick().unwrap();
    }

    fn pump_until(&mut self, rounds: usize, done: impl Fn(&Self) -> bool) {
        for _ in 0..rounds {
            if done(self) {
                return;
            }
            self.pump();
        }
    }

    fn core_state(&self, tx_id: TxId) -> Option<Fate> {
        crate::local_executor::block_on(self.core.node().borrow_mut().transaction_state(tx_id))
            .map(|(fate, ..)| fate)
    }

    fn relay_state(&self, tx_id: TxId) -> Option<Fate> {
        crate::local_executor::block_on(self.relay.node.node.borrow_mut().transaction_state(tx_id))
            .map(|(fate, ..)| fate)
    }

    fn retry_later_sent_to_alice(&self, tx_id: TxId) -> bool {
        self.to_alice.borrow().iter().any(|message| {
            matches!(message, SyncMessage::RetryLater { tx_id: retried, .. } if *retried == tx_id)
        })
    }
}

fn is_global(db: &Db, tx_id: TxId) -> bool {
    db.write_state(tx_id)
        .is_ok_and(|state| state.durability == DurabilityTier::Global)
}

/// The relay has alice's two chained edits queued, and its link to Core
/// loses the first. Core answers the second with `RetryLater`. The relay
/// holds back alice's chain for the backoff and forwards the answer to
/// her, but bob's write behind hers in the relay's outbox goes up at once.
/// After the backoff alice's chain is accepted in order.
///
/// ```text
/// alice ── e1, e2 ──► relay ── e2 ──► core   RetryLater(e2, awaiting e1)
/// relay ── RetryLater ──► alice     alice's chain held for the backoff
/// bob   ── b1 ──► relay ── b1 ──► core       accepted at once
/// (backoff) relay ── e1, e2 ──► core         accepted in order
/// ```
#[test]
fn relay_holds_back_only_the_retried_writer_and_forwards_retry_later() {
    let mut fixture = RelayFixture::new(0x71);
    let e1 = fixture.alice_edit("e1");
    let e2 = fixture.alice_edit("e2");
    let relay_uploaded = |fixture: &RelayFixture, tx_id| {
        fixture
            .relay_uploads
            .borrow()
            .iter()
            .any(|message| is_commit_unit_for(message, tx_id))
    };
    for _ in 0..8 {
        if relay_uploaded(&fixture, e2) {
            break;
        }
        fixture.alice.tick().unwrap();
        fixture.relay.tick().unwrap();
    }
    assert!(relay_uploaded(&fixture, e1) && relay_uploaded(&fixture, e2));
    // The relay's link to Core loses e1.
    fixture
        .relay_uploads
        .borrow_mut()
        .retain(|message| !is_commit_unit_for(message, e1));
    fixture.core.tick().unwrap();
    assert_eq!(fixture.core_state(e2), None, "Core stored nothing for e2");
    fixture.relay.tick().unwrap();
    assert!(
        fixture.retry_later_sent_to_alice(e2),
        "the relay forwards the answer to alice"
    );
    assert_eq!(fixture.relay_state(e2), Some(Fate::Pending));

    let b1 = crate::local_executor::block_on(fixture.bob.insert(
        "tracks",
        title("b1"),
        Default::default(),
    ))
    .unwrap()
    .tx_id;
    fixture.pump_until(8, |fixture| is_global(&fixture.bob, b1));
    assert!(
        is_global(&fixture.bob, b1),
        "bob's write is not held back by alice's deferral"
    );
    for tx_id in [e1, e2] {
        assert_eq!(fixture.core_state(tx_id), None, "alice's chain waits");
        assert_eq!(
            fixture.alice.write_state(tx_id).unwrap().fate,
            Fate::Pending
        );
    }

    fixture.clock.set(fixture.clock.get() + 60_000);
    let alice_global =
        |fixture: &RelayFixture| [e1, e2].iter().all(|tx| is_global(&fixture.alice, *tx));
    fixture.pump_until(32, alice_global);
    assert!(alice_global(&fixture), "alice's chain is accepted");
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    let mut titles = rows
        .iter()
        .filter_map(|row| match row.cell(&music_schema().tables[0], "title") {
            Some(Value::String(title)) => Some(title),
            _ => None,
        })
        .collect::<Vec<_>>();
    titles.sort();
    assert_eq!(titles, ["b1", "e2"]);
}

/// alice's link to the relay loses her first edit, so the relay never has
/// it. Core answers her second edit with `RetryLater`. The relay did not
/// author the edit, so it does not decide that its predecessor is lost:
/// the edit stays pending at the relay and at alice, and the relay
/// forwards the answer. alice still has her first edit queued, sends her
/// chain again after the backoff, and both edits are accepted.
#[test]
fn relay_never_fails_a_clients_write_whose_predecessor_it_lacks() {
    let mut fixture = RelayFixture::new(0x75);
    let e1 = fixture.alice_edit("e1");
    let e2 = fixture.alice_edit("e2");
    fixture.alice.tick().unwrap();
    fixture
        .alice_uploads
        .borrow_mut()
        .retain(|message| !is_commit_unit_for(message, e1));
    for _ in 0..3 {
        fixture.relay.tick().unwrap();
        fixture.core.tick().unwrap();
    }
    fixture.relay.tick().unwrap();
    assert_eq!(fixture.core_state(e2), None);
    assert_eq!(fixture.relay_state(e1), None, "the relay never had e1");
    assert!(fixture.retry_later_sent_to_alice(e2));
    assert_eq!(
        fixture.relay_state(e2),
        Some(Fate::Pending),
        "the relay does not fail alice's write"
    );
    fixture.alice.tick().unwrap();
    assert_eq!(
        fixture.alice.write_state(e2).unwrap().fate,
        Fate::Pending,
        "alice keeps her write pending"
    );

    fixture.clock.set(fixture.clock.get() + 60_000);
    let alice_global =
        |fixture: &RelayFixture| [e1, e2].iter().all(|tx| is_global(&fixture.alice, *tx));
    fixture.pump_until(32, alice_global);
    assert!(alice_global(&fixture), "alice's chain is accepted");
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    assert_eq!(
        rows[0].cell(&music_schema().tables[0], "title"),
        Some(Value::String("e2".to_owned()))
    );
}

/// A relay forwards a `RetryLater` to the author's route while that route
/// is still blocked on replay. The relay has dropped its own copy of the
/// write, so the answer is the author's only signal to resend: the route
/// holds it (a later progress update does not replace it) and delivers it
/// once replay is ready.
#[test]
fn retry_later_forwarded_to_a_blocked_replay_route_is_delivered_once_ready() {
    let routes: LocalFateRoutes = Rc::new(RefCell::new(BTreeMap::new()));
    let queue: PendingDownstreamFates = Rc::new(RefCell::new(Vec::new()));
    let author = AuthorSubject::for_test_bytes([0xf1; 16]);
    let writer = NodeUuid::from_bytes([0xf2; 16]);
    let tx_id = TxId::new(TxTime(2), writer);
    let retry_later = SyncMessage::RetryLater {
        tx_id,
        awaiting: TxId::new(TxTime(1), writer),
    };
    register_local_replay_route(&routes, tx_id, &queue, author, None);
    route_local_fate(&routes, tx_id, &retry_later);
    route_local_fate(
        &routes,
        tx_id,
        &SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Pending,
            global_time: None,
            durability: Some(DurabilityTier::Local),
        },
    );
    assert!(queue.borrow().is_empty(), "nothing goes out while blocked");
    register_local_fate_route(&routes, tx_id, &queue);
    release_local_replay_fates(&routes);
    assert_eq!(queue.borrow().as_slice(), [retry_later]);
}

/// A blocked replay route that already holds a terminal fate keeps it: a
/// later forwarded `RetryLater` does not replace it.
#[test]
fn retry_later_never_replaces_a_held_terminal_fate() {
    let routes: LocalFateRoutes = Rc::new(RefCell::new(BTreeMap::new()));
    let queue: PendingDownstreamFates = Rc::new(RefCell::new(Vec::new()));
    let author = AuthorSubject::for_test_bytes([0xf3; 16]);
    let writer = NodeUuid::from_bytes([0xf4; 16]);
    let tx_id = TxId::new(TxTime(2), writer);
    let rejected = SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
        global_time: None,
        durability: None,
    };
    register_local_replay_route(&routes, tx_id, &queue, author, None);
    route_local_fate(&routes, tx_id, &rejected);
    route_local_fate(
        &routes,
        tx_id,
        &SyncMessage::RetryLater {
            tx_id,
            awaiting: TxId::new(TxTime(1), writer),
        },
    );
    register_local_fate_route(&routes, tx_id, &queue);
    release_local_replay_fates(&routes);
    assert_eq!(queue.borrow().as_slice(), [rejected]);
}

/// alice's first edit failed its large-value upload: her link dropped it
/// from the outbox and staged its rejection, which is not applied yet, so
/// the edit is still pending here. Core then answers her second edit with
/// `RetryLater`, naming the first. The failed edit is not queued again;
/// the second fails as "predecessor lost". (The link state is set the way
/// the large-value `Rejected` answer sets it; the staged rejection is left
/// out so the window before it applies stays open.)
#[test]
fn predecessor_whose_large_value_upload_failed_is_not_queued_again() {
    let mut fixture = Fixture::new(0xeb);
    let e1 = fixture.edit("e1");
    let e2 = fixture.edit("e2");
    fixture.upload(e2);
    fixture.take_upload(e1);
    {
        let mut link = fixture.links.0.borrow_mut();
        let ConnectionLink::Upstream(state) = &mut link.link else {
            unreachable!("the writer's link is upstream")
        };
        state.failed_large_value_uploads.insert(e1);
    }
    {
        let mut outbox = fixture.writer.node.outbox.borrow_mut();
        outbox.retain(|pending| pending.tx_id != e1);
        outbox.mark_upload_failed(e1, "its large value was not staged by the server");
    }
    assert_eq!(fixture.writer.write_state(e1).unwrap().fate, Fate::Pending);
    let failed = |fixture: &Fixture| {
        matches!(
            fixture.writer.write_state(e2).map(|state| state.fate),
            Ok(Fate::Rejected(_))
        )
    };
    fixture.pump_until(16, failed);
    let Ok(Fate::Rejected(RejectionReason::MalformedCommit(message))) =
        fixture.writer.write_state(e2).map(|state| state.fate)
    else {
        panic!("e2 fails: {:?}", fixture.writer.write_state(e2));
    };
    assert!(message.starts_with("predecessor lost"), "{message}");
    assert!(
        !fixture.writer.node.outbox.borrow().contains(e1),
        "the failed predecessor is not queued again"
    );
    assert_eq!(fixture.core_state(e1), None);
}
