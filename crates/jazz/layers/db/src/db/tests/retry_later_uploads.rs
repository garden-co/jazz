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
    _links: (
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
            _links: (upstream, subscriber),
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
