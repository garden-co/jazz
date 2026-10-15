//! Upload order (SPEC 14, SPEC 4 §4.6).
//!
//! A writer's commit units travel to the authority in the order they were
//! written, and the authority decides them in arrival order. It never waits
//! for a chained write's pending predecessor: one it has not decided never
//! arrives, so the write applies as written. A predecessor that arrives
//! after a later write of its writer anyway is refused (`INV-HIST-23`).
//! These tests reorder a writer's uploads at the link by hand.

use super::*;
use crate::model::test_support::AllowAll;

fn music_schema() -> JazzSchema {
    build_public_db_test_schema(
        PublicSchemaBuilder::new()
            .table(
                PublicTableSchemaBuilder::new("tracks")
                    .column("title", PublicColumnType::Text)
                    .column("notes", PublicColumnType::Text),
            )
            .allow_all(),
    )
}

fn title(value: impl Into<String>) -> RowCells {
    BTreeMap::from([("title".to_owned(), Value::String(value.into()))])
}

fn is_commit_unit_for(message: &SyncMessage, tx_id: TxId) -> bool {
    matches!(message, SyncMessage::CommitUnit { tx, .. } if tx.tx_id == tx_id)
}

/// A writer `node` linked to a fresh Core through a tap on its outbound
/// messages, and one accepted seed row.
struct Fixture {
    core: CoreDb,
    writer: Db,
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

    fn is_rejected(&self, tx_id: TxId) -> bool {
        self.writer
            .write_state(tx_id)
            .is_ok_and(|state| matches!(state.fate, Fate::Rejected(_)))
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

/// The ordinary path: alice's chained edits go up in the order she made
/// them and Core accepts them in that order.
#[test]
fn chained_writes_go_up_and_are_accepted_in_order() {
    let mut fixture = Fixture::new(0xe1);
    let edits = (1..=40)
        .map(|index| fixture.edit(&format!("e{index}")))
        .collect::<Vec<_>>();
    fixture.upload(*edits.last().unwrap());
    let on_wire = fixture
        .uploads
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::CommitUnit { tx, .. } => Some(tx.tx_id),
            _ => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(on_wire, edits, "uploads keep the writer's order");
    let edits_global = |fixture: &Fixture| edits.iter().all(|tx_id| fixture.is_global(*tx_id));
    fixture.pump_until(64, edits_global);
    assert!(edits_global(&fixture));
    let seqs = edits
        .iter()
        .map(|tx_id| fixture.core_state(*tx_id).and_then(|(_, seq, _)| seq))
        .collect::<Vec<_>>();
    assert!(seqs.iter().all(Option::is_some));
    assert!(seqs.windows(2).all(|pair| pair[0] < pair[1]), "{seqs:?}");
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    assert_eq!(
        rows[0].cell(&music_schema().tables[0], "title"),
        Some(Value::String("e40".to_owned()))
    );
}

/// The link delivers alice's first edit after her second, against the
/// order the transport keeps. Core does not wait for the first: it applies
/// the second as written. The first then arrives late and is refused, so
/// the row never goes back to it, and alice sees the refusal.
///
/// ```text
/// alice ══ e1, e2 (e2 chained on e1)
/// link  ── e2 ──► core   e2 accepted   title=e2
/// link  ── e1 ──► core   e1 rejected (late)   title=e2
/// ```
#[test]
fn predecessor_reaching_core_after_its_chained_write_is_refused() {
    let mut fixture = Fixture::new(0xe3);
    let e1 = fixture.edit("e1");
    let e2 = fixture.edit("e2");
    fixture.upload(e2);
    let first = fixture.take_upload(e1);
    fixture.uploads.borrow_mut().push_back(first);
    fixture.pump_until(32, |fixture| {
        fixture.is_global(e2) && fixture.is_rejected(e1)
    });
    assert!(fixture.is_global(e2));
    let Fate::Rejected(RejectionReason::MalformedCommit(reason)) =
        fixture.writer.write_state(e1).unwrap().fate
    else {
        panic!("e1 must be refused as late");
    };
    assert!(
        reason.contains("arrived after a later write of its writer"),
        "{reason}"
    );
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&music_schema().tables[0], "title"),
        Some(Value::String("e2".to_owned()))
    );
}

/// A queued edit and a streamed value to the same row keep the order they
/// were made in. `enqueue_update` reserves its transaction when it is queued;
/// the streamed upload is finished after it, while the edit still waits in
/// the owner queue. The streamed write must not take an earlier place in the
/// writer's order than the edit made before it: Core decides one writer's
/// writes in transaction order, so the edit would otherwise reach Core as
/// older than a write it had already accepted, and be refused.
///
/// ```text
/// alice ══ enqueue_update(title=status)   reserved t1, queued
/// alice ══ finish streaming notes          t2
/// core     both accepted; title=status, notes=streamed
/// ```
#[test]
fn queued_edit_and_later_streamed_value_keep_their_order() {
    let mut fixture = Fixture::new(0xe5);
    let queued = fixture
        .writer
        .enqueue_update(
            "tracks".to_owned(),
            fixture.target,
            title("status"),
            Default::default(),
        )
        .unwrap();
    let edit = queued.mergeable_tx_id();
    let cells = BTreeMap::new();
    let mut upload = fixture
        .writer
        .begin_streaming_value_upload("tracks", &cells, "notes")
        .unwrap();
    crate::local_executor::block_on(
        fixture
            .writer
            .push_streaming_value_upload(&mut upload, b"streamed"),
    )
    .unwrap();
    let streamed = crate::local_executor::block_on(fixture.writer.finish_streaming_value_upload(
        upload,
        crate::db::StreamingMutationKind::Update,
        "tracks",
        fixture.target,
        cells,
        "notes",
        crate::db::WriteIdentity::Database,
        None,
        None,
        None,
    ))
    .unwrap()
    .mergeable_tx_id();
    assert!(
        edit < streamed,
        "the edit was made first, so it is first in its writer's order"
    );
    let settled = |fixture: &Fixture| {
        [edit, streamed]
            .iter()
            .all(|tx_id| fixture.is_global(*tx_id) || fixture.is_rejected(*tx_id))
    };
    fixture.pump_until(64, settled);
    for tx_id in [edit, streamed] {
        let state = fixture.writer.write_state(tx_id).unwrap();
        assert!(
            fixture.is_global(tx_id),
            "{tx_id:?} must be accepted: {:?}",
            state.fate
        );
    }
    let edit_seq = fixture.core_state(edit).and_then(|(_, seq, _)| seq);
    let streamed_seq = fixture.core_state(streamed).and_then(|(_, seq, _)| seq);
    assert!(edit_seq < streamed_seq, "{edit_seq:?} {streamed_seq:?}");
    let rows = fixture.core.read(&Query::from("tracks")).unwrap();
    let table = &music_schema().tables[0];
    assert_eq!(
        rows[0].cell(table, "title"),
        Some(Value::String("status".to_owned()))
    );
}

/// A stand-in unit: the upload order never looks inside one.
fn replay_unit(tx_id: TxId) -> SyncMessage {
    SyncMessage::FateUpdate {
        tx_id,
        fate: Fate::Pending,
        global_time: None,
        durability: None,
    }
}

/// Recovery queues a writer's pending units in transaction order whether
/// their causal closure is complete or not: a blocked `p` goes up before the
/// complete `w` written after it, not after every complete unit.
#[test]
fn recovery_uploads_a_blocked_unit_before_a_later_complete_one() {
    let node = NodeUuid::from_bytes([0xe7; 16]);
    let p = TxId::new(TxTime(100), node);
    let w = TxId::new(TxTime(101), node);
    let settled = TxId::new(TxTime(99), node);
    let pending = BTreeSet::from([p, w]);
    let roots = BTreeSet::from([p, w]);
    let statuses = BTreeMap::from([
        (p, LocalReplayStatus::Blocked),
        (w, LocalReplayStatus::Complete),
        (settled, LocalReplayStatus::Complete),
    ]);
    let complete = vec![(settled, replay_unit(settled)), (w, replay_unit(w))];
    let blocked = BTreeMap::from([(p, replay_unit(p))]);
    let uploads = local_replay_upload_order(
        &pending,
        &complete,
        &roots,
        &statuses,
        &blocked,
        &BTreeMap::new(),
    );
    assert_eq!(
        uploads.keys().copied().collect::<Vec<_>>(),
        vec![p, w],
        "settled ancestors are not uploaded; p goes before w"
    );

    // A blocked root the plan kept no unit for uploads the route's retained
    // unit, in the same order.
    let uploads = local_replay_upload_order(
        &pending,
        &complete,
        &roots,
        &statuses,
        &BTreeMap::new(),
        &BTreeMap::from([(p, replay_unit(p))]),
    );
    assert_eq!(uploads.keys().copied().collect::<Vec<_>>(), vec![p, w]);
}

/// A transaction the plan completed keeps its complete unit even if a
/// blocked or retained unit is also known for it.
#[test]
fn recovery_keeps_the_complete_unit_of_a_completed_transaction() {
    let node = NodeUuid::from_bytes([0xe8; 16]);
    let w = TxId::new(TxTime(101), node);
    let complete_unit = replay_unit(w);
    let retained_unit = SyncMessage::FateUpdate {
        tx_id: w,
        fate: Fate::Accepted,
        global_time: None,
        durability: None,
    };
    let uploads = local_replay_upload_order(
        &BTreeSet::from([w]),
        &[(w, complete_unit.clone())],
        &BTreeSet::from([w]),
        &BTreeMap::from([(w, LocalReplayStatus::Complete)]),
        &BTreeMap::from([(w, retained_unit.clone())]),
        &BTreeMap::from([(w, retained_unit)]),
    );
    assert_eq!(uploads.get(&w), Some(&complete_unit));
}
