//! Retry-later answers to uploads (SPEC 4 §4.6, SPEC 8).
//!
//! The authority holds a chained write while its pending predecessor has no
//! fate there, up to a cap per writer node. Over the cap it stores nothing
//! and answers with a retry-later fate update; the writer keeps the write
//! pending and uploads it again after a backoff. These tests reorder a
//! writer's uploads at the link so its chain reaches Core out of order.

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

/// alice makes more chained edits of one row than Core holds for a writer
/// node, and her first edit reaches Core last. Core holds the first 256
/// waiting edits and asks her to retry the rest; once the first edit lands
/// the held ones are accepted, and her backed-off resends of the rest are
/// accepted too. Every edit stays pending at alice until then, and none is
/// lost.
///
/// ```text
/// alice ══ e1, e2 … e260 (each chained on the one before)
/// link  ── e2 … e260 ──► core   e2..e257 held, e258..e260: retry later
/// link  ── e1 ─────────► core   e1 … e257 accepted
/// alice ── (backoff) e258 … e260 ──► core   accepted   title=e260
/// ```
#[test]
fn chained_writes_over_the_parking_cap_converge_after_retry_later() {
    let node = 0xe1;
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
    let _upstream = crate::local_executor::block_on(writer.connect_upstream(writer_transport));
    let _subscriber = core.accept_subscriber(core_transport, author);
    let pump = |rounds: usize| {
        for _ in 0..rounds {
            writer.tick().unwrap();
            core.tick().unwrap();
        }
    };
    let is_global =
        |tx_id: TxId| writer.write_state(tx_id).unwrap().durability == DurabilityTier::Global;

    let seed =
        crate::local_executor::block_on(writer.insert("tracks", title("seed"), Default::default()))
            .unwrap();
    for _ in 0..16 {
        pump(1);
        if is_global(seed.tx_id) {
            break;
        }
    }
    assert!(is_global(seed.tx_id));
    let target = seed.row_uuid();

    let cap = crate::node::MAX_PREDECESSOR_PARKED_PER_WRITER_NODE;
    let edits = (1..=cap + 4)
        .map(|index| {
            crate::local_executor::block_on(writer.update(
                "tracks",
                target,
                title(format!("e{index}")),
                Default::default(),
            ))
            .unwrap()
            .tx_id
        })
        .collect::<Vec<_>>();
    // alice sends her chain; the link delivers her first edit last.
    for _ in 0..8 {
        writer.tick().unwrap();
        if uploads
            .borrow()
            .iter()
            .any(|message| is_commit_unit_for(message, *edits.last().unwrap()))
        {
            break;
        }
    }
    {
        let mut uploads = uploads.borrow_mut();
        let first = uploads
            .iter()
            .position(|message| is_commit_unit_for(message, edits[0]))
            .expect("alice uploaded her first edit");
        let first = uploads.remove(first).unwrap();
        uploads.push_back(first);
    }
    for _ in 0..64 {
        pump(1);
        if edits[..=cap].iter().all(|tx_id| is_global(*tx_id)) {
            break;
        }
    }
    assert!(
        edits[..=cap].iter().all(|tx_id| is_global(*tx_id)),
        "the held edits are accepted once the first one lands"
    );
    for tx_id in &edits[cap + 1..] {
        let state = writer.write_state(*tx_id).unwrap();
        assert_eq!(
            state.fate,
            Fate::Pending,
            "an edit asked to retry stays pending"
        );
        assert_eq!(
            crate::local_executor::block_on(core.node().borrow_mut().transaction_state(*tx_id)),
            None,
            "Core stored nothing for it"
        );
    }

    // After the backoff alice sends the rest again, and Core accepts them.
    clock.set(clock.get() + 60_000);
    for _ in 0..64 {
        pump(1);
        if edits.iter().all(|tx_id| is_global(*tx_id)) {
            break;
        }
    }
    assert!(
        edits.iter().all(|tx_id| is_global(*tx_id)),
        "no edit is lost"
    );
    let rows = core.read(&Query::from("tracks")).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].cell(&schema.tables[0], "title"),
        Some(Value::String(format!("e{}", cap + 4)))
    );
}
