//! The host's tick request for what the runtime asked from inside its own
//! futures: a browser host ticks only when asked, so a request or commit that
//! waits to go upstream needs one ask, and a transport that cannot drain must
//! not turn it into an ask on every turn.

mod common;

use std::cell::Cell;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use common::*;
use jazz::db::block_on;
use jazz::query::Query;
use jazz::tools::OpenTransactionId;
use jazz_e2ee_client::MemoryStore;
use jazz_e2ee_client::host::bridge::JsStore;
use jazz_e2ee_client::host::runtime::{Built, Encryption};

/// A runtime over a database linked to a server nothing delivers to, and the
/// count of ticks the host was asked for.
fn runtime() -> (
    Rc<Encryption>,
    Rc<jazz::db::Db>,
    jazz::db::Db,
    Rc<Cell<usize>>,
) {
    let server = authority();
    let client = open_client(Rc::new(MemoryStore::new()));
    let db = Rc::new(client_db(&client, None));
    let _link = link(&db, &server);
    // The connection's own messages have gone: nothing waits.
    pump(&server, &db);
    let store = Rc::new(JsStore::default());
    let asked = Rc::new(Cell::new(0));
    let count = Rc::clone(&asked);
    store.set_wake(Box::new(move || count.set(count.get() + 1)));
    let encryption = Encryption::new(
        Built {
            spaces: serde_json::json!({}),
            client: Rc::new(client),
            reports: Rc::default(),
        },
        store,
        Rc::clone(&db),
    );
    (encryption, db, server, asked)
}

/// Contract: a commit waiting to go upstream makes the runtime ask the host
/// for the tick that sends it (nothing else would tick a browser tab).
#[test]
fn a_commit_waiting_to_go_upstream_asks_for_a_tick() {
    let (encryption, db, _server, asked) = runtime();
    encryption.wake_if_owed();
    assert_eq!(asked.get(), 0, "nothing waits yet");
    project(&db, "plans");
    encryption.wake_if_owed();
    assert_eq!(asked.get(), 1);
}

/// Contract: while what waits cannot be sent (no tick drained it), asking again
/// and again asks the host once, so a stuck transport does not spin the host;
/// once it is sent, a new batch asks again.
#[test]
fn a_transport_that_cannot_drain_is_asked_for_once_per_batch() {
    let (encryption, db, server, asked) = runtime();
    project(&db, "plans");
    for _ in 0..50 {
        encryption.wake_if_owed();
    }
    assert_eq!(asked.get(), 1, "one ask for the batch that waits");
    // The tick the ask was for sends it; the next batch is a new ask.
    pump(&server, &db);
    encryption.wake_if_owed();
    assert_eq!(asked.get(), 1, "nothing waits after the send");
    project(&db, "more");
    encryption.wake_if_owed();
    assert_eq!(asked.get(), 2);
}

/// Contract: when Alice's first ledger read is answered, the next read in
/// the same transaction schedules its own tick, even if the host observes
/// the queues only after polling the continuation, as the WASM host does.
///
/// Alice ──read projects──► authority ──reply──► Alice ──read notes──► authority
#[test]
fn a_continuation_wakes_for_the_next_ledger_read_after_a_drain() {
    let (encryption, db, server, asked) = runtime();
    let id = OpenTransactionId::new();
    block_on(db.begin_serializable(id)).unwrap();
    let tx = db.tx_ref(id);
    let projects = db.prepare_query(&Query::from("projects")).unwrap();
    let notes = db.prepare_query(&Query::from("notes")).unwrap();
    let mut reads = std::pin::pin!(async {
        tx.ledger_read(&projects).await.unwrap();
        tx.ledger_read(&notes).await.unwrap();
    });
    let context = &mut Context::from_waker(Waker::noop());

    assert!(reads.as_mut().poll(context).is_pending());
    encryption.wake_if_owed();
    assert_eq!(asked.get(), 1, "the first read needs a tick");
    pump(&server, &db);
    assert!(!db.owes_requests_sent(), "the first batch was drained");

    // Poll the continuation before the latch can see the empty queues.
    assert!(reads.as_mut().poll(context).is_pending());
    assert!(db.owes_requests_sent(), "the second read is queued");
    for _ in 0..50 {
        encryption.wake_if_owed();
    }
    assert_eq!(asked.get(), 2, "the second read wakes once for its batch");
    pump(&server, &db);
    assert_eq!(reads.as_mut().poll(context), Poll::Ready(()));
}
