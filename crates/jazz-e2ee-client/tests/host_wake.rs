//! The host's tick request for what the runtime asked from inside its own
//! futures: a browser host ticks only when asked, so a request or commit that
//! waits to go upstream needs one ask, and a transport that cannot drain must
//! not turn it into an ask on every turn.

mod common;

use std::cell::Cell;
use std::rc::Rc;

use common::*;
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
