//! A replica's file that reopens as an older state than it uploaded from.
//!
//! A browser replica's IndexedDB store applies commits asynchronously
//! (`crates/jazz-wasm/src/browser.rs`: "a crash can lose the newest ones
//! until `Store::flush` resolves") while the core already sent them
//! upstream; a native file under `synchronous=NORMAL` can lose its newest
//! commits to power loss; and a device or file backup restores an older
//! file. The reopened file is then a prefix of what the authority holds.
//! The authority deduplicates uploads by transaction id `seq@node`
//! (INV-EDGE-16), so the replica must never hand out an id it may already
//! have uploaded: a file-backed replica numbers its commits no lower than
//! its wall clock (INV-EDGE-28).
//!
//! Each replica here runs on a test clock that moves forward between
//! sessions, as wall time does between a crash and a reopen.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::cell::Cell;
use std::collections::BTreeMap;
use std::future::Future;
use std::path::Path;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, RemoteLinkHint, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

/// 2026-01-01T00:00:00Z, in Unix ms.
const START_MS: u64 = 1_767_225_600_000;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// alice's wall clock (Unix ms), which the test moves forward.
#[derive(Clone)]
struct Clock(Rc<Cell<u64>>);

impl Clock {
    fn new() -> Self {
        Clock(Rc::new(Cell::new(START_MS)))
    }

    fn advance(&self, ms: u64) {
        self.0.set(self.0.get() + ms);
    }
}

/// alice's replica, opened from the file at `path` (node `0x61`).
fn open_alice(path: &Path, clock: &Clock) -> Db {
    let clock = clock.0.clone();
    block_on(Db::open_at(
        config(0x61).with_clock(move || clock.get()),
        path,
        FileOptions::default(),
    ))
    .unwrap()
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn drive<T>(dbs: &[&Db], future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..200 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
    panic!("did not complete");
}

fn connect(client: &Db, server: &Db) {
    let (client_side, server_side) = duplex();
    block_on(client.connect_upstream(client_side));
    jazz::db::block_on(client.set_remote_link_hint(RemoteLinkHint::Live));
    jazz::db::block_on(server.accept_subscriber(server_side, AuthorSubject::SYSTEM)).unwrap();
}

fn titles(db: &Db) -> BTreeMap<u8, String> {
    let prepared = db.prepare_query(&Query::from("docs")).unwrap();
    block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .into_iter()
        .map(|current| {
            let title = match current.cells().find(|(n, _)| *n == "title") {
                Some((_, Some(Value::String(value)))) => value.clone(),
                other => panic!("title: {other:?}"),
            };
            (current.row_uuid().as_bytes()[0], title)
        })
        .collect()
}

fn insert(client: &Db, n: u8, title: &str) -> u64 {
    block_on(client.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap()
    .mergeable_tx_id()
}

fn insert_and_settle(client: &Db, server: &Db, n: u8, title: &str) -> u64 {
    let tx = insert(client, n, title);
    drive(
        &[client, server, client],
        client.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .unwrap();
    tx
}

fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), to.join(entry.file_name())).unwrap();
    }
}

/// Put the file back as the copy in `from` holds it.
fn restore_dir(from: &Path, to: &Path) {
    std::fs::remove_dir_all(to).unwrap();
    copy_dir(from, to);
}

fn expected(entries: &[(u8, &str)]) -> BTreeMap<u8, String> {
    entries
        .iter()
        .map(|(n, title)| (*n, (*title).to_owned()))
        .collect()
}

/// alice's browser replica uploads a commit, the authority accepts it, and
/// the tab dies before IndexedDB persisted that commit. On reload her file
/// is the state before it. Her next, different write still lands, and her
/// replica keeps it.
///
/// ```
/// alice ──insert "one"──► server (accepted)      file: {one}  ← copy
/// alice ──insert "two"──► server (accepted)      file lost "two" (crash)
/// alice (reopened from {one}) ──insert "three"──► server
///                                     server: {one, two, three}
/// ```
#[test]
fn a_replica_that_lost_its_uploaded_tail_still_lands_new_writes() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("live");
    let durable = dir.path().join("durable");
    std::fs::create_dir_all(&live).unwrap();
    let path = live.join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(0xee))).unwrap();
    let clock = Clock::new();

    {
        let alice = open_alice(&path, &clock);
        connect(&alice, &server);
        insert_and_settle(&alice, &server, 1, "one");
    }
    // The durable prefix the crash will leave behind.
    copy_dir(&live, &durable);
    clock.advance(1_000);
    let lost = {
        let alice = open_alice(&path, &clock);
        connect(&alice, &server);
        insert_and_settle(&alice, &server, 2, "two")
    };
    assert_eq!(titles(&server), expected(&[(1, "one"), (2, "two")]));

    // The crash: IndexedDB never applied the commit of "two".
    restore_dir(&durable, &live);
    clock.advance(1_000);

    let alice = open_alice(&path, &clock);
    connect(&alice, &server);
    let next = insert_and_settle(&alice, &server, 3, "three");
    assert_ne!(next, lost, "the lost commit's transaction id is not reused");
    assert_eq!(
        titles(&server),
        expected(&[(1, "one"), (2, "two"), (3, "three")]),
        "alice's new write reached the authority"
    );
    drive(&[&alice, &server, &alice], async {});
    assert_eq!(
        titles(&alice).get(&3).map(String::as_str),
        Some("three"),
        "alice's replica keeps her new write"
    );
}

/// alice restores her replica from a backup taken several sessions ago.
/// Every write she made since was uploaded and accepted; none is in the
/// restored file. Each of her new writes lands, however many sessions and
/// commits the backup is behind.
///
/// ```
/// alice: {one}  ← backup
/// alice ──"two", "three"──► server   (session 2)
/// alice ──"four"──────────► server   (session 3)
/// alice (restored to {one}) ──"five", "six", "seven"──► server
///                server: {one, …, seven}
/// ```
#[test]
fn a_replica_restored_from_an_older_backup_still_lands_new_writes() {
    let dir = tempfile::tempdir().unwrap();
    let live = dir.path().join("live");
    let backup = dir.path().join("backup");
    std::fs::create_dir_all(&live).unwrap();
    let path = live.join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(0xed))).unwrap();
    let clock = Clock::new();

    {
        let alice = open_alice(&path, &clock);
        connect(&alice, &server);
        insert_and_settle(&alice, &server, 1, "one");
    }
    copy_dir(&live, &backup);
    let mut uploaded = Vec::new();
    for session in [&[(2, "two"), (3, "three")][..], &[(4, "four")][..]] {
        clock.advance(60_000);
        let alice = open_alice(&path, &clock);
        connect(&alice, &server);
        for (n, title) in session {
            uploaded.push(insert_and_settle(&alice, &server, *n, title));
        }
    }

    restore_dir(&backup, &live);
    clock.advance(3_600_000);

    let alice = open_alice(&path, &clock);
    connect(&alice, &server);
    for (n, title) in [(5, "five"), (6, "six"), (7, "seven")] {
        let tx = insert_and_settle(&alice, &server, n, title);
        assert!(!uploaded.contains(&tx), "tx {tx} was handed out before");
    }
    assert_eq!(
        titles(&server),
        expected(&[
            (1, "one"),
            (2, "two"),
            (3, "three"),
            (4, "four"),
            (5, "five"),
            (6, "six"),
            (7, "seven"),
        ]),
    );
}

/// alice's commit is applied by the authority, but the connection drops
/// before its fate reaches her. On reopen her file still holds the commit
/// as pending and resends it under the same transaction id: the authority
/// answers with the recorded fate and does not apply it twice. Her next
/// write, numbered after her clock, lands as a new commit.
///
/// ```
/// alice ──insert "one"──► server (applied)  ✗ fate lost
/// alice (reopened) ──resend "one" (same id)──► server: recorded fate
/// alice ──insert "two"──► server
///                server: {one, two}
/// ```
#[test]
fn a_reopened_replicas_resend_is_still_deduplicated() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("replica.sqlite");
    let server = block_on(Db::open_history_complete(config(0xec))).unwrap();
    let clock = Clock::new();

    let pending = {
        let alice = open_alice(&path, &clock);
        connect(&alice, &server);
        let tx = insert(&alice, 1, "one");
        block_on(alice.tick()).unwrap(); // upload
        block_on(server.tick()).unwrap(); // apply; alice never reads the fate
        assert_eq!(titles(&server), expected(&[(1, "one")]));
        tx
    };
    clock.advance(1_000);

    let alice = open_alice(&path, &clock);
    connect(&alice, &server);
    drive(
        &[&alice, &server, &alice],
        alice.wait_for_transaction(pending, DurabilityTier::Global),
    )
    .expect("the resend gets the recorded fate");
    assert_eq!(
        titles(&server),
        expected(&[(1, "one")]),
        "the resend was deduplicated"
    );
    let next = insert_and_settle(&alice, &server, 2, "two");
    assert_ne!(next, pending);
    assert_eq!(titles(&server), expected(&[(1, "one"), (2, "two")]));
}
