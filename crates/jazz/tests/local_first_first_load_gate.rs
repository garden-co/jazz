//! The machinery behind the core `Db` first-load gate
//! (`FirstLoad::WaitForRemote`) and the host remote-link hint: link hints and
//! the attempt window, release on link failure or loss, strict remote offset
//! windows with their local fallback, and the non-durable foreground's
//! wait for the authority's answer through its storage owner.
//!
//! Most reads here use a long server wait to isolate link and answer behavior;
//! deadline tests use short waits and tick past them explicitly.
//!
//! Every client here is a core `Db` connected to a history-complete server
//! `Db` over an in-memory duplex transport, with ticks driven explicitly, so
//! "the server has not answered yet" is a deterministic state rather than a
//! race.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

use crate::common;

use jazz::block_on;
use jazz::db::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, FirstLoad, REMOTE_LINK_ATTEMPT_WINDOW, ReadOpts, RemoteLinkHint,
    SerializedReadResult, SubscriptionEvent, SubscriptionStream,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

const LABELS: [&str; 10] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
/// Bound on owner turns for anything that only needs a server round trip.
const MAX_TURNS: usize = 200;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    let schema = schema();
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

fn row(index: usize) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000u64.to_be_bytes());
    bytes[8..].copy_from_slice(&(index as u64 + 1).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

/// A server holding rows `a..j` (`row(0)..row(9)`).
fn seeded_server() -> Db {
    let server = block_on(Db::open_history_complete(config(
        0x51,
        AuthorSubject::SYSTEM,
    )))
    .expect("open server");
    for (index, label) in LABELS.iter().enumerate() {
        server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                row(index),
                AuthorSubject::SYSTEM,
                BTreeMap::from([("label".to_owned(), Value::String((*label).to_owned()))]),
            )
            .expect("seed server row");
    }
    server
}

/// A fresh client whose local store holds none of the server's rows.
fn fresh_client(node: u8) -> Db {
    block_on(Db::open(config(
        node,
        AuthorSubject::for_test_bytes([node; 16]),
    )))
    .expect("open client")
}

fn connect(client: &Db, server: &Db) {
    let (client_transport, server_transport) = duplex();
    let _upstream = block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
}

fn turn(client: &Db, server: Option<&Db>) {
    block_on(client.tick()).expect("tick client");
    if let Some(server) = server {
        block_on(server.tick()).expect("tick server");
        block_on(client.tick()).expect("tick client after server");
    }
}

/// Long enough that no test here reaches it by accident.
const LONG: Duration = Duration::from_secs(60);
/// Short enough to sleep through.
const SHORT: Duration = Duration::from_millis(200);

fn wait_for_remote(timeout: Duration) -> ReadOpts {
    ReadOpts {
        first_load: FirstLoad::WaitForRemote {
            timeout_ms: timeout.as_millis() as u64,
        },
        ..ReadOpts::for_read_tier(jazz::db::ReadTier::LocalFirst)
    }
}

/// A read that waits for the server's answer while the remote could answer.
fn gated() -> ReadOpts {
    wait_for_remote(LONG)
}

fn items() -> Query {
    Query::from("items")
}

/// The reviewer's window: rows `e, f` of `a..j`.
fn window() -> Query {
    Query::from("items")
        .order_by("label", OrderDirection::Asc)
        .offset(4)
        .limit(2)
}

fn subscribe(client: &Db, query: &Query, opts: ReadOpts) -> SubscriptionStream {
    let prepared = client.prepare_query(query).expect("prepare query");
    block_on(client.subscribe(&prepared, opts)).expect("subscribe")
}

/// The first event, driving owner turns until one is published.
fn first_event(
    stream: &mut SubscriptionStream,
    client: &Db,
    server: Option<&Db>,
) -> SubscriptionEvent {
    for _ in 0..MAX_TURNS {
        if let Some(event) = stream.try_next_event() {
            return event;
        }
        turn(client, server);
    }
    panic!("no subscription event within {MAX_TURNS} owner turns");
}

/// Drive owner turns and assert the stream publishes nothing.
fn assert_withheld(
    stream: &mut SubscriptionStream,
    client: &Db,
    server: Option<&Db>,
    turns: usize,
) {
    for _ in 0..turns {
        turn(client, server);
        if let Some(event) = stream.try_next_event() {
            panic!("the opening was published while it should be withheld: {event:?}");
        }
    }
}

/// `(reset, row ids in delivery order, settled)` of a delta.
fn opening(event: SubscriptionEvent) -> (bool, Vec<RowUuid>, bool) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            settled,
            ..
        } => (
            reset,
            added.iter().map(|row| row.row_uuid()).collect(),
            settled,
        ),
        other => panic!("expected an opening delta, got {other:?}"),
    }
}

/// A released opening is not followed by a second one: whatever the stream
/// publishes next is an incremental delta.
fn assert_no_second_opening(stream: &mut SubscriptionStream) {
    while let Some(event) = stream.try_next_event() {
        if let SubscriptionEvent::Delta { reset: true, .. } = event {
            panic!("a released opening was published again: {event:?}");
        }
    }
}

/// Run a host one-shot read to completion, driving owner turns between polls
/// the way bindings re-poll a pending native read.
fn one_shot(
    client: &Db,
    server: Option<&Db>,
    query: &Query,
    opts: ReadOpts,
    max_turns: usize,
) -> Vec<RowUuid> {
    let bytes = postcard::to_allocvec(query).expect("encode query");
    let read = client.all_serialized_query(&bytes, opts, None, None, None);
    let mut read = pin!(read);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..max_turns {
        if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
            return match result.expect("one-shot read") {
                SerializedReadResult::Rows(rows) => rows.iter().map(|row| row.row_uuid()).collect(),
                SerializedReadResult::Relation(snapshot) => snapshot
                    .rows
                    .iter()
                    .take(snapshot.root_count)
                    .map(|row| row.row_uuid())
                    .collect(),
            };
        }
        turn(client, server);
    }
    panic!("one-shot read did not complete within {max_turns} owner turns");
}

fn all_rows() -> Vec<RowUuid> {
    (0..LABELS.len()).map(row).collect()
}

fn seed(server: &Db, index: usize, label: &str) {
    server
        .seed_settled_mergeable_for_bootstrap(
            "items",
            row(index),
            AuthorSubject::SYSTEM,
            BTreeMap::from([("label".to_owned(), Value::String(label.to_owned()))]),
        )
        .expect("seed server row");
}

/// A host that reports an attempt, attaches its transport, and then reports
/// the link live gets the server's rows as the opening, never its unsettled
/// local one.
///
/// ```text
/// alice: hint Attempting ─ subscribe ─ (withheld) ─ connect ─ hint Live
///                                                        │
/// server ─────────────────────────── first view a..j ────┴─► alice: opening a..j
/// ```
#[test]
fn attempting_then_live_opening_waits_for_the_servers_rows() {
    let server = seeded_server();
    let alice = fresh_client(0x61);
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut stream = subscribe(&alice, &items(), gated());
    assert_withheld(&mut stream, &alice, None, 5);

    connect(&alice, &server);
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    let (reset, mut rows, settled) = opening(first_event(&mut stream, &alice, Some(&server)));
    rows.sort();
    assert!(reset, "the opening is a reset");
    assert!(settled, "the opening carries the settled remote view");
    assert_eq!(rows, all_rows());

    // One-shot: a second fresh client reads the server's rows.
    let bob = fresh_client(0x62);
    bob.set_remote_link_hint(RemoteLinkHint::Attempting);
    connect(&bob, &server);
    bob.set_remote_link_hint(RemoteLinkHint::Live);
    let mut rows = one_shot(&bob, Some(&server), &items(), gated(), MAX_TURNS);
    rows.sort();
    assert_eq!(rows, all_rows());
}

/// An attempt that neither succeeds nor fails holds an opening only for
/// `REMOTE_LINK_ATTEMPT_WINDOW` from the attempt's start, however long the
/// read's own server wait. A read that starts after the window has elapsed
/// does not wait at all.
///
/// ```text
/// alice: hint Attempting ─ subscribe (wait 60 s) ─ (withheld) ── 5 s ──► local opening
///        subscribe / one-shot after the window ──────────────────────► local at once
/// ```
#[test]
fn an_attempt_that_outlives_its_window_releases_the_held_opening() {
    let alice = fresh_client(0x63);
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut held = subscribe(&alice, &items(), gated());
    assert_withheld(&mut held, &alice, None, 3);

    std::thread::sleep(REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(100));
    let (reset, rows, settled) = opening(first_event(&mut held, &alice, None));
    assert!(
        reset && rows.is_empty() && !settled,
        "held opening released as the unsettled local result"
    );

    // The attempt is still reported, but its window is spent: no new wait.
    let mut late = subscribe(&alice, &items(), gated());
    let (reset, rows, settled) = opening(first_event(&mut late, &alice, None));
    assert!(reset && rows.is_empty() && !settled);
    assert!(one_shot(&alice, None, &items(), gated(), 3).is_empty());
}

/// Leaving `Live` for a new attempt is a link loss: an opening held on the
/// lost link is released at once and does not start waiting again on the new
/// attempt's window.
///
/// ```text
/// bob: hint Live ─ subscribe (wait 60 s) ─ (withheld) ─ hint Attempting (reconnect) ─► local opening
/// ```
#[test]
fn leaving_a_live_link_for_a_new_attempt_releases_a_held_opening() {
    let bob = fresh_client(0x6a);
    bob.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&bob, &items(), gated());
    assert_withheld(&mut stream, &bob, None, 5);
    bob.set_remote_link_hint(RemoteLinkHint::Attempting);
    let (reset, rows, settled) = opening(
        stream
            .try_next_event()
            .expect("losing the live link publishes the opening"),
    );
    assert!(reset && rows.is_empty() && !settled);
}

/// Without any host report the core derives reachability from its own
/// upstream: a live link holds the opening, and losing that link releases
/// it. Detaching the own upstream is a loss even while the host
/// still reports the path live.
///
/// ```text
/// alice ══ upstream ══ server (never ticked)
/// alice: subscribe ─ (withheld) ─ detach upstream ─► local opening
/// bob (hint Live): same ─────────────────────────────► local opening
/// ```
#[test]
fn losing_the_own_upstream_releases_a_held_opening() {
    let server = seeded_server();
    let alice = fresh_client(0x67);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(alice.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let mut stream = subscribe(&alice, &items(), gated());
    assert_withheld(&mut stream, &alice, None, 5);

    assert!(alice.detach_connection(&upstream));
    let (reset, rows, settled) = opening(first_event(&mut stream, &alice, None));
    assert!(reset && rows.is_empty() && !settled);

    // A host that still reports the path live cannot keep a held opening
    // waiting on a link the core has lost.
    let bob = fresh_client(0x69);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(bob.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    bob.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&bob, &items(), gated());
    assert_withheld(&mut stream, &bob, None, 5);
    assert!(bob.detach_connection(&upstream));
    let (reset, rows, settled) = opening(first_event(&mut stream, &bob, None));
    assert!(reset && rows.is_empty() && !settled);
}

/// An offset window is read as the strict remote view while the server could
/// answer, so a partly synced cache cannot produce a wrong or empty page.
///
/// ```text
/// alice: remote read of window ─► [e, f]    (local cache now holds e, f only)
/// alice: subscribe window (wait 60 s) ─► first delivery [e, f]
/// alice: one-shot window (wait 60 s)  ─► [e, f]
/// ```
#[test]
fn an_offset_window_reads_the_remote_page_after_a_partial_sync() {
    let server = seeded_server();
    let alice = fresh_client(0x68);
    connect(&alice, &server);
    let expected = vec![row(4), row(5)];
    let remote = ReadOpts {
        tier: jazz::db::ReadTier::Remote,
        ..ReadOpts::default()
    };
    let bytes = postcard::to_allocvec(&window()).expect("encode window");
    let expired = || false;
    let remote_read = alice.all_serialized_query(&bytes, remote, None, None, Some(&expired));
    let mut remote_read = pin!(remote_read);
    let mut context = Context::from_waker(Waker::noop());
    let remote_rows = loop {
        if let Poll::Ready(result) = remote_read.as_mut().poll(&mut context) {
            match result.expect("remote window read") {
                SerializedReadResult::Rows(rows) => {
                    break rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>();
                }
                SerializedReadResult::Relation(_) => panic!("window is a row query"),
            }
        }
        turn(&alice, Some(&server));
    };
    assert_eq!(remote_rows, expected, "the server's window");

    let mut stream = subscribe(&alice, &window(), gated());
    let (reset, rows, settled) = opening(first_event(&mut stream, &alice, Some(&server)));
    assert!(reset && settled);
    assert_eq!(rows, expected, "the first delivery is the server's page");

    assert_eq!(
        one_shot(&alice, Some(&server), &window(), gated(), MAX_TURNS),
        expected
    );
}

/// A window whose remote stops being able to answer before it opens is
/// served by the plain local-first read of the same window, like the one-shot
/// read: a warm cache never shows an empty page because the link is down.
///
/// ```text
/// alice ══ upstream ══ server(a..j): remote read of all items (cache warm)
/// alice: detach, hint Attempting
/// alice: subscribe window (wait 60 s) ─ (withheld) ── 5 s ──► [e, f]
/// alice: subscribe window, then hint Failed ────────────────────► [e, f]
/// alice: one-shot window (wait 60 s) ────────────────────────► [e, f]
/// ```
#[test]
fn an_offset_window_falls_back_to_the_warm_cache_when_the_remote_cannot_answer() {
    let server = seeded_server();
    let alice = fresh_client(0x69);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(alice.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let remote = ReadOpts {
        tier: jazz::db::ReadTier::Remote,
        ..ReadOpts::default()
    };
    let mut warm = subscribe(&alice, &items(), remote);
    let (_, mut rows, settled) = opening(first_event(&mut warm, &alice, Some(&server)));
    rows.sort();
    assert!(settled);
    assert_eq!(rows, all_rows(), "the cache holds every item");
    let expected = vec![row(4), row(5)];

    assert!(alice.detach_connection(&upstream));
    // Delta `added` order is not the window order; compare row sets.
    let page = |event| {
        let (reset, mut rows, _) = opening(event);
        rows.sort();
        (reset, rows)
    };
    let mut plain = subscribe(&alice, &window(), ReadOpts::default());
    let (_, plain_page) = page(first_event(&mut plain, &alice, None));
    assert_eq!(plain_page, expected, "the plain local-first page is cached");

    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut held = subscribe(&alice, &window(), gated());
    assert_withheld(&mut held, &alice, None, 3);
    std::thread::sleep(REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(100));
    let (reset, rows) = page(first_event(&mut held, &alice, None));
    assert!(reset, "the fallback opens with a reset");
    assert_eq!(rows, expected, "the cached page, not an empty one");

    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut released = subscribe(&alice, &window(), gated());
    assert_withheld(&mut released, &alice, None, 3);
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let (reset, rows) = page(first_event(&mut released, &alice, None));
    assert!(reset);
    assert_eq!(rows, expected, "a failed link releases to the cached page");

    assert_eq!(one_shot(&alice, None, &window(), gated(), 10), expected);
    block_on(released.close()).expect("close the fallen-back window");
}

/// A fallen-back window keeps its remote window registered: once the
/// server can answer, the server's page replaces the local one, even though
/// the local-first read of a partly synced cache would never converge on it.
///
/// ```text
/// alice (empty): hint Attempting ─ subscribe window ── 5 s ──► reset []
/// alice ══ connect ══ server(a..j), hint Live ────────────────► [e, f]
/// ```
#[test]
fn a_fallen_back_window_switches_to_the_servers_page_once_it_answers() {
    let server = seeded_server();
    let alice = fresh_client(0x6a);
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut stream = subscribe(&alice, &window(), gated());
    std::thread::sleep(REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(100));
    let (reset, rows, settled) = opening(first_event(&mut stream, &alice, None));
    assert!(reset && rows.is_empty() && !settled, "the empty local page");

    connect(&alice, &server);
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    // The page as a consumer applying each delta sees it; it may only ever
    // hold the server's page rows.
    let expected = vec![row(4), row(5)];
    let mut page = Vec::new();
    let mut settled = false;
    for _ in 0..MAX_TURNS {
        while let Some(event) = stream.try_next_event() {
            let SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                settled: now_settled,
                ..
            } = event
            else {
                panic!("expected a window delta, got {event:?}");
            };
            if reset {
                page.clear();
            }
            page.retain(|row| !removed.iter().any(|gone| gone.row_uuid == *row));
            page.extend(added.iter().map(|row| row.row_uuid()));
            page.sort();
            page.dedup();
            settled = now_settled;
            assert!(
                page.iter().all(|row| expected.contains(row)),
                "the window never shows a row off the server's page: {page:?}"
            );
        }
        if page == expected && settled {
            break;
        }
        turn(&alice, Some(&server));
    }
    assert!(settled, "the window's page is the settled server view");
    assert_eq!(page, expected, "the server's page replaces the local one");
    block_on(stream.close()).expect("close the window");
}

/// A non-durable foreground (a browser tab or an RN foreground) over a
/// durable storage owner (its worker or relay) that is connected to `server`.
struct Foreground {
    tab: Db,
    worker: Db,
}

fn worker_connected_to(node: u8, server: &Db) -> Db {
    let worker = fresh_client(node);
    connect(&worker, server);
    worker
}

fn foreground_over(node: u8, worker: Db) -> Foreground {
    let tab = fresh_client(node);
    tab.set_non_durable_client();
    let (tab_transport, worker_transport) = duplex();
    let _upstream = block_on(tab.connect_upstream(tab_transport));
    let _foreground =
        worker.accept_subscriber(worker_transport, AuthorSubject::for_test_bytes([node; 16]));
    Foreground { tab, worker }
}

impl Foreground {
    /// One owner turn of the tab and its worker; the server takes part only
    /// when given.
    fn turn(&self, server: Option<&Db>) {
        block_on(self.tab.tick()).expect("tick tab");
        block_on(self.worker.tick()).expect("tick worker");
        if let Some(server) = server {
            block_on(server.tick()).expect("tick server");
            block_on(self.worker.tick()).expect("tick worker after server");
        }
        block_on(self.tab.tick()).expect("tick tab after worker");
    }

    fn first_event(
        &self,
        stream: &mut SubscriptionStream,
        server: Option<&Db>,
    ) -> SubscriptionEvent {
        for _ in 0..MAX_TURNS {
            if let Some(event) = stream.try_next_event() {
                return event;
            }
            self.turn(server);
        }
        panic!("no foreground subscription event within {MAX_TURNS} owner turns");
    }

    fn one_shot(&self, server: Option<&Db>, query: &Query) -> Vec<RowUuid> {
        let bytes = postcard::to_allocvec(query).expect("encode query");
        let read = self
            .tab
            .all_serialized_query(&bytes, gated(), None, None, None);
        let mut read = pin!(read);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
                return match result.expect("foreground one-shot read") {
                    SerializedReadResult::Rows(rows) => {
                        rows.iter().map(|row| row.row_uuid()).collect()
                    }
                    SerializedReadResult::Relation(_) => panic!("items is a row query"),
                };
            }
            self.turn(server);
        }
        panic!("foreground one-shot read did not complete within {MAX_TURNS} owner turns");
    }
}

/// A foreground's own stream settles at its storage owner's local answer, so
/// the gate waits for the authority's answer relayed by the owner instead:
/// while the host reports the owner's server link live, a cold owner's empty
/// answer is not published, and the first delivery is the server's rows.
/// Once the gate releases, the stream stays open without a second opening.
///
/// ```text
/// server(a..j) ══ worker(cold) ══ tab (non-durable, hint Live)
/// tab: subscribe ─ worker answers empty (server silent) ─ (withheld)
/// server answers ─► worker ─► tab: opening a..j, no second opening
/// ```
#[test]
fn a_foreground_opening_waits_for_the_servers_rows_through_its_owner() {
    let server = seeded_server();
    let foreground = foreground_over(0x71, worker_connected_to(0x70, &server));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Live);

    let mut stream = subscribe(&foreground.tab, &items(), gated());
    for _ in 0..6 {
        foreground.turn(None);
        assert!(
            stream.try_next_event().is_none(),
            "the owner's empty local answer must not open the stream"
        );
    }
    let (reset, mut rows, settled) = opening(foreground.first_event(&mut stream, Some(&server)));
    rows.sort();
    assert!(reset && settled);
    assert_eq!(rows, all_rows(), "the first delivery is the server's rows");
    foreground.turn(Some(&server));
    assert_no_second_opening(&mut stream);

    // One-shot: a second foreground reads the server's rows through its
    // owner rather than the owner's empty local answer.
    let other = foreground_over(0x72, worker_connected_to(0x73, &server));
    other.tab.set_remote_link_hint(RemoteLinkHint::Live);
    let mut rows = other.one_shot(Some(&server), &items());
    rows.sort();
    assert_eq!(rows, all_rows());
}

/// The authority answering "nothing matches" releases a held foreground
/// opening as an empty result; it does not wait for rows that will not come.
///
/// ```text
/// server(no rows) ══ worker ══ tab (hint Live)
/// tab: subscribe ─ (withheld) ─ server answers ─► tab: empty opening
/// ```
#[test]
fn an_empty_authority_answer_releases_a_foreground_opening() {
    let server = block_on(Db::open_history_complete(config(
        0x52,
        AuthorSubject::SYSTEM,
    )))
    .expect("open empty server");
    let foreground = foreground_over(0x75, worker_connected_to(0x74, &server));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Live);

    let mut stream = subscribe(&foreground.tab, &items(), gated());
    for _ in 0..6 {
        foreground.turn(None);
        assert!(stream.try_next_event().is_none());
    }
    let (reset, rows, _) = opening(foreground.first_event(&mut stream, Some(&server)));
    assert!(reset && rows.is_empty());
    foreground.turn(Some(&server));
    assert_no_second_opening(&mut stream);
}

/// When the owner's server link has failed and the host reports that, a
/// foreground opens with the owner's local answer at once.
///
/// ```text
/// server(a..j)    worker(cold, link failed) ══ tab (hint Failed)
/// tab: subscribe ─► the owner's (empty) local opening
/// ```
#[test]
fn a_foreground_whose_owner_link_failed_opens_at_once() {
    let foreground = foreground_over(0x77, fresh_client(0x76));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Failed);

    let mut stream = subscribe(&foreground.tab, &items(), gated());
    let (reset, rows, _) = opening(foreground.first_event(&mut stream, None));
    assert!(reset && rows.is_empty());
    assert!(foreground.one_shot(None, &items()).is_empty());
}

/// A storage owner connected to `server` whose cache holds `a..j`.
fn warm_worker(node: u8, server: &Db) -> Db {
    let worker = worker_connected_to(node, server);
    let remote = ReadOpts {
        tier: jazz::db::ReadTier::Remote,
        ..ReadOpts::default()
    };
    let mut cache = subscribe(&worker, &items(), remote);
    let (_, mut cached, settled) = opening(first_event(&mut cache, &worker, Some(server)));
    cached.sort();
    assert!(settled);
    assert_eq!(cached, all_rows(), "the worker cached a..j");
    block_on(cache.close()).expect("close the warming read");
    worker
}

/// A warm owner cache does not open a foreground early: unlike the removed
/// unless-empty gate, a non-empty owner answer is withheld too, and the first
/// delivery is the authority's answer relayed by the owner, including a row
/// the owner has not cached. The wait stays bounded by the read's timeout:
/// with the server silent, the owner's cached rows open the stream once it
/// elapses.
///
/// ```text
/// server(a..j, k) ══ worker(cache a..j) ══ tab (hint Live)
/// tab: subscribe (wait 60 s) ─ (withheld, server silent) ─ server answers ─► opening a..k
///
/// quiet(a..j, silent from here) ══ worker(cache a..j) ══ tab (hint Live)
/// tab: subscribe (wait 200 ms) ─ (withheld) ── 200 ms ──► opening a..j from the owner
/// ```
#[test]
fn a_warm_owner_cache_waits_for_the_authority_answer() {
    let server = seeded_server();
    let answered = foreground_over(0x79, warm_worker(0x78, &server));
    seed(&server, 10, "k");
    answered.tab.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&answered.tab, &items(), gated());
    for _ in 0..6 {
        answered.turn(None);
        assert!(
            stream.try_next_event().is_none(),
            "the owner's cached answer must not open the stream"
        );
    }
    let (reset, mut rows, _) = opening(answered.first_event(&mut stream, Some(&server)));
    rows.sort();
    assert!(reset);
    assert_eq!(
        rows,
        (0..11).map(row).collect::<Vec<_>>(),
        "the first delivery is the authority's answer"
    );
    answered.turn(Some(&server));
    assert_no_second_opening(&mut stream);

    // A second server stays silent once it has warmed its owner: the
    // foreground's timeout releases the owner's cached rows.
    let quiet_server = seeded_server();
    let silent = foreground_over(0x7e, warm_worker(0x7d, &quiet_server));
    silent.tab.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&silent.tab, &items(), wait_for_remote(SHORT));
    for _ in 0..3 {
        silent.turn(None);
        assert!(stream.try_next_event().is_none());
    }
    std::thread::sleep(SHORT + Duration::from_millis(50));
    let (reset, mut rows, _) = opening(silent.first_event(&mut stream, None));
    rows.sort();
    assert!(reset);
    assert_eq!(rows, all_rows(), "the owner's cached rows");
    silent.turn(None);
    silent.turn(None);
    assert_no_second_opening(&mut stream);
}

/// A held foreground opening is released when the owner's server link
/// fails (the owner and its host both see it): the owner serves its cached
/// rows at once, and the opening is not repeated.
///
/// ```text
/// server(a..j, silent) ══ worker(cache a..j) ══ tab (hint Live)
/// tab: subscribe ─ (withheld) ─ link Failed ─► opening a..j from the owner
/// ```
#[test]
fn a_held_foreground_opening_is_released_when_its_owner_link_fails() {
    let server = seeded_server();
    let foreground = foreground_over(0x7b, warm_worker(0x7a, &server));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&foreground.tab, &items(), gated());
    for _ in 0..6 {
        foreground.turn(None);
        assert!(stream.try_next_event().is_none());
    }

    foreground
        .worker
        .set_remote_link_hint(RemoteLinkHint::Failed);
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let (reset, mut rows, _) = opening(foreground.first_event(&mut stream, None));
    rows.sort();
    assert!(reset);
    assert_eq!(
        rows,
        all_rows(),
        "the owner's cached rows, not the tab's empty store"
    );
    foreground.turn(None);
    foreground.turn(None);
    assert_no_second_opening(&mut stream);
}

/// Tick `db` once `deadline` has passed.
fn tick_after(db: &Db, deadline: Instant) {
    std::thread::sleep(deadline.saturating_duration_since(Instant::now()));
    block_on(db.tick()).expect("tick past the deadline");
}

/// Alice's two held subscriptions each time out at their own deadline while
/// Bob stays silent: servicing the earlier deadline releases only the read it
/// belongs to, and the later one still releases when its own time comes.
///
/// ```text
/// alice ══ bob (never ticked)
/// subscribe short (200 ms), long (600 ms) ─ (both withheld)
/// tick after 200 ms ─► short opens, long withheld
/// tick after 600 ms ─► long opens
/// ```
#[test]
fn first_load_deadlines_release_each_held_read_at_its_own_time() {
    let bob = seeded_server();
    let alice = fresh_client(0x81);
    connect(&alice, &bob);
    let started = Instant::now();
    let long_wait = Duration::from_millis(600);
    let mut short = subscribe(&alice, &items(), wait_for_remote(SHORT));
    let mut long = subscribe(&alice, &items(), wait_for_remote(long_wait));
    assert_withheld(&mut short, &alice, None, 3);
    assert!(long.try_next_event().is_none());

    tick_after(&alice, started + SHORT + Duration::from_millis(50));
    let (reset, rows, _) = opening(
        short
            .try_next_event()
            .expect("the short read releases at its deadline"),
    );
    assert!(reset && rows.is_empty());
    // On a machine slow enough to pass the long deadline already, the long
    // read may be released by now too; otherwise it is still held.
    if started.elapsed() < long_wait {
        assert!(
            long.try_next_event().is_none(),
            "the long read is held until its own deadline"
        );
    }

    tick_after(&alice, started + long_wait + Duration::from_millis(50));
    let (reset, rows, _) = opening(
        long.try_next_event()
            .expect("the long read releases at its deadline"),
    );
    assert!(reset && rows.is_empty());
}

/// Alice's long first-load wait releases when her connection attempt
/// expires, even after an unrelated read's earlier deadline was serviced
/// first.
///
/// ```text
/// hint Attempting ─ subscribe held (60 s), early (200 ms)
/// tick after 200 ms ─► early opens, held withheld
/// tick after the attempt window ─► held opens with the local result
/// ```
#[test]
fn first_load_attempt_expiry_releases_after_an_earlier_deadline() {
    let alice = fresh_client(0x82);
    let started = Instant::now();
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut held = subscribe(&alice, &items(), gated());
    let mut early = subscribe(&alice, &items(), wait_for_remote(SHORT));
    assert_withheld(&mut held, &alice, None, 3);

    tick_after(&alice, started + SHORT + Duration::from_millis(50));
    let (reset, rows, _) = opening(
        early
            .try_next_event()
            .expect("the early read releases at its deadline"),
    );
    assert!(reset && rows.is_empty());
    assert!(
        held.try_next_event().is_none(),
        "the attempt window has not expired yet"
    );

    tick_after(
        &alice,
        started + REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(100),
    );
    let (reset, rows, _) = opening(
        held.try_next_event()
            .expect("attempt expiry must release the local opening"),
    );
    assert!(reset && rows.is_empty());
}

/// The link reports a browser host sends over a client's life, ending in
/// shutdown, with an answered gated subscription, a held gated window, an
/// armed attempt window and a one-shot racing the remote. Every held read
/// is released, and closing the Db and then ticking it (as a host timer
/// firing late would) and reporting the link again are harmless.
///
/// ```text
/// hint Attempting ─ connect ─ hint Live ─ items opens a..j (answered)
///   window subscribed (held) ─ one-shot window (racing the remote)
/// shutdown: hint Attempting ─► window opens (link lost), one-shot falls back
///           to the local window [e, f] of the synced cache
///           hint Failed ─ detach ─ close ─ timer tick ─ hint Failed
/// ```
#[test]
fn a_host_shutdown_link_sequence_releases_held_reads_and_closes_cleanly() {
    let server = seeded_server();
    let alice = fresh_client(0x7c);
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(alice.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    alice.set_remote_link_hint(RemoteLinkHint::Live);

    let mut answered = subscribe(&alice, &items(), gated());
    let (_, mut rows, _) = opening(first_event(&mut answered, &alice, Some(&server)));
    rows.sort();
    assert_eq!(rows, all_rows());

    // The server stays silent from here on: both window reads are held.
    let mut held = subscribe(&alice, &window(), gated());
    let bytes = postcard::to_allocvec(&window()).expect("encode window");
    let one_shot = alice.all_serialized_query(&bytes, gated(), None, None, None);
    let mut one_shot = pin!(one_shot);
    let mut context = Context::from_waker(Waker::noop());
    assert!(one_shot.as_mut().poll(&mut context).is_pending());
    assert_withheld(&mut held, &alice, None, 3);
    assert!(one_shot.as_mut().poll(&mut context).is_pending());

    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let (reset, _, settled) = opening(
        held.try_next_event()
            .expect("losing the live link releases the held window"),
    );
    assert!(reset && !settled);
    let Poll::Ready(result) = one_shot.as_mut().poll(&mut context) else {
        panic!("the one-shot falls back once the live link is lost");
    };
    let rows = match result.expect("fallback read") {
        SerializedReadResult::Rows(rows) => rows,
        SerializedReadResult::Relation(_) => panic!("window is a row query"),
    };
    assert_eq!(
        rows.iter().map(|row| row.row_uuid()).collect::<Vec<_>>(),
        vec![row(4), row(5)],
        "the local-first window over the synced cache"
    );

    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(alice.tick()).expect("tick after the link failed");
    assert!(alice.detach_connection(&upstream));
    block_on(alice.tick()).expect("tick after detaching");
    block_on(alice.close()).expect("close");
    // A host timer fires after close, and the host reports the link once
    // more while shutting down: neither may disturb the closed Db.
    let _ = block_on(alice.tick());
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let _ = block_on(alice.tick());
    drop(held);
    drop(answered);
    let _ = block_on(alice.tick());
}

/// Alice's local-first subscription, opened with Bob's answer, keeps her
/// pending writes visible, while her Remote subscription to the same query
/// waits for Bob's confirmation.
///
/// ```text
/// bob ── settled rows ──► alice: local-first + Remote openings
/// alice ── pending insert ──► local-first delta
/// bob is not ticked: Remote has no confirmation and publishes nothing
/// ```
#[test]
fn remote_answer_coverage_preserves_local_first_pending_writes() {
    let bob = seeded_server();
    let alice = fresh_client(0x7e);
    connect(&alice, &bob);
    let mut local_first = subscribe(&alice, &items(), gated());
    let mut remote = subscribe(
        &alice,
        &items(),
        ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
    );
    let mut opened = opening(first_event(&mut local_first, &alice, Some(&bob))).1;
    opened.sort();
    assert_eq!(opened, all_rows());
    let mut opened = opening(first_event(&mut remote, &alice, Some(&bob))).1;
    opened.sort();
    assert_eq!(opened, all_rows());
    block_on(alice.insert(
        "items",
        BTreeMap::from([("label".to_owned(), Value::String("pending".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row(20)),
            ..Default::default()
        },
    ))
    .expect("Alice's pending insert");
    let (reset, added, _) = opening(first_event(&mut local_first, &alice, None));
    assert!(!reset);
    assert_eq!(added, vec![row(20)]);
    assert_withheld(&mut remote, &alice, None, 8);
    block_on(local_first.close()).expect("close local-first read");
    block_on(remote.close()).expect("close remote read");
}

/// A foreground over a storage owner that has no server (a browser app
/// without `serverUrl`): its own write opens its subscription, and a
/// second foreground over the same owner sees it too.
#[test]
fn a_foreground_over_an_owner_without_a_server_reads_the_owners_store() {
    // A binding's store is a replica from the start, server or not.
    let worker = fresh_client(0x80);
    worker.become_replica();
    let foreground = foreground_over(0x81, worker);
    block_on(foreground.tab.insert(
        "items",
        BTreeMap::from([("label".to_owned(), Value::String("local".to_owned()))]),
        jazz::db::InsertOptions {
            row_id: Some(row(30)),
            ..Default::default()
        },
    ))
    .expect("insert through the foreground");
    let mut stream = subscribe(&foreground.tab, &items(), ReadOpts::default());
    let (_, rows, _) = opening(foreground.first_event(&mut stream, None));
    assert_eq!(rows, vec![row(30)]);

    let other = fresh_client(0x82);
    other.set_non_durable_client();
    let (tab_transport, worker_transport) = duplex();
    let _upstream = block_on(other.connect_upstream(tab_transport));
    let _foreground = foreground
        .worker
        .accept_subscriber(worker_transport, AuthorSubject::for_test_bytes([0x82; 16]));
    let mut stream = subscribe(&other, &items(), ReadOpts::default());
    let rows = (0..MAX_TURNS)
        .find_map(|_| {
            let event = stream.try_next_event();
            if event.is_none() {
                block_on(other.tick()).expect("tick second tab");
                foreground.turn(None);
                block_on(other.tick()).expect("tick second tab after worker");
            }
            event
        })
        .map(|event| opening(event).1)
        .expect("the second tab's opening");
    assert_eq!(rows, vec![row(30)], "the second tab reads the owner's rows");
}

/// A foreground's one-shot read with a first-load wait, through a worker
/// whose live server does not answer: the worker serves its store once the
/// wait runs out, and the read completes with the rows held locally.
///
/// ```text
/// server (silent) ══ worker ══ tab (hint Live)
/// tab: all(wait 200ms) ─ worker relays ─ (no answer) ─ 200ms ─► worker's rows
/// ```
#[test]
fn a_foreground_one_shot_falls_back_to_its_owner_when_a_live_server_is_silent() {
    let server = seeded_server();
    let foreground = foreground_over(0x75, worker_connected_to(0x74, &server));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Live);
    foreground.worker.set_remote_link_hint(RemoteLinkHint::Live);

    // An open subscription to the same query, released by its own wait.
    let mut stream = subscribe(&foreground.tab, &items(), wait_for_remote(SHORT));
    let opened = Instant::now();
    while stream.try_next_event().is_none() {
        assert!(
            opened.elapsed() < Duration::from_secs(5),
            "the opening is released"
        );
        std::thread::sleep(Duration::from_millis(10));
        foreground.turn(None);
    }

    let bytes = postcard::to_allocvec(&items()).expect("encode query");
    let started = Instant::now();
    let read =
        foreground
            .tab
            .all_serialized_query(&bytes, wait_for_remote(SHORT), None, None, None);
    let mut read = pin!(read);
    let mut context = Context::from_waker(Waker::noop());
    let rows = loop {
        if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
            break match result.expect("foreground one-shot read") {
                SerializedReadResult::Rows(rows) => rows.len(),
                SerializedReadResult::Relation(_) => panic!("items is a row query"),
            };
        }
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the read must not outlive its first-load wait"
        );
        std::thread::sleep(Duration::from_millis(10));
        foreground.turn(None);
    };
    assert!(started.elapsed() >= SHORT);
    assert_eq!(rows, 0, "the cold worker holds none of the server's rows");
}
