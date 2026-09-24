//! The core `Db` local-first-unless-empty gate (`EmptyOpening::AwaitRemote`)
//! and the host remote-link hint.
//!
//! Every client here is a core `Db` connected to a history-complete server
//! `Db` over an in-memory duplex transport, with ticks driven explicitly, so
//! "the server has not answered yet" is a deterministic state rather than a
//! race.

use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, EmptyOpening, LocalUpdates, REMOTE_LINK_ATTEMPT_WINDOW, ReadOpts,
    RemoteLinkHint, SerializedReadResult, SubscriptionEvent, SubscriptionStream, TickScheduler,
    TickUrgency,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_testkit::duplex_transport::duplex;

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

fn config(node: u8, author: AuthorSubject) -> DbConfig<TestStorage> {
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    DbConfig::new(
        schema,
        TestStorage::new(&refs),
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
fn seeded_server() -> Db<TestStorage> {
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
fn fresh_client(node: u8) -> Db<TestStorage> {
    block_on(Db::open(config(
        node,
        AuthorSubject::for_test_bytes([node; 16]),
    )))
    .expect("open client")
}

fn connect(client: &Db<TestStorage>, server: &Db<TestStorage>) {
    let (client_transport, server_transport) = duplex();
    let _upstream = block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
}

fn turn(client: &Db<TestStorage>, server: Option<&Db<TestStorage>>) {
    block_on(client.tick()).expect("tick client");
    if let Some(server) = server {
        block_on(server.tick()).expect("tick server");
        block_on(client.tick()).expect("tick client after server");
    }
}

fn unless_empty() -> ReadOpts {
    ReadOpts {
        empty_opening: EmptyOpening::AwaitRemote,
        ..ReadOpts::default()
    }
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

fn subscribe(client: &Db<TestStorage>, query: &Query, opts: ReadOpts) -> SubscriptionStream {
    let prepared = client.prepare_query(query).expect("prepare query");
    block_on(client.subscribe(&prepared, opts)).expect("subscribe")
}

/// The first event, driving owner turns until one is published.
fn first_event(
    stream: &mut SubscriptionStream,
    client: &Db<TestStorage>,
    server: Option<&Db<TestStorage>>,
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
    client: &Db<TestStorage>,
    server: Option<&Db<TestStorage>>,
    turns: usize,
) {
    for _ in 0..turns {
        turn(client, server);
        if let Some(event) = stream.try_next_event() {
            panic!("the empty opening was published while it should be withheld: {event:?}");
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

/// Run a host one-shot read to completion, driving owner turns between polls
/// the way bindings re-poll a pending native read.
fn one_shot(
    client: &Db<TestStorage>,
    server: Option<&Db<TestStorage>>,
    query: &Query,
    opts: ReadOpts,
    max_turns: usize,
) -> Vec<RowUuid> {
    let bytes = postcard::to_allocvec(query).expect("encode query");
    let read = client.all_serialized_query(
        &bytes,
        opts,
        None,
        None,
        None,
        false,
        || false,
        |attachment| client.detach_query(attachment),
    );
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

/// A host that reports an attempt, attaches its transport, and then reports
/// the link live gets the server's rows as the opening, never an empty frame.
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
    let mut stream = subscribe(&alice, &items(), unless_empty());
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
    let mut rows = one_shot(&bob, Some(&server), &items(), unless_empty(), MAX_TURNS);
    rows.sort();
    assert_eq!(rows, all_rows());
}

/// An attempt that neither succeeds nor fails holds an empty opening only
/// for `REMOTE_LINK_ATTEMPT_WINDOW` from the attempt's start. A read that
/// starts after the window has elapsed does not wait at all.
///
/// ```text
/// alice: hint Attempting ─ subscribe ─ (withheld) ── 5 s ──► empty opening
///        subscribe / one-shot after the window ─────────────► empty at once
/// ```
#[test]
fn an_attempt_that_outlives_its_window_releases_the_empty_opening() {
    let alice = fresh_client(0x63);
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut held = subscribe(&alice, &items(), unless_empty());
    assert_withheld(&mut held, &alice, None, 3);

    std::thread::sleep(REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(100));
    let (reset, rows, settled) = opening(first_event(&mut held, &alice, None));
    assert!(
        reset && rows.is_empty() && !settled,
        "held opening released empty"
    );

    // The attempt is still reported, but its window is spent: no new wait.
    let mut late = subscribe(&alice, &items(), unless_empty());
    let (reset, rows, settled) = opening(first_event(&mut late, &alice, None));
    assert!(reset && rows.is_empty() && !settled);
    assert!(one_shot(&alice, None, &items(), unless_empty(), 3).is_empty());
}

/// `Failed` (or `NoServer`) means nothing can answer: the empty local
/// opening is delivered at once, even with a connected upstream relay.
///
/// ```text
/// alice ══ upstream ══ server(a..j)      hint Failed
/// alice: subscribe ─► empty opening (server not consulted first)
/// ```
#[test]
fn a_failed_link_delivers_the_empty_opening_immediately() {
    let server = seeded_server();
    for (node, hint) in [
        (0x64, RemoteLinkHint::Failed),
        (0x65, RemoteLinkHint::NoServer),
    ] {
        let alice = fresh_client(node);
        connect(&alice, &server);
        alice.set_remote_link_hint(hint);
        let mut stream = subscribe(&alice, &items(), unless_empty());
        let (reset, rows, settled) = opening(first_event(&mut stream, &alice, None));
        assert!(reset && rows.is_empty() && !settled, "{hint:?}");
        assert!(one_shot(&alice, None, &items(), unless_empty(), 3).is_empty());
    }
}

/// A held opening is released as soon as the host reports the link failed,
/// or reports that a live link was lost and a new attempt started.
///
/// ```text
/// alice: hint Live ─ subscribe ─ (withheld, server silent) ─ hint Failed ─► empty opening
/// bob:   hint Live ─ subscribe ─ (withheld) ─ hint Attempting (reconnect) ─► empty opening
/// ```
#[test]
fn a_held_opening_is_released_when_the_host_reports_failure() {
    let alice = fresh_client(0x66);
    alice.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&alice, &items(), unless_empty());
    assert_withheld(&mut stream, &alice, None, 5);

    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let (reset, rows, settled) = opening(
        stream
            .try_next_event()
            .expect("the failure report publishes the opening"),
    );
    assert!(reset && rows.is_empty() && !settled);

    // Leaving `Live` for a new attempt is a link loss too: an opening held
    // on the lost link does not start waiting again on the new attempt.
    let bob = fresh_client(0x6a);
    bob.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&bob, &items(), unless_empty());
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
/// upstream: a live link holds the empty opening, and losing that link
/// releases it. Detaching the own upstream is a loss even while the host
/// still reports the path live.
///
/// ```text
/// alice ══ upstream ══ server (never ticked)
/// alice: subscribe ─ (withheld) ─ detach upstream ─► empty opening
/// bob (hint Live): same ─────────────────────────────► empty opening
/// ```
#[test]
fn losing_the_own_upstream_releases_a_held_opening() {
    let server = seeded_server();
    let alice = fresh_client(0x67);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(alice.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let mut stream = subscribe(&alice, &items(), unless_empty());
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
    let mut stream = subscribe(&bob, &items(), unless_empty());
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
/// alice: subscribe window (unless-empty) ─► first delivery [e, f]
/// alice: one-shot window (unless-empty)  ─► [e, f]
/// ```
#[test]
fn an_offset_window_reads_the_remote_page_after_a_partial_sync() {
    let server = seeded_server();
    let alice = fresh_client(0x68);
    connect(&alice, &server);
    let expected = vec![row(4), row(5)];
    let remote = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Immediate,
        ..ReadOpts::default()
    };
    let bytes = postcard::to_allocvec(&window()).expect("encode window");
    let remote_read = alice.all_serialized_query(
        &bytes,
        remote,
        None,
        None,
        None,
        true,
        || false,
        |attachment| alice.detach_query(attachment),
    );
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

    let mut stream = subscribe(&alice, &window(), unless_empty());
    let (reset, rows, settled) = opening(first_event(&mut stream, &alice, Some(&server)));
    assert!(reset && settled);
    assert_eq!(rows, expected, "the first delivery is the server's page");

    assert_eq!(
        one_shot(&alice, Some(&server), &window(), unless_empty(), MAX_TURNS),
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
/// alice: subscribe window (unless-empty) ─ (withheld) ── 5 s ──► [e, f]
/// alice: subscribe window, then hint Failed ────────────────────► [e, f]
/// alice: one-shot window (unless-empty) ────────────────────────► [e, f]
/// ```
#[test]
fn an_offset_window_falls_back_to_the_warm_cache_when_the_remote_cannot_answer() {
    let server = seeded_server();
    let alice = fresh_client(0x69);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(alice.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let remote = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Immediate,
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
    let mut held = subscribe(&alice, &window(), unless_empty());
    assert_withheld(&mut held, &alice, None, 3);
    std::thread::sleep(REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(100));
    let (reset, rows) = page(first_event(&mut held, &alice, None));
    assert!(reset, "the fallback opens with a reset");
    assert_eq!(rows, expected, "the cached page, not an empty one");

    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut released = subscribe(&alice, &window(), unless_empty());
    assert_withheld(&mut released, &alice, None, 3);
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let (reset, rows) = page(first_event(&mut released, &alice, None));
    assert!(reset);
    assert_eq!(rows, expected, "a failed link releases to the cached page");

    assert_eq!(
        one_shot(&alice, None, &window(), unless_empty(), 10),
        expected
    );
    block_on(released.close()).expect("close the fallen-back window");
}

/// A non-durable foreground (a browser tab or an RN foreground) over a
/// durable storage owner (its worker or relay) that is connected to `server`.
struct Foreground {
    tab: Db<TestStorage>,
    worker: Db<TestStorage>,
}

fn worker_connected_to(node: u8, server: &Db<TestStorage>) -> Db<TestStorage> {
    let worker = fresh_client(node);
    connect(&worker, server);
    worker
}

fn foreground_over(node: u8, worker: Db<TestStorage>) -> Foreground {
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
    fn turn(&self, server: Option<&Db<TestStorage>>) {
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
        server: Option<&Db<TestStorage>>,
    ) -> SubscriptionEvent {
        for _ in 0..MAX_TURNS {
            if let Some(event) = stream.try_next_event() {
                return event;
            }
            self.turn(server);
        }
        panic!("no foreground subscription event within {MAX_TURNS} owner turns");
    }

    fn one_shot(&self, server: Option<&Db<TestStorage>>, query: &Query) -> Vec<RowUuid> {
        let bytes = postcard::to_allocvec(query).expect("encode query");
        let read = self.tab.all_serialized_query(
            &bytes,
            unless_empty(),
            None,
            None,
            None,
            false,
            || false,
            |attachment| self.tab.detach_query(attachment),
        );
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

    /// Coverage groups the tab holds upstream. Internal: whether the gate's
    /// authority witness was retired has no public observable, because the
    /// retained owner-local coverage delivers the same rows either way.
    fn coverage_groups(&self) -> usize {
        self.tab.query_coverage_attachment_counts_for_test().0
    }
}

/// A foreground's own stream settles at its storage owner's local answer, so
/// the gate waits for the authority's answer relayed by the owner instead:
/// while the host reports the owner's server link live, a cold owner's empty
/// answer is not published, and the first delivery is the server's rows.
/// Once the gate releases, only the ordinary owner-local coverage remains.
///
/// ```text
/// server(a..j) ══ worker(cold) ══ tab (non-durable, hint Live)
/// tab: subscribe ─ worker answers empty (server silent) ─ (withheld)
/// server answers ─► worker ─► tab: opening a..j, witness coverage retired
/// ```
#[test]
fn a_foreground_opening_waits_for_the_servers_rows_through_its_owner() {
    let server = seeded_server();
    let foreground = foreground_over(0x71, worker_connected_to(0x70, &server));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Live);

    let mut stream = subscribe(&foreground.tab, &items(), unless_empty());
    for _ in 0..6 {
        foreground.turn(None);
        assert!(
            stream.try_next_event().is_none(),
            "the owner's empty local answer must not open the stream"
        );
    }
    assert_eq!(
        foreground.coverage_groups(),
        2,
        "owner-local coverage plus the gate's authority witness"
    );

    let (reset, mut rows, settled) = opening(foreground.first_event(&mut stream, Some(&server)));
    rows.sort();
    assert!(reset && settled);
    assert_eq!(rows, all_rows(), "the first delivery is the server's rows");
    foreground.turn(Some(&server));
    assert_eq!(
        foreground.coverage_groups(),
        1,
        "a released gate keeps only the local-first coverage"
    );

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

    let mut stream = subscribe(&foreground.tab, &items(), unless_empty());
    for _ in 0..6 {
        foreground.turn(None);
        assert!(stream.try_next_event().is_none());
    }
    let (reset, rows, _) = opening(foreground.first_event(&mut stream, Some(&server)));
    assert!(reset && rows.is_empty());
    foreground.turn(Some(&server));
    assert_eq!(foreground.coverage_groups(), 1);
}

/// When the owner's server link has failed and the host reports that, a
/// foreground opens with the owner's (empty) local answer at once.
///
/// ```text
/// server(a..j)    worker(cold, link failed) ══ tab (hint Failed)
/// tab: subscribe ─► empty opening, no witness coverage
/// ```
#[test]
fn a_foreground_whose_owner_link_failed_opens_empty_at_once() {
    let foreground = foreground_over(0x77, fresh_client(0x76));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Failed);

    let mut stream = subscribe(&foreground.tab, &items(), unless_empty());
    let (reset, rows, _) = opening(foreground.first_event(&mut stream, None));
    assert!(reset && rows.is_empty());
    assert_eq!(foreground.coverage_groups(), 1, "no authority witness");
    assert!(foreground.one_shot(None, &items()).is_empty());
}

/// A warm owner cache is the foreground's local answer: it opens the stream
/// at once even while the owner is still attempting to reach the server.
///
/// ```text
/// server(a..j) ══ worker ─ caches a..j ─ loses its server link
/// worker(a..j) ══ tab (hint Attempting)
/// tab: subscribe ─► opening a..j from the worker, without waiting
/// ```
#[test]
fn a_warm_owner_cache_opens_a_foreground_at_once() {
    let server = seeded_server();
    let worker = fresh_client(0x78);
    let (worker_transport, server_transport) = duplex();
    let upstream = block_on(worker.connect_upstream(worker_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    let mut cache = subscribe(&worker, &items(), unless_empty());
    let (_, cached, _) = opening(first_event(&mut cache, &worker, Some(&server)));
    assert_eq!(cached.len(), LABELS.len(), "the worker cached a..j");
    assert!(worker.detach_connection(&upstream));

    let foreground = foreground_over(0x79, worker);
    foreground
        .tab
        .set_remote_link_hint(RemoteLinkHint::Attempting);
    let mut stream = subscribe(&foreground.tab, &items(), unless_empty());
    let (reset, mut rows, _) = opening(foreground.first_event(&mut stream, None));
    rows.sort();
    assert!(reset);
    assert_eq!(rows, all_rows(), "the owner's cached rows");
    foreground.turn(None);
    assert_eq!(foreground.coverage_groups(), 1);
}

/// A held foreground opening is released when the host reports that the
/// owner's server link failed, and the witness coverage is retired.
///
/// ```text
/// server(a..j, silent) ══ worker ══ tab (hint Live)
/// tab: subscribe ─ (withheld) ─ hint Failed ─► empty opening, witness retired
/// ```
#[test]
fn a_held_foreground_opening_is_released_when_its_owner_link_fails() {
    let server = seeded_server();
    let foreground = foreground_over(0x7b, worker_connected_to(0x7a, &server));
    foreground.tab.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&foreground.tab, &items(), unless_empty());
    for _ in 0..6 {
        foreground.turn(None);
        assert!(stream.try_next_event().is_none());
    }

    foreground.tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let (reset, rows, _) = opening(
        stream
            .try_next_event()
            .expect("the failure report publishes the opening"),
    );
    assert!(reset && rows.is_empty());
    foreground.turn(None);
    foreground.turn(None);
    assert_eq!(foreground.coverage_groups(), 1);
}

/// A host tick scheduler like the WASM and NAPI hosts': it records wakes and
/// timer requests instead of running them, so the test fires them itself.
#[derive(Default)]
struct RecordingScheduler {
    timers_ms: RefCell<Vec<u64>>,
}

impl TickScheduler for RecordingScheduler {
    fn schedule_tick(&self, _urgency: TickUrgency) {}

    fn schedule_tick_after(&self, delay_ms: u64) {
        self.timers_ms.borrow_mut().push(delay_ms);
    }
}

/// The link reports a browser host sends over a client's life, ending in
/// shutdown, with an answered gated subscription, a held gated window, an
/// armed attempt window and a one-shot racing the remote. Every held read
/// is released, the attempt timer is requested from the host scheduler, and
/// closing the Db and then firing that timer and reporting the link again
/// are harmless.
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
    let scheduler = Rc::new(RecordingScheduler::default());
    alice.set_tick_scheduler(Some(scheduler.clone()));
    alice.set_remote_link_hint(RemoteLinkHint::Attempting);
    let (client_transport, server_transport) = duplex();
    let upstream = block_on(alice.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);
    alice.set_remote_link_hint(RemoteLinkHint::Live);

    let mut answered = subscribe(&alice, &items(), unless_empty());
    let (_, mut rows, _) = opening(first_event(&mut answered, &alice, Some(&server)));
    rows.sort();
    assert_eq!(rows, all_rows());

    // The server stays silent from here on: both window reads are held.
    let mut held = subscribe(&alice, &window(), unless_empty());
    let bytes = postcard::to_allocvec(&window()).expect("encode window");
    let one_shot = alice.all_serialized_query(
        &bytes,
        unless_empty(),
        None,
        None,
        None,
        false,
        || false,
        |attachment| alice.detach_query(attachment),
    );
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
    assert!(
        scheduler
            .timers_ms
            .borrow()
            .iter()
            .any(|delay| *delay <= REMOTE_LINK_ATTEMPT_WINDOW.as_millis() as u64 + 1),
        "the attempt window asks the host for a timer tick"
    );
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
    // The host's attempt timer fires after close, and the host reports the
    // link once more while shutting down: neither may disturb the closed Db.
    let _ = block_on(alice.tick());
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let _ = block_on(alice.tick());
    drop(held);
    drop(answered);
    let _ = block_on(alice.tick());
}
