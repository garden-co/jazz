//! The core `Db` local-first-unless-empty gate (`EmptyOpening::AwaitRemote`)
//! and the host remote-link hint.
//!
//! Every client here is a core `Db` connected to a history-complete server
//! `Db` over an in-memory duplex transport, with ticks driven explicitly, so
//! "the server has not answered yet" is a deterministic state rather than a
//! race.

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

mod common;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, EmptyOpening, LocalUpdates, REMOTE_LINK_ATTEMPT_WINDOW, ReadOpts,
    RemoteLinkHint, SerializedReadResult, SubscriptionEvent, SubscriptionStream,
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
