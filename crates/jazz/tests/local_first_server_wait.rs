//! Local first with a server-wait timeout (`FirstLoad::WaitForRemote`):
//! the opening, empty or not, waits a bounded time for the server's answer
//! while the remote could answer, and never waits when it cannot.
//!
//! Every client here is a core `Db` connected to a history-complete server
//! `Db` over an in-memory duplex transport, with ticks driven explicitly, so
//! "the server has not answered yet" is a deterministic state rather than a
//! race.

// Shared with jazz-testkit by path so Jazz needs no testkit dev-dependency.
#[path = "../../jazz-testkit/src/duplex_transport.rs"]
mod duplex_transport;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::Duration;

mod common;

use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, FirstLoad, LocalUpdates, ReadOpts, RemoteLinkHint,
    SerializedReadResult, SubscriptionEvent, SubscriptionStream,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

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

fn first_load_remote_wait(timeout: Duration) -> ReadOpts {
    ReadOpts {
        first_load: FirstLoad::WaitForRemote {
            timeout_ms: timeout.as_millis() as u64,
        },
        ..ReadOpts::default()
    }
}

/// Long enough that no test here reaches it by accident.
const LONG: Duration = Duration::from_secs(60);
/// Short enough to sleep through.
const SHORT: Duration = Duration::from_millis(200);

fn items() -> Query {
    Query::from("items")
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

/// A client whose cache holds `a..j`, still connected to the server.
fn warm_client(node: u8, server: &Db) -> Db {
    let client = fresh_client(node);
    connect(&client, server);
    client.set_remote_link_hint(RemoteLinkHint::Live);
    let remote = ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Immediate,
        ..ReadOpts::default()
    };
    let mut warm = subscribe(&client, &items(), remote);
    let (_, mut rows, settled) = opening(first_event(&mut warm, &client, Some(server)));
    rows.sort();
    assert!(settled);
    assert_eq!(rows, all_rows(), "the cache holds every item");
    block_on(warm.close()).expect("close the warming read");
    client
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

/// Run a one-shot read, sleeping between owner turns so a wall-clock
/// deadline can pass while the server stays silent.
fn slow_one_shot(client: &Db, query: &Query, opts: ReadOpts) -> Vec<RowUuid> {
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
    for _ in 0..MAX_TURNS {
        if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
            return match result.expect("one-shot read") {
                SerializedReadResult::Rows(rows) => rows.iter().map(|row| row.row_uuid()).collect(),
                SerializedReadResult::Relation(_) => panic!("items is a row query"),
            };
        }
        std::thread::sleep(Duration::from_millis(10));
        turn(client, None);
    }
    panic!("one-shot read did not complete within {MAX_TURNS} slow owner turns");
}

/// Unlike the deprecated unless-empty gate, a warm cache waits too: the
/// opening is the server's answer, including a row the cache has not seen.
///
/// ```text
/// alice (cache a..j) ══ server(a..j, k)
/// alice: subscribe (wait 60 s) ─ (withheld, server silent) ─ server answers ─► opening a..k, settled
/// alice: one-shot  (wait 60 s) ─────────────────────── server answers ─► a..l
/// ```
#[test]
fn a_warm_cache_waits_for_the_servers_answer() {
    let server = seeded_server();
    let alice = warm_client(0x71, &server);
    seed(&server, 10, "k");

    let mut stream = subscribe(&alice, &items(), first_load_remote_wait(LONG));
    assert_withheld(&mut stream, &alice, None, 5);
    let (reset, mut rows, settled) = opening(first_event(&mut stream, &alice, Some(&server)));
    rows.sort();
    assert!(reset, "the opening is a reset");
    assert!(settled, "the opening is the server's answer");
    assert_eq!(rows, (0..11).map(row).collect::<Vec<_>>());

    seed(&server, 11, "l");
    let mut rows = one_shot(
        &alice,
        Some(&server),
        &items(),
        first_load_remote_wait(LONG),
        MAX_TURNS,
    );
    rows.sort();
    assert_eq!(rows, (0..12).map(row).collect::<Vec<_>>());
}

/// A server that does not answer in time releases the local result at the
/// deadline; the stream then behaves like plain local-first.
///
/// ```text
/// alice (cache a..j) ══ server (never ticked)
/// alice: subscribe (wait 200 ms) ─ (withheld) ── 200 ms ──► opening a..j, unsettled
/// alice: one-shot  (wait 200 ms) ──────────────── 200 ms ──► a..j
/// ```
#[test]
fn the_timeout_releases_the_local_result() {
    let server = seeded_server();
    let alice = warm_client(0x72, &server);

    let mut stream = subscribe(&alice, &items(), first_load_remote_wait(SHORT));
    assert_withheld(&mut stream, &alice, None, 3);
    std::thread::sleep(SHORT + Duration::from_millis(50));
    let (reset, mut rows, settled) = opening(first_event(&mut stream, &alice, None));
    rows.sort();
    assert!(
        reset && !settled,
        "the local result, before the server answered"
    );
    assert_eq!(rows, all_rows());

    let mut rows = slow_one_shot(&alice, &items(), first_load_remote_wait(SHORT));
    rows.sort();
    assert_eq!(
        rows,
        all_rows(),
        "the one-shot falls back to the local result"
    );
}

/// Without a server that could answer, nothing waits, whatever the timeout.
/// A zero timeout is plain local-first even while the server could answer.
///
/// ```text
/// alice ══ server   hint Failed / NoServer ─► opening at once
/// bob   ══ server   hint Live, wait 0 ───────► opening at once
/// ```
#[test]
fn nothing_waits_when_the_server_cannot_answer_or_the_timeout_is_zero() {
    let server = seeded_server();
    for (node, hint) in [
        (0x73, RemoteLinkHint::Failed),
        (0x74, RemoteLinkHint::NoServer),
    ] {
        let alice = fresh_client(node);
        connect(&alice, &server);
        alice.set_remote_link_hint(hint);
        let mut stream = subscribe(&alice, &items(), first_load_remote_wait(LONG));
        let (reset, rows, settled) = opening(first_event(&mut stream, &alice, None));
        assert!(reset && rows.is_empty() && !settled, "{hint:?}");
        assert!(one_shot(&alice, None, &items(), first_load_remote_wait(LONG), 3).is_empty());
    }

    let bob = fresh_client(0x75);
    connect(&bob, &server);
    bob.set_remote_link_hint(RemoteLinkHint::Live);
    let mut stream = subscribe(&bob, &items(), first_load_remote_wait(Duration::ZERO));
    let (reset, rows, settled) = opening(first_event(&mut stream, &bob, None));
    assert!(reset && rows.is_empty() && !settled);
    assert!(
        one_shot(
            &bob,
            None,
            &items(),
            first_load_remote_wait(Duration::ZERO),
            3
        )
        .is_empty()
    );
}

/// A held opening is released as soon as the link fails, long before its
/// timeout.
///
/// ```text
/// alice (cache a..j): subscribe (wait 60 s) ─ (withheld) ─ hint Failed ─► opening a..j
/// ```
#[test]
fn a_failed_link_releases_a_held_opening_before_its_timeout() {
    let server = seeded_server();
    let alice = warm_client(0x76, &server);
    let mut stream = subscribe(&alice, &items(), first_load_remote_wait(LONG));
    assert_withheld(&mut stream, &alice, None, 3);
    alice.set_remote_link_hint(RemoteLinkHint::Failed);
    let (reset, mut rows, settled) = opening(first_event(&mut stream, &alice, None));
    rows.sort();
    assert!(reset && !settled);
    assert_eq!(rows, all_rows());
}
