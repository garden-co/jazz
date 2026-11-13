//! A relay that refuses its upstream (INV-SYNC-54, SPEC 8 §8.10) fails the
//! live reads it relays for its foregrounds with that refusal, as a store
//! that holds the link itself fails its own: a non-durable foreground (a
//! browser tab over its SharedWorker, an app over its native relay) is
//! told why instead of waiting for an answer that never comes.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::pin::pin;
use std::task::{Context, Poll};

use crate::common;

use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, FirstLoad, ReadOpts, ReadTier,
    SerializedReadResult, SubscriptionEvent, SubscriptionStream, Upstream,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

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

fn config(node: u8) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// An in-memory server holding `count` rows labelled `g{generation}-*`.
fn server_with(generation: u8, count: u8) -> Db {
    let server = block_on(Db::open_history_complete(config(0x51))).expect("open server");
    for index in 0..count {
        let mut bytes = [0u8; 16];
        bytes[..8].copy_from_slice(&0x019e_0000_0000_7000u64.to_be_bytes());
        bytes[8] = generation;
        bytes[15] = index + 1;
        server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                RowUuid::from_bytes(bytes),
                AuthorSubject::SYSTEM,
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String(format!("g{generation}-{index}")),
                )]),
            )
            .expect("seed");
    }
    server
}

fn connect(client: &Db, server: &Db) -> Upstream {
    let (client_side, server_side) = duplex();
    let upstream = block_on(client.connect_upstream(client_side));
    server
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    upstream
}

/// Alice's relay (her SharedWorker, say) and her tab over it.
struct Device {
    relay: Db,
    tab: Db,
}

impl Device {
    fn new() -> Self {
        let relay = block_on(Db::open(config(0x61))).expect("open relay");
        let tab = block_on(Db::open(config(0x62))).expect("open tab");
        tab.set_non_durable_client();
        connect(&tab, &relay);
        Self { relay, tab }
    }

    fn pump(&self, server: &Db) {
        for _ in 0..40 {
            block_on(self.tab.tick()).unwrap();
            block_on(self.relay.tick()).unwrap();
            block_on(server.tick()).unwrap();
            block_on(self.relay.tick()).unwrap();
        }
        block_on(self.tab.tick()).unwrap();
    }

    /// The tab's one-shot read, as its bindings make it, polled between
    /// pumps: `None` while it is still waiting.
    fn fetch(&self, server: &Db, opts: ReadOpts) -> Option<Result<usize, Error>> {
        let query = postcard::to_allocvec(&Query::from("items")).unwrap();
        let mut read = pin!(
            self.tab
                .all_serialized_query(&query, opts, None, None, None)
        );
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..20 {
            if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
                return Some(result.map(|result| match result {
                    SerializedReadResult::Rows(rows) => rows.len(),
                    SerializedReadResult::Relation(snapshot) => snapshot.root_count,
                }));
            }
            self.pump(server);
        }
        None
    }

    fn subscribe(&self, opts: ReadOpts) -> SubscriptionStream {
        let prepared = self.tab.prepare_query(&Query::from("items")).unwrap();
        block_on(self.tab.subscribe(&prepared, opts)).unwrap()
    }
}

fn remote() -> ReadOpts {
    ReadOpts::for_read_tier(ReadTier::Remote)
}

fn local_first_waiting() -> ReadOpts {
    ReadOpts {
        first_load: FirstLoad::WaitForRemote { timeout_ms: 60_000 },
        ..ReadOpts::for_read_tier(ReadTier::LocalFirst)
    }
}

fn local_first() -> ReadOpts {
    ReadOpts::for_read_tier(ReadTier::LocalFirst)
}

/// What `stream` delivered since the last call: the code of its rejection,
/// if any, and the labels of the rows its deltas added.
fn drain(stream: &mut SubscriptionStream) -> (Option<ErrorCode>, usize) {
    let (mut code, mut added) = (None, 0);
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Rejected { code: c, .. } => code = Some(c),
            SubscriptionEvent::Delta { added: rows, .. } => added += rows.len(),
            _ => {}
        }
    }
    (code, added)
}

/// As [`drain`], and whether a delta said the result is settled.
fn drain_settled(stream: &mut SubscriptionStream) -> (Option<ErrorCode>, usize, bool) {
    let (mut code, mut added, mut settled) = (None, 0, false);
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Rejected { code: c, .. } => code = Some(c),
            SubscriptionEvent::Delta {
                added: rows,
                settled: flag,
                ..
            } => {
                added += rows.len();
                settled |= flag;
            }
            _ => {}
        }
    }
    (code, added, settled)
}

/// A relay synced from server₁, then linked to server₂ (a recreated store,
/// another authority namespace), which it refuses with `AuthorityChanged`.
fn refused_device() -> (Device, Db, Db) {
    let server1 = server_with(1, 3);
    let device = Device::new();
    let first = connect(&device.relay, &server1);
    let mut warm = device.subscribe(local_first());
    device.pump(&server1);
    assert_eq!(drain(&mut warm), (None, 3), "synced from server₁");
    drop(warm);
    assert!(device.relay.detach_connection(&first));
    let server2 = server_with(2, 5);
    connect(&device.relay, &server2);
    (device, server1, server2)
}

/// Every live read the tab relays through its refused relay ends with the
/// relay's `AuthorityChanged`, whatever its tier, as it would on a store
/// holding the link itself: a Remote read (which would otherwise wait for
/// an answer forever), a local-first read mid first-load wait, and a
/// local-first read already served from the relay's store. Reads opened
/// after the refusal fail with it too. A LocalOnly read never asked the
/// upstream and is untouched.
///
/// ```text
/// tab ── live reads ──► relay ── Join ──► server₂ (recreated)
///                         ◄── Welcome{namespace₂}
///                         ✗ AuthorityChanged: link refused
/// tab ◄── Refused{AuthorityChanged} per relayed read ── relay
/// ```
#[test]
fn live_reads_relayed_through_a_refused_upstream_end_with_its_refusal() {
    let (device, _server1, server2) = refused_device();
    let mut remote_read = device.subscribe(remote());
    let mut waiting = device.subscribe(local_first_waiting());
    let mut served = device.subscribe(local_first());
    let mut local = device.subscribe(ReadOpts::for_read_tier(ReadTier::LocalOnly));
    device.pump(&server2);

    let refusal = ErrorCode::AuthorityChanged;
    assert_eq!(drain(&mut remote_read).0, Some(refusal), "Remote");
    assert_eq!(drain(&mut waiting).0, Some(refusal), "LocalFirst, waiting");
    assert_eq!(drain(&mut served).0, Some(refusal), "LocalFirst");
    assert_eq!(drain(&mut local), (None, 3), "LocalOnly reads the store");

    // Opened after the refusal: told at once.
    let mut late_remote = device.subscribe(remote());
    let mut late_waiting = device.subscribe(local_first_waiting());
    let mut late = device.subscribe(local_first());
    device.pump(&server2);
    assert_eq!(drain(&mut late_remote).0, Some(refusal), "late Remote");
    assert_eq!(drain(&mut late_waiting).0, Some(refusal), "late waiting");
    assert_eq!(drain(&mut late).0, Some(refusal), "late LocalFirst");

    // One-shot reads: a Remote one fails with the same code; a local-first
    // one reads the relay's store, as it would on a store holding the link.
    assert_eq!(
        device
            .fetch(&server2, remote())
            .expect("the one-shot Remote read ends")
            .unwrap_err()
            .code,
        refusal
    );
    assert_eq!(
        device
            .fetch(&server2, local_first_waiting())
            .unwrap()
            .unwrap(),
        3
    );
    assert_eq!(device.fetch(&server2, local_first()).unwrap().unwrap(), 3);
}

/// A refusal ends a relayed read once. Relinked to the authority it
/// follows, the relay answers its reads again: those it refused are settled
/// again, and a read opened then (one sharing a refused read's upstream
/// read included) is served and settled, not refused with the stale
/// refusal.
///
/// ```text
/// relay ── server₂ ── AuthorityChanged ── tab's reads rejected
/// relay ── server₁ again ── tab's reads settle; nothing rejected again
/// ```
#[test]
fn a_relay_relinked_to_its_authority_serves_refused_reads_again() {
    let (device, server1, server2) = refused_device();
    let mut reads =
        [remote(), local_first_waiting(), local_first()].map(|opts| device.subscribe(opts));
    device.pump(&server2);
    for read in &mut reads {
        assert_eq!(drain(read).0, Some(ErrorCode::AuthorityChanged));
    }

    connect(&device.relay, &server1);
    device.pump(&server1);
    for (read, tier) in reads.iter_mut().zip(["Remote", "waiting", "LocalFirst"]) {
        let (rejected, _, settled) = drain_settled(read);
        assert_eq!(rejected, None, "{tier}: rejected once");
        assert!(settled, "{tier}: settled again");
    }
    for (opts, tier) in [remote(), local_first_waiting(), local_first()]
        .into_iter()
        .zip(["Remote", "waiting", "LocalFirst"])
    {
        let mut fresh = device.subscribe(opts);
        device.pump(&server1);
        assert_eq!(
            drain_settled(&mut fresh),
            (None, 3, true),
            "a new {tier} read"
        );
    }
}

/// The parity the relay keeps: a store holding the refused link itself
/// fails its own Remote one-shot read with the refusal, instead of waiting
/// for an answer no link will give until its coverage budget runs out.
///
/// ```text
/// alice ── server₁ ── settles ── ╳
/// alice ── server₂ (recreated) ── AuthorityChanged
/// alice: one-shot Remote read ✗ AuthorityChanged
/// ```
#[test]
fn a_remote_one_shot_read_on_a_refused_link_fails_with_its_refusal() {
    let server1 = server_with(1, 3);
    let alice = block_on(Db::open(config(0x63))).expect("open alice");
    let first = connect(&alice, &server1);
    let query = postcard::to_allocvec(&Query::from("items")).unwrap();
    let pump = |server: &Db| {
        for _ in 0..40 {
            block_on(alice.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
    };
    let read = |server: &Db| {
        let mut read = pin!(alice.all_serialized_query(&query, remote(), None, None, None));
        let waker = futures::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        for _ in 0..20 {
            if let Poll::Ready(result) = read.as_mut().poll(&mut cx) {
                return Some(result.map(|_| ()));
            }
            pump(server);
        }
        None
    };
    assert!(matches!(read(&server1), Some(Ok(()))), "a healthy link");
    assert!(alice.detach_connection(&first));
    let server2 = server_with(2, 5);
    connect(&alice, &server2);
    pump(&server2);
    assert_eq!(
        read(&server2).expect("the read ends").unwrap_err().code,
        ErrorCode::AuthorityChanged
    );
    // A new link to the authority it follows answers again.
    connect(&alice, &server1);
    assert!(matches!(read(&server1), Some(Ok(()))), "relinked");
}
