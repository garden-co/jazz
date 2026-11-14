//! A relay that refuses its upstream (INV-SYNC-51, INV-SYNC-54, SPEC 8
//! §8.10) refuses the Remote live reads it relays for its foregrounds with
//! that refusal and its code, as a store holding the link itself fails its
//! own remote reads: a non-durable foreground (a browser tab over its
//! SharedWorker, an app over its native relay) is told why instead of
//! waiting for an answer no link will give. Its local-first reads read the
//! relay's store, the foreground's local view, as when there is no server.

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
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn table_with(table: &str, column: &str) -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new(table)
                .column(column, ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .build()
}

fn items() -> JazzSchema {
    compile_schema(&table_with("items", "label"))
}

fn config(schema: JazzSchema, node: u8) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn row(generation: u8, index: u8) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000u64.to_be_bytes());
    bytes[8] = generation;
    bytes[15] = index + 1;
    RowUuid::from_bytes(bytes)
}

/// Seed rows `range` of `generation` into `server`'s `items`.
fn seed(server: &Db, generation: u8, range: std::ops::Range<u8>) {
    for index in range {
        server
            .seed_settled_mergeable_for_bootstrap(
                "items",
                row(generation, index),
                AuthorSubject::SYSTEM,
                BTreeMap::from([(
                    "label".to_owned(),
                    Value::String(format!("g{generation}-{index}")),
                )]),
            )
            .expect("seed");
    }
}

/// An in-memory authority holding `count` rows labelled `g{generation}-*`.
fn server_with(generation: u8, count: u8) -> Db {
    let server = block_on(Db::open_history_complete(config(items(), 0x51))).expect("open server");
    seed(&server, generation, 0..count);
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
    table: &'static str,
}

impl Device {
    fn new(schema: JazzSchema, table: &'static str) -> Self {
        Self::over(
            block_on(Db::open(config(schema.clone(), 0x61))).expect("open relay"),
            schema,
            table,
        )
    }

    fn over(relay: Db, schema: JazzSchema, table: &'static str) -> Self {
        let tab = block_on(Db::open(config(schema, 0x62))).expect("open tab");
        tab.set_non_durable_client();
        connect(&tab, &relay);
        Self { relay, tab, table }
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
        let query = postcard::to_allocvec(&Query::from(self.table)).unwrap();
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
        let prepared = self.tab.prepare_query(&Query::from(self.table)).unwrap();
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

/// One subscription event, as these tests compare them: in order, each one
/// on its own (a settled flag is never merged into another event's).
#[derive(Debug, PartialEq, Eq)]
enum Seen {
    Rejected(ErrorCode),
    Delta {
        added: usize,
        removed: usize,
        settled: bool,
        global: bool,
    },
}

fn seen(stream: &mut SubscriptionStream) -> Vec<Seen> {
    std::iter::from_fn(|| stream.try_next_event())
        .filter_map(|event| match event {
            SubscriptionEvent::Rejected { code, .. } => Some(Seen::Rejected(code)),
            SubscriptionEvent::Delta {
                added,
                removed,
                settled,
                tier,
                ..
            } => Some(Seen::Delta {
                added: added.len(),
                removed: removed.len(),
                settled,
                global: tier == DurabilityTier::Global,
            }),
            _ => None,
        })
        .collect()
}

fn rejections(events: &[Seen]) -> Vec<ErrorCode> {
    events
        .iter()
        .filter_map(|event| match event {
            Seen::Rejected(code) => Some(*code),
            Seen::Delta { .. } => None,
        })
        .collect()
}

/// A refused Remote read's events: the refusal first, once, and nothing
/// settled after it (what the tab's own store holds may follow, unsettled,
/// as on any rejected subscription).
fn assert_refused(events: &[Seen], code: ErrorCode, what: &str) {
    assert_eq!(
        events.first(),
        Some(&Seen::Rejected(code)),
        "{what}: {events:?}"
    );
    assert_eq!(rejections(events), [code], "{what}: refused once");
    assert!(
        events
            .iter()
            .all(|e| !matches!(e, Seen::Delta { settled: true, .. })),
        "{what}: nothing settled: {events:?}"
    );
}

fn rows(events: &[Seen]) -> isize {
    events
        .iter()
        .map(|event| match event {
            Seen::Delta { added, removed, .. } => *added as isize - *removed as isize,
            Seen::Rejected(_) => 0,
        })
        .sum()
}

/// A relay synced from server₁ (three rows), then linked to server₂ (a
/// recreated store, another authority namespace), which it refuses with
/// `AuthorityChanged`.
fn authority_changed_device() -> (Device, Db, Db, Upstream) {
    let server1 = server_with(1, 3);
    let device = Device::new(items(), "items");
    let first = connect(&device.relay, &server1);
    let mut warm = device.subscribe(local_first());
    device.pump(&server1);
    assert_eq!(rows(&seen(&mut warm)), 3, "synced from server₁");
    drop(warm);
    assert!(device.relay.detach_connection(&first));
    let server2 = server_with(2, 5);
    let second = connect(&device.relay, &server2);
    (device, server1, server2, second)
}

/// What the tab's reads get through a relay that refused `upstream`: its
/// Remote reads, live and one-shot, opened before the refusal reached the
/// relay and after it, are refused with the relay's own refusal, once
/// each. Its local-first reads (with a first-load wait or without) read
/// the relay's store (its `held` rows, when given), never as the
/// authority's global result, and are not refused. Returns that refusal.
fn tab_reads_through_a_refused_relay(
    device: &Device,
    server: &Db,
    upstream: &Upstream,
    held: Option<isize>,
) -> Error {
    let mut remote_read = device.subscribe(remote());
    let mut waiting = device.subscribe(local_first_waiting());
    let mut served = device.subscribe(local_first());
    device.pump(server);
    let refusal = device
        .relay
        .upstream_refusal(upstream)
        .expect("the relay refused its upstream");

    assert_refused(&seen(&mut remote_read), refusal.code, "Remote");
    for (stream, tier) in [(&mut waiting, "waiting"), (&mut served, "LocalFirst")] {
        let events = seen(stream);
        assert_eq!(rejections(&events), [], "{tier}: not refused");
        if let Some(held) = held {
            assert_eq!(rows(&events), held, "{tier}: reads the relay's store");
        }
        assert!(
            events
                .iter()
                .all(|e| !matches!(e, Seen::Delta { global: true, .. })),
            "{tier}: never the authority's result: {events:?}"
        );
    }

    // Opened after the refusal: told at once, and once.
    let mut late_remote = device.subscribe(remote());
    let mut late = device.subscribe(local_first_waiting());
    device.pump(server);
    assert_refused(&seen(&mut late_remote), refusal.code, "late Remote");
    let events = seen(&mut late);
    assert_eq!(rejections(&events), [], "late LocalFirst");
    if let Some(held) = held {
        assert_eq!(rows(&events), held, "late LocalFirst");
    }
    device.pump(server);
    for stream in [&mut remote_read, &mut late_remote] {
        let events = seen(stream);
        assert_eq!(rejections(&events), [], "refused once");
        assert!(
            events
                .iter()
                .all(|e| !matches!(e, Seen::Delta { settled: true, .. })),
            "never settled while refused: {events:?}"
        );
    }

    // One-shot reads, as the control: a Remote one fails with the same
    // code; a local-first one reads the relay's store.
    assert_eq!(
        device
            .fetch(server, remote())
            .expect("the one-shot Remote read ends")
            .unwrap_err()
            .code,
        refusal.code
    );
    for opts in [local_first_waiting(), local_first()] {
        let fetched = device
            .fetch(server, opts)
            .expect("answered")
            .expect("not refused");
        if let Some(held) = held {
            assert_eq!(fetched as isize, held);
        }
    }
    refusal
}

/// A relay refuses a recreated server with `AuthorityChanged`: the tab's
/// Remote reads end with it, and its local-first reads keep the rows the
/// relay holds from the old history. A LocalOnly read never asked the
/// upstream and is untouched.
///
/// ```text
/// tab ── live reads ──► relay ── Join ──► server₂ (recreated)
///                         ◄── Welcome{namespace₂}
///                         ✗ AuthorityChanged: link refused
/// tab ◄── Refused{AuthorityChanged} per Remote read ── relay
/// tab ◄── the relay's rows, per local-first read ───── relay
/// ```
#[test]
fn a_relay_refusing_a_recreated_server_refuses_the_tabs_remote_reads() {
    let (device, _server1, server2, second) = authority_changed_device();
    let mut local = device.subscribe(ReadOpts::for_read_tier(ReadTier::LocalOnly));
    let refusal = tab_reads_through_a_refused_relay(&device, &server2, &second, Some(3));
    assert_eq!(refusal.code, ErrorCode::AuthorityChanged);
    let events = seen(&mut local);
    // The bindings hand JS the rejection with its code, which jazz-tools
    // reads as an AuthorityChangedError.
    let mut late = device.subscribe(remote());
    device.pump(&server2);
    let rejected = late.try_next_event().expect("refused");
    match jazz::binding_host::event_chunk(rejected).unwrap() {
        jazz::binding_host::EventChunk::Rejected { code, detail } => {
            assert_eq!(code, "authority_changed");
            assert!(detail.contains("deleteClientStorage()"), "{detail}");
        }
        _ => panic!("expected a rejection chunk"),
    }
    assert_eq!(
        (rejections(&events), rows(&events)),
        (vec![], 3),
        "LocalOnly"
    );
}

/// A relay that committed writes before it became a replica refuses every
/// upstream (INV-SYNC-51): the tab's Remote reads end with that refusal,
/// and its local-first reads keep reading the relay's store.
///
/// ```text
/// relay writes r0 offline ── Join ──► server   ✗ stranded: link refused
/// tab ◄── Refused{Protocol} per Remote read; local-first reads served
/// ```
#[test]
fn a_stranded_relay_refuses_the_tabs_remote_reads() {
    let server = server_with(1, 3);
    let relay = block_on(Db::open(config(items(), 0x61))).expect("open relay");
    block_on(relay.insert(
        "items",
        BTreeMap::from([("label".to_owned(), Value::String("r0".into()))]),
        Default::default(),
    ))
    .unwrap();
    let device = Device::over(relay, items(), "items");
    let upstream = connect(&device.relay, &server);
    // Its own unsynced write is not what this test is about: how a relay
    // serves it to sessions is the same with any upstream or none.
    let refusal = tab_reads_through_a_refused_relay(&device, &server, &upstream, None);
    assert_eq!(refusal.code, ErrorCode::Protocol);
    assert!(
        refusal.message.contains("before it became a replica"),
        "{refusal}"
    );
}

/// The `jazz.app.json` of an app that started at `todos(label)` and
/// renamed `label` to `title`, as `jazz-tools validate` writes it.
fn renamed_label_bundle() -> serde_json::Value {
    let (v1, v2) = (table_with("todos", "label"), table_with("todos", "title"));
    let hash = |schema: &jazz::tools::Schema| jazz::tools::SchemaHash::compute(schema).to_string();
    let input = serde_json::json!({
        "current": hash(&v2),
        "schemas": [v1, v2],
        "migrations": [{
            "fromHash": hash(&v1),
            "toHash": hash(&v2),
            "forward": [{
                "table": "todos",
                "operations": [{"type": "rename", "column": "label", "value": "title"}],
            }],
        }],
    });
    let text = jazz::tools::deployment::artifact::write(serde_json::from_value(input).unwrap())
        .unwrap()
        .app;
    serde_json::from_str(&text).unwrap()
}

/// A relay opened from the app's head schema alone, without the migrations
/// its server's history needs, stores the head's column under another name
/// than the server does, so it cannot mirror the server's catalogue and
/// refuses it (the relay half of #4777): the tab's Remote reads end with
/// that refusal, not `AuthorityChanged`.
///
/// ```text
/// server (v1 + lens → v2) ──catalogue──► relay (v2 alone)
///                                          ✗ cannot mirror: link refused
/// tab ◄── Refused{Query} per Remote read ── relay
/// ```
#[test]
fn a_relay_that_cannot_mirror_its_servers_catalogue_refuses_the_tabs_remote_reads() {
    let jazz::db::Migrations { schemas, lenses } = jazz::tools::deployment::artifact::load_client(
        renamed_label_bundle().to_string().as_bytes(),
    )
    .unwrap();
    let head = compile_schema(&table_with("todos", "title"));
    let server = block_on(Db::open_history_complete(
        config(head.clone(), 0x51).with_migrations(schemas, lenses),
    ))
    .unwrap();
    server.set_authority();
    let device = Device::new(head, "todos");
    let upstream = connect(&device.relay, &server);
    let refusal = tab_reads_through_a_refused_relay(&device, &server, &upstream, Some(0));
    assert_ne!(refusal.code, ErrorCode::AuthorityChanged);
    assert!(
        refusal.message.contains("catalogue") || refusal.message.contains("stor"),
        "{refusal}"
    );
}

/// A refusal ends a relayed Remote read once. Relinked to the authority it
/// follows (which took two rows meanwhile), the relay answers again: its
/// local-first reads follow the authority's rows without ever having been
/// refused, a Remote read opened while the relay relinks (before the new
/// link's `Welcome`) is not refused with the last link's refusal, and a
/// Remote read opened then is answered and settled with every row, never
/// settled short of them.
///
/// ```text
/// relay ── server₂ ── AuthorityChanged ── tab's Remote read refused
/// server₁ takes g1-3, g1-4
/// relay ── server₁ again ── tab's reads follow server₁; nothing refused again
/// ```
#[test]
fn a_relay_relinked_to_its_authority_answers_the_tabs_reads_again() {
    let (device, server1, server2, _second) = authority_changed_device();
    let mut refused = device.subscribe(remote());
    let mut local = device.subscribe(local_first());
    device.pump(&server2);
    assert_refused(&seen(&mut refused), ErrorCode::AuthorityChanged, "Remote");
    assert_eq!(rows(&seen(&mut local)), 3);
    seed(&server1, 1, 3..5);

    connect(&device.relay, &server1);
    // Opened before the relay heard server₁'s `Welcome`.
    // Another query than the refused one's: its own upstream read.
    let ordered = device
        .tab
        .prepare_query(&Query::from("items").order_by("label", OrderDirection::Asc))
        .unwrap();
    let mut relinking = block_on(device.tab.subscribe(&ordered, remote())).unwrap();
    // The relay serves the tab before server₁ answered anything.
    block_on(device.tab.tick()).unwrap();
    for _ in 0..3 {
        block_on(device.relay.tick()).unwrap();
        block_on(device.tab.tick()).unwrap();
    }
    let early = seen(&mut relinking);
    assert_eq!(
        rejections(&early),
        [],
        "not refused with the last link's refusal"
    );
    device.pump(&server1);
    // The refused read is answered again: it settles with every row of
    // server₁, and is not refused again.
    let events = seen(&mut refused);
    assert_eq!(rejections(&events), [], "refused once");
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Seen::Delta { settled: true, .. })),
        "answered again: {events:?}"
    );
    let mut answered = device.subscribe(remote());
    device.pump(&server1);
    assert_eq!(rows(&seen(&mut answered)), 5, "server₁'s rows");
    let events = seen(&mut local);
    assert_eq!(
        (rejections(&events), rows(&events)),
        (vec![], 2),
        "LocalFirst follows server₁"
    );
    let events = seen(&mut relinking);
    assert_eq!(rejections(&events), [], "nor once answered");
    assert_eq!(rows(&early) + rows(&events), 5);

    let mut fresh = device.subscribe(remote());
    device.pump(&server1);
    let events = seen(&mut fresh);
    assert_eq!(rejections(&events), []);
    // Every settled delta lists every row: none settles short of them.
    let mut held = 0;
    for event in &events {
        if let Seen::Delta {
            added,
            removed,
            settled,
            ..
        } = event
        {
            held += *added as isize - *removed as isize;
            assert!(
                !settled || held == 5,
                "settled with {held} of 5 rows: {events:?}"
            );
        }
    }
    assert_eq!(held, 5);
    assert!(
        events
            .iter()
            .any(|e| matches!(e, Seen::Delta { settled: true, .. }))
    );
}

/// A store's own subscription opened while it relinks takes the last
/// link's refusal until the new link's catalogue is mirrored. Polled only
/// after that, it is not told a refusal that no longer holds.
///
/// ```text
/// relay ── server₂ ── AuthorityChanged
/// relay ── server₁ again; relay subscribes (attached refused)
///   ◄── Welcome (mirrored) ── answer ── relay polls: rows, no rejection
/// ```
#[test]
fn a_subscription_opened_while_relinking_is_not_told_a_refusal_that_no_longer_holds() {
    let (device, server1, server2, _second) = authority_changed_device();
    device.pump(&server2);
    connect(&device.relay, &server1);
    let prepared = device.relay.prepare_query(&Query::from("items")).unwrap();
    let mut stream = block_on(device.relay.subscribe(&prepared, remote())).unwrap();
    device.pump(&server1);
    let events = seen(&mut stream);
    assert_eq!(rejections(&events), [], "{events:?}");
    assert_eq!(rows(&events), 3);
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
    let alice = block_on(Db::open(config(items(), 0x63))).expect("open alice");
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
