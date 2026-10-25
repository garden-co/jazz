//! Foregrounds distinguish a storage-owner response from authority settlement.
//! Every actor uses the public SQLite Db and the normal sync transport.
mod common;
#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use jazz::db::{
    Db, DbConfig, DbIdentity, FirstLoad, ReadOpts, ReadTier, RemoteLinkHint, SerializedReadResult,
    SubscriptionEvent, SubscriptionStream, TickScheduler, TickUrgency, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use std::cell::RefCell;
use std::collections::BTreeMap;
use std::future::Future;
use std::pin::{Pin, pin};
use std::rc::Rc;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, Instant};

fn open(node: u8, authority: bool) -> Db {
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .policies(common::allow_all_policies()),
            )
            .build(),
    );
    let db = block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap();
    if authority {
        db.set_authority();
    }
    db
}
fn insert(db: &Db, text: &str) {
    block_on(db.insert(
        "notes",
        BTreeMap::from([("text".into(), Value::String(text.into()))]),
        Default::default(),
    ))
    .unwrap();
}
fn connect(client: &Db, owner: &Db) {
    let (a, b) = duplex_transport::duplex();
    block_on(client.connect_upstream(a));
    owner.accept_subscriber(b, AuthorSubject::SYSTEM);
}
fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}
fn opts(ms: u64) -> ReadOpts {
    ReadOpts {
        first_load: FirstLoad::WaitForRemote { timeout_ms: ms },
        ..Default::default()
    }
}
fn subscribe(db: &Db, opts: ReadOpts) -> SubscriptionStream {
    let query = db.prepare_query(&Query::from("notes")).unwrap();
    block_on(db.subscribe(&query, opts)).unwrap()
}
fn opening(stream: &mut SubscriptionStream, dbs: &[&Db]) -> (Vec<String>, DurabilityTier) {
    for _ in 0..100 {
        if let Some(SubscriptionEvent::Delta {
            reset,
            added,
            settled,
            tier,
            ..
        }) = stream.try_next_event()
        {
            assert!(reset && settled, "the opening must be an answered snapshot");
            let mut rows: Vec<String> = added
                .iter()
                .map(
                    |row| match row.cells().find(|(n, _)| *n == "text").unwrap().1.unwrap() {
                        Value::String(text) => text.clone(),
                        value => panic!("{value:?}"),
                    },
                )
                .collect();
            rows.sort();
            return (rows, tier);
        }
        turn(dbs);
    }
    panic!("no opening");
}
fn one_shot<'a>(
    db: &'a Db,
    query: &'a [u8],
    opts: ReadOpts,
) -> impl Future<Output = Result<SerializedReadResult, jazz::db::Error>> + 'a {
    db.all_serialized_query(query, opts, None, None, None, false, || false, |_| {})
}
fn poll<T>(future: Pin<&mut impl Future<Output = T>>) -> Poll<T> {
    future.poll(&mut Context::from_waker(Waker::noop()))
}
fn warm(owner: &Db, server: &Db) {
    let mut stream = subscribe(owner, ReadOpts::for_read_tier(ReadTier::Remote));
    let (_, tier) = opening(&mut stream, &[owner, server, owner]);
    assert_eq!(tier, DurabilityTier::Global);
    drop(stream);
    turn(&[owner, server, owner]);
}

/// Alice's two streams share one upstream query on Bob's authority. Cancelling
/// one preserves the other; cancelling both releases the authority subscription.
/// The internal inventory is needed to distinguish a shared remote reference
/// from two independent queries that happen to return the same public rows.
#[test]
fn remote_attachment_inventory_tracks_shared_streams_and_cancellation() {
    let authority = open(0xf1, true);
    insert(&authority, "initial");
    let alice = open(0xf2, false);
    connect(&alice, &authority);
    let mut first = subscribe(&alice, ReadOpts::for_read_tier(ReadTier::Remote));
    let mut second = subscribe(&alice, ReadOpts::for_read_tier(ReadTier::Remote));
    assert_eq!(alice.query_coverage_attachment_counts_for_test(), (1, 2));
    assert_eq!(
        opening(&mut first, &[&alice, &authority, &alice]).0,
        vec!["initial"]
    );
    assert_eq!(
        opening(&mut second, &[&alice, &authority, &alice]).0,
        vec!["initial"]
    );
    assert_eq!(authority.runtime_stats_for_test().active_subscriptions, 1);
    drop(first);
    turn(&[&alice, &authority, &alice]);
    assert_eq!(alice.query_coverage_attachment_counts_for_test(), (1, 1));
    assert_eq!(authority.runtime_stats_for_test().active_subscriptions, 1);
    insert(&authority, "after cancellation");
    turn(&[&authority, &alice]);
    assert!(
        matches!(second.try_next_event(), Some(SubscriptionEvent::Delta { added, .. }) if added.len() == 1)
    );
    drop(second);
    turn(&[&alice, &authority, &alice]);
    assert_eq!(alice.query_coverage_attachment_counts_for_test(), (0, 0));
    assert_eq!(authority.runtime_stats_for_test().active_subscriptions, 0);
}

/// Alice's fresh tab cannot mistake its own empty memory for the owner's cache,
/// even before the worker connects or after the authority-wait deadline expires.
#[test]
fn foreground_waits_for_owner_before_any_local_fallback() {
    let owner = open(2, false);
    insert(&owner, "cached");
    let tab = open(1, false);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let mut stream = subscribe(&tab, opts(10));
    assert!(stream.try_next_event().is_none());
    std::thread::sleep(Duration::from_millis(20));
    assert!(stream.try_next_event().is_none());
    connect(&tab, &owner);
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &tab]),
        (vec!["cached".into()], DurabilityTier::Local)
    );
}

/// Alice's cold worker reports local completion first. A requested authority wait
/// holds both nonempty and empty openings until the authority has answered.
#[test]
fn live_foreground_waits_for_authority_including_empty_answers() {
    for empty in [false, true] {
        let server = open(3, true);
        if !empty {
            insert(&server, "accepted");
        }
        let owner = open(2, false);
        connect(&owner, &server);
        let tab = open(1, false);
        tab.set_non_durable_client();
        connect(&tab, &owner);
        let mut stream = subscribe(&tab, opts(60_000));
        for _ in 0..5 {
            turn(&[&tab, &owner, &tab]);
            assert!(stream.try_next_event().is_none());
        }
        let expected = if empty {
            vec![]
        } else {
            vec!["accepted".into()]
        };
        assert_eq!(
            opening(&mut stream, &[&tab, &owner, &server, &owner, &tab]),
            (expected, DurabilityTier::Global)
        );
        assert_eq!(
            block_on(tab.capture_snapshot()).unwrap().global_base.0,
            server.seq_high_water()
        );
    }
}

/// Alice opts into an immediate local-first read: her tab may open from the
/// owner's warm cache while the authority is silent, labelled Local only.
#[test]
fn immediate_foreground_reads_use_the_owner_cache() {
    let server = open(3, true);
    insert(&server, "cached");
    let owner = open(2, false);
    connect(&owner, &server);
    warm(&owner, &server);
    let tab = open(1, false);
    tab.set_non_durable_client();
    connect(&tab, &owner);
    let mut stream = subscribe(&tab, ReadOpts::default());
    assert!(stream.try_next_event().is_none());
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &tab]),
        (vec!["cached".into()], DurabilityTier::Local)
    );
}

/// Alice's warm worker is a fallback, not authority evidence. The same cached
/// answer is released on timeout or a host-reported loss of the server link.
#[test]
fn foreground_fallback_keeps_owner_rows_and_local_tier() {
    for timeout in [false, true] {
        let server = open(3, true);
        insert(&server, "cached");
        let owner = open(2, false);
        connect(&owner, &server);
        warm(&owner, &server);
        let tab = open(1, false);
        tab.set_non_durable_client();
        connect(&tab, &owner);
        let mut stream = subscribe(&tab, opts(if timeout { 80 } else { 60_000 }));
        for _ in 0..3 {
            turn(&[&tab, &owner, &tab]);
            assert!(stream.try_next_event().is_none());
        }
        if timeout {
            std::thread::sleep(Duration::from_millis(100));
        } else {
            tab.set_remote_link_hint(RemoteLinkHint::Failed);
        }
        assert_eq!(
            opening(&mut stream, &[&tab, &owner, &tab]),
            (vec!["cached".into()], DurabilityTier::Local)
        );
    }
}

/// A strict remote opening cannot use the worker's local completion, timeout or
/// Failed hint as an authority answer. It opens when the server really responds.
#[test]
fn strict_remote_waits_never_promote_a_local_relay_answer() {
    let server = open(3, true);
    insert(&server, "accepted");
    let owner = open(2, false);
    connect(&owner, &server);
    let tab = open(1, false);
    tab.set_non_durable_client();
    connect(&tab, &owner);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let mut stream = subscribe(
        &tab,
        ReadOpts {
            tier: ReadTier::Remote,
            ..opts(1)
        },
    );
    for _ in 0..5 {
        turn(&[&tab, &owner, &tab]);
        assert!(stream.try_next_event().is_none());
    }
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &server, &owner, &tab]),
        (vec!["accepted".into()], DurabilityTier::Global)
    );
}

/// Alice's one-shot uses the same owner/authority gate as subscriptions. An
/// offline owner returns its cached row rather than the foreground's empty store.
#[test]
fn foreground_one_shot_hydrates_from_its_offline_owner() {
    let owner = open(2, false);
    insert(&owner, "cached");
    let tab = open(1, false);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = pin!(one_shot(&tab, &query, ReadOpts::default()));
    assert!(poll(read.as_mut()).is_pending());
    connect(&tab, &owner);
    for _ in 0..100 {
        turn(&[&tab, &owner, &tab]);
        if let Poll::Ready(result) = poll(read.as_mut()) {
            let SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("root rows")
            };
            assert_eq!(rows.len(), 1);
            return;
        }
    }
    panic!("one-shot never answered");
}

#[derive(Default)]
struct Earliest(RefCell<Option<Instant>>);
impl TickScheduler for Earliest {
    fn schedule_tick(&self, _: TickUrgency) {}
    fn schedule_tick_after(&self, delay_ms: u64) {
        let at = Instant::now() + Duration::from_millis(delay_ms);
        let mut slot = self.0.borrow_mut();
        *slot = Some(slot.map_or(at, |old| old.min(at)));
    }
}

/// The host keeps only its earliest timer. Cancelling Alice's first read must
/// not strand Bob's later deadline when that first timer fires on a quiet link.
#[test]
fn cancelled_earlier_timer_rearms_remaining_read_deadlines() {
    let server = open(3, true);
    let tab = open(1, false);
    connect(&tab, &server);
    let scheduler = Rc::new(Earliest::default());
    tab.set_tick_scheduler(Some(scheduler.clone()));
    let first = subscribe(&tab, opts(40));
    let mut second = subscribe(&tab, opts(180));
    let first_at = scheduler
        .0
        .borrow_mut()
        .take()
        .expect("a timer was requested");
    drop(first);
    std::thread::sleep(
        first_at.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
    );
    block_on(tab.tick()).unwrap();
    let second_at = scheduler
        .0
        .borrow_mut()
        .take()
        .expect("next timer rearmed after first cancellation");
    assert!(second_at > Instant::now());
    assert!(second.try_next_event().is_none());
    std::thread::sleep(
        second_at.saturating_duration_since(Instant::now()) + Duration::from_millis(5),
    );
    block_on(tab.tick()).unwrap();
    assert!(matches!(
        second.try_next_event(),
        Some(SubscriptionEvent::Delta {
            reset: true,
            settled: false,
            ..
        })
    ));
}

/// Alice may open on a local cache and then receive a pure settlement upgrade
/// when the empty authority answers. No row delta is needed to report Global.
#[test]
fn local_opening_can_upgrade_to_global_without_row_changes() {
    let server = open(3, true);
    let owner = open(2, false);
    connect(&owner, &server);
    let tab = open(1, false);
    tab.set_non_durable_client();
    connect(&tab, &owner);
    let mut stream = subscribe(&tab, ReadOpts::default());
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &tab]),
        (vec![], DurabilityTier::Local)
    );
    for _ in 0..100 {
        turn(&[&tab, &owner, &server, &owner, &tab]);
        if let Some(SubscriptionEvent::Delta {
            settled: true,
            tier: DurabilityTier::Global,
            ..
        }) = stream.try_next_event()
        {
            return;
        }
    }
    panic!("empty authority answer did not upgrade settlement");
}

/// Alice's owner holds both users' rows, but its ordinary SELECT policy sends
/// only Alice's row to her fresh foreground. A delegated Bob read is refused.
#[test]
fn foreground_cache_hydration_preserves_the_owners_session_policy() {
    use jazz::tools::{PolicyExpr, TablePolicies};
    let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let schema = common::compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("text", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::eq_session(
                        "owner",
                        vec!["user".into(), "identity".into(), "subject".into()],
                    ))),
            )
            .build(),
    );
    let make = |node, author| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let owner = make(2, AuthorSubject::SYSTEM);
    for name in ["alice", "bob"] {
        block_on(owner.insert(
            "notes",
            BTreeMap::from([
                ("text".into(), Value::String(name.into())),
                ("owner".into(), Value::String(name.into())),
            ]),
            Default::default(),
        ))
        .unwrap();
    }
    let tab = make(1, alice);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let (a, b) = duplex_transport::duplex();
    block_on(tab.connect_upstream(a));
    owner.accept_subscriber(b, alice);
    let mut stream = subscribe(&tab, ReadOpts::default());
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &tab]),
        (vec!["alice".into()], DurabilityTier::Local)
    );
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let error = block_on(tab.all_serialized_query(
        &query,
        ReadOpts::default(),
        None,
        None,
        Some(AuthorSubject::authenticated("https://issuer.example", "bob").unwrap()),
        false,
        || false,
        |_| {},
    ))
    .unwrap_err();
    assert!(error.message.contains("request-scoped foreground reads"));
}

/// A cancelled one-shot releases its normal upstream subscription. Observing
/// protocol frames is intentional: cleanup has no remaining row result to query.
#[test]
fn cancelled_foreground_one_shot_releases_its_upstream_attachment() {
    struct Capture {
        messages: Rc<RefCell<Vec<jazz::db::SyncMessage>>>,
    }
    impl jazz::db::Transport for Capture {
        fn send(&mut self, m: jazz::db::SyncMessage) -> Result<(), jazz::db::TransportError> {
            self.messages.borrow_mut().push(m);
            Ok(())
        }
        fn try_recv(&mut self) -> Option<jazz::db::SyncMessage> {
            None
        }
    }
    let messages = Rc::new(RefCell::new(vec![]));
    let tab = open(1, false);
    tab.set_non_durable_client();
    block_on(tab.connect_upstream(Box::new(Capture {
        messages: messages.clone(),
    })));
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = Box::pin(one_shot(&tab, &query, opts(60_000)));
    assert!(poll(read.as_mut()).is_pending());
    block_on(tab.tick()).unwrap();
    let id = messages
        .borrow()
        .iter()
        .find_map(|m| match m {
            jazz::db::SyncMessage::Subscribe { id, .. } => Some(*id),
            _ => None,
        })
        .expect("ordinary subscription sent");
    drop(read);
    block_on(tab.tick()).unwrap();
    assert!(
        messages
            .borrow()
            .iter()
            .any(|m| matches!(m,jazz::db::SyncMessage::Unsubscribe{id:closed} if *closed==id))
    );
}

/// A foreground's one-shot with an authority wait does not return its warm
/// owner's stale cache. It returns the updated server rows through normal sync.
#[test]
fn foreground_one_shot_waits_for_authority_after_owner_reply() {
    let server = open(3, true);
    insert(&server, "cached");
    let owner = open(2, false);
    connect(&owner, &server);
    warm(&owner, &server);
    insert(&server, "new");
    let tab = open(1, false);
    tab.set_non_durable_client();
    connect(&tab, &owner);
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = pin!(one_shot(&tab, &query, opts(60_000)));
    assert!(poll(read.as_mut()).is_pending());
    for _ in 0..4 {
        turn(&[&tab, &owner, &tab]);
        assert!(poll(read.as_mut()).is_pending());
    }
    for _ in 0..100 {
        turn(&[&tab, &owner, &server, &owner, &tab]);
        if let Poll::Ready(result) = poll(read.as_mut()) {
            let SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("root rows")
            };
            assert_eq!(rows.len(), 2);
            return;
        }
    }
    panic!("authority result not delivered");
}

/// This byte-level fixture is intentional: owner-local and authority responses
/// reuse the existing Settled tag and fields without silently changing the ABI.
#[test]
fn read_settlement_v1_keeps_existing_local_and_global_bytes() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/sqlite_read_settlement_v1.json")).unwrap();
    assert_eq!(fixture["role"], "jazz.sqlite.sync.read-settlement.v1");
    assert_eq!(fixture["version"], 1);
    for (name, global) in [("local", false), ("global", true)] {
        let message = jazz::db::SyncMessage::Settled {
            id: 7,
            through: 9,
            window: None,
            global,
        };
        let bytes = postcard::to_allocvec(&message).unwrap();
        let hex: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
        assert_eq!(hex, fixture[name].as_str().unwrap());
        let decoded: jazz::db::SyncMessage = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(postcard::to_allocvec(&decoded).unwrap(), bytes);
    }
}

/// Alice's tab waits for the chunks behind the owner's row too; receiving the
/// local settlement marker alone must not publish an empty or incomplete value.
#[test]
fn foreground_owner_opening_waits_for_large_value_chunks() {
    let owner = open(2, false);
    let text = "x".repeat(200_000);
    insert(&owner, &text);
    let tab = open(1, false);
    tab.set_non_durable_client();
    connect(&tab, &owner);
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    let query = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = pin!(one_shot(&tab, &query, ReadOpts::default()));
    assert!(poll(read.as_mut()).is_pending());
    for _ in 0..100 {
        turn(&[&tab, &owner, &tab]);
        if let Poll::Ready(result) = poll(read.as_mut()) {
            let SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("root rows")
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(
                rows[0].cells().find(|(n, _)| *n == "text").unwrap().1,
                Some(&Value::String(text))
            );
            return;
        }
    }
    panic!("owner chunks did not hydrate");
}

/// Alice requests a long wait during connection establishment. The host must
/// still wake at the shorter attempt window, then expose the owner's cache.
#[test]
fn attempting_foreground_has_a_bounded_host_wake() {
    let owner = open(2, false);
    insert(&owner, "cached");
    let tab = open(1, false);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Attempting);
    connect(&tab, &owner);
    let scheduler = Rc::new(Earliest::default());
    tab.set_tick_scheduler(Some(scheduler.clone()));
    let mut stream = subscribe(&tab, opts(60_000));
    for _ in 0..3 {
        turn(&[&tab, &owner, &tab]);
        assert!(stream.try_next_event().is_none());
    }
    let due = scheduler
        .0
        .borrow_mut()
        .take()
        .expect("attempt-window timer requested");
    assert!(due.saturating_duration_since(Instant::now()) <= jazz::db::REMOTE_LINK_ATTEMPT_WINDOW);
    std::thread::sleep(due.saturating_duration_since(Instant::now()) + Duration::from_millis(20));
    block_on(tab.tick()).unwrap();
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &tab]),
        (vec!["cached".into()], DurabilityTier::Local)
    );
}

/// Alice's foreground cannot claim local durability for an in-memory mutation.
/// Standalone and staged writes fail until worker upload forwarding is ported.
#[test]
fn foreground_writes_fail_without_publishing_or_claiming_durability() {
    let tab = open(1, false);
    tab.set_non_durable_client();
    let cells = || BTreeMap::from([("text".into(), Value::String("unsaved".into()))]);
    let error = block_on(tab.insert("notes", cells(), Default::default())).unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Query);
    assert!(error.message.contains("non-durable foreground"));
    let batch = jazz::tools::OpenTransactionId::new();
    block_on(tab.begin_mergeable(batch)).unwrap();
    let error = block_on(
        tab.mergeable_tx_ref(batch)
            .insert("notes", cells(), Default::default()),
    )
    .unwrap_err();
    assert_eq!(error.code, jazz::db::ErrorCode::Query);
    tab.abandon_transaction_handle(batch).unwrap();
    let query = tab.prepare_query(&Query::from("notes")).unwrap();
    assert!(
        block_on(tab.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly)))
            .unwrap()
            .is_empty()
    );
}

/// Alice's host restarts connection establishment while an older timer is still
/// queued. Its next tick must arm the new attempt boundary, not the long read wait.
#[test]
fn restarted_attempt_rearms_after_the_hosts_older_timer() {
    let owner = open(2, false);
    insert(&owner, "cached");
    let tab = open(1, false);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Attempting);
    connect(&tab, &owner);
    let scheduler = Rc::new(Earliest::default());
    tab.set_tick_scheduler(Some(scheduler.clone()));
    let mut stream = subscribe(&tab, opts(60_000));
    for _ in 0..3 {
        turn(&[&tab, &owner, &tab]);
    }
    let original = scheduler.0.borrow().expect("first attempt wake");
    std::thread::sleep(
        original.saturating_duration_since(Instant::now()) + Duration::from_millis(20),
    );
    tab.set_remote_link_hint(RemoteLinkHint::Attempting);
    assert!(scheduler.0.borrow_mut().take().unwrap() <= Instant::now());
    block_on(tab.tick()).unwrap();
    let next = scheduler
        .0
        .borrow_mut()
        .take()
        .expect("restarted attempt wake");
    assert!(next > Instant::now());
    assert!(
        next.saturating_duration_since(Instant::now())
            <= jazz::db::REMOTE_LINK_ATTEMPT_WINDOW + Duration::from_millis(10)
    );
    assert!(stream.try_next_event().is_none());
    tab.set_remote_link_hint(RemoteLinkHint::Failed);
    assert_eq!(
        opening(&mut stream, &[&tab, &owner, &tab]),
        (vec!["cached".into()], DurabilityTier::Local)
    );
}

/// Alice's already-open Remote subscription pauses when her replica loses its
/// authority. Local previews remain usable; reconnect reconciles the last result.
/// alice -> authority; disconnect -> offline write -> reconnect -> poll
#[test]
fn strict_remote_subscription_rechecks_authority_after_its_opening() {
    for consume_offline in [true, false] {
        let server = open(93, true);
        insert(&server, "accepted");
        let alice = open(92, false);
        let (up, down) = duplex_transport::duplex();
        let connection = block_on(alice.connect_upstream(up));
        let subscriber = server.accept_subscriber(down, AuthorSubject::SYSTEM);
        let mut stream = subscribe(&alice, ReadOpts::for_read_tier(ReadTier::Remote));
        assert_eq!(
            opening(&mut stream, &[&alice, &server, &alice]),
            (vec!["accepted".into()], DurabilityTier::Global)
        );
        while stream.try_next_event().is_some() {}
        assert!(alice.detach_connection(&connection));
        server.disconnect_subscriber(subscriber);
        alice.set_remote_link_hint(RemoteLinkHint::NoServer);
        if consume_offline {
            assert!(stream.try_next_event().is_none(), "lost authority");
        }
        insert(&alice, "offline");
        for _ in 0..10 {
            turn(&[&alice]);
            if consume_offline {
                assert!(stream.try_next_event().is_none(), "offline preview leaked");
            }
        }
        let query = alice.prepare_query(&Query::from("notes")).unwrap();
        assert_eq!(
            block_on(alice.all(&query, ReadOpts::default()))
                .unwrap()
                .len(),
            2
        );
        connect(&alice, &server);
        alice.set_remote_link_hint(RemoteLinkHint::Live);
        for _ in 0..20 {
            turn(&[&alice, &server, &alice]);
        }
        let Some(SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            settled,
            tier,
            ..
        }) = stream.try_next_event()
        else {
            panic!("missing resumed delta")
        };
        assert!(!reset && settled);
        assert_eq!(tier, DurabilityTier::Global);
        assert!(removed.is_empty());
        assert_eq!(added.len(), 1);
        assert_eq!(
            added[0]
                .cells()
                .find(|(name, _)| *name == "text")
                .unwrap()
                .1
                .unwrap(),
            &Value::String("offline".into())
        );
        assert!(stream.try_next_event().is_none(), "duplicate resumed delta");
    }
}

/// Alice consumes ordered deltas before an outage. Reconciliation uses the last
/// delivered ordering, even when several updates were queued between polls.
#[test]
fn strict_remote_reconnect_preserves_delivered_row_indexes() {
    let server = open(83, true);
    let put = |text: &str| {
        block_on(server.insert(
            "notes",
            BTreeMap::from([("text".into(), Value::String(text.into()))]),
            Default::default(),
        ))
        .unwrap()
        .row_uuid()
    };
    let a = put("a");
    let b = put("b");
    let c = put("c");
    let alice = open(82, false);
    let (up, down) = duplex_transport::duplex();
    let connection = block_on(alice.connect_upstream(up));
    let subscriber = server.accept_subscriber(down, AuthorSubject::SYSTEM);
    let query = alice
        .prepare_query(&Query::from("notes").order_by("text", jazz::query::OrderDirection::Asc))
        .unwrap();
    let mut stream =
        block_on(alice.subscribe(&query, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
    assert_eq!(
        opening(&mut stream, &[&alice, &server, &alice]).0,
        ["a", "b", "c"]
    );
    block_on(server.update(
        "notes",
        b,
        BTreeMap::from([("text".into(), Value::String("z".into()))]),
        Default::default(),
    ))
    .unwrap();
    for _ in 0..5 {
        turn(&[&alice, &server, &alice]);
    }
    block_on(server.delete("notes", a, Default::default())).unwrap();
    put("d");
    for _ in 0..5 {
        turn(&[&alice, &server, &alice]);
    }
    while stream.try_next_event().is_some() {}
    // The delivered order is now c, d, z.
    assert!(alice.detach_connection(&connection));
    server.disconnect_subscriber(subscriber);
    insert(&alice, "offline");
    assert!(stream.try_next_event().is_none());
    block_on(server.delete("notes", c, Default::default())).unwrap();
    connect(&alice, &server);
    for _ in 0..20 {
        turn(&[&alice, &server, &alice]);
    }
    let Some(SubscriptionEvent::Delta {
        reset,
        added,
        removed,
        settled,
        tier,
        ..
    }) = stream.try_next_event()
    else {
        panic!("missing resumed delta")
    };
    assert!(!reset && settled);
    assert_eq!(tier, DurabilityTier::Global);
    assert_eq!(removed.len(), 1);
    assert_eq!((removed[0].row_uuid, removed[0].index), (c, 0));
    assert_eq!(added.len(), 1);
    assert_eq!(added[0].index, 1);
    assert_eq!(
        added[0]
            .cells()
            .find(|(name, _)| *name == "text")
            .unwrap()
            .1
            .unwrap(),
        &Value::String("offline".into())
    );
}

/// Alice's real writes advance the SQLite change receipt. Reads, diagnostics and
/// idle ticks leave it unchanged. This low-level receipt supports host tests
/// that must distinguish scheduling a wake from writing persistent state.
#[test]
fn sqlite_change_receipt_observes_writes_without_mutating_storage() {
    let alice = open(91, false);
    let before = alice.sqlite_total_changes_for_test();
    assert_eq!(alice.sqlite_total_changes_for_test(), before);
    insert(&alice, "actual stored row");
    let after_write = alice.sqlite_total_changes_for_test();
    assert!(
        after_write > before,
        "the receipt must observe actual SQL writes"
    );
    let query = alice.prepare_query(&Query::from("notes")).unwrap();
    for _ in 0..16 {
        let rows =
            block_on(alice.all(&query, ReadOpts::for_read_tier(ReadTier::LocalOnly))).unwrap();
        assert_eq!(rows.len(), 1);
        block_on(alice.tick()).unwrap();
        assert_eq!(alice.sqlite_total_changes_for_test(), after_write);
    }
}
