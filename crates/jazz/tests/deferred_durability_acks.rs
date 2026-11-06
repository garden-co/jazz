//! A relay whose file writes finish after its commits return (a browser
//! worker's IndexedDB file) holds the acks that tell a session a commit is
//! `Local` until its host reports the store persisted (INV-SYNC-55). Before,
//! a tab heard "stored" while the commit was still only in memory, so a
//! failed IndexedDB write (quota) lost writes the app was told were saved.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

fn open(node: u8) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn insert(db: &Db, title: &str) -> u64 {
    block_on(db.insert(
        "todos",
        BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
        Default::default(),
    ))
    .unwrap()
    .mergeable_tx_id()
}

/// Whether `future` is ready after one poll.
fn ready<T>(future: impl Future<Output = T>) -> Option<T> {
    match pin!(future).poll(&mut Context::from_waker(Waker::noop())) {
        Poll::Ready(output) => Some(output),
        Poll::Pending => None,
    }
}

fn tick(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}

/// A worker relay over an offline upstream (core is never ticked) with one
/// tab, as in the browser.
fn offline_worker_and_tab() -> (Db, Db, Db) {
    let core = open(0xc0);
    let relay = open(0x10);
    relay.defer_durability_acks();
    let tab = open(0x11);
    tab.set_non_durable_client();
    let (client_side, server_side) = duplex();
    block_on(relay.connect_upstream(client_side));
    core.accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    relay
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    (core, relay, tab)
}

#[test]
fn a_tabs_write_is_local_only_once_its_relay_persisted_it() {
    let (_core, relay, tab) = offline_worker_and_tab();
    let tx = insert(&tab, "photo");
    tick(&[&tab, &relay, &tab]);
    assert!(
        ready(tab.wait_for_transaction(tx, DurabilityTier::Local)).is_none(),
        "the relay holds the commit only in memory: the tab must not count it Local"
    );

    let point = relay.persist_point().expect("an ack waits for a persist");
    assert!(relay.persisted(point), "the held ack is released");
    assert_eq!(relay.persist_point(), None, "nothing else waits");
    tick(&[&relay, &tab]);
    assert_eq!(
        ready(tab.wait_for_transaction(tx, DurabilityTier::Local)).map(Result::unwrap),
        Some(tx),
        "once persisted, the tab's write is Local"
    );
}

#[test]
fn a_persist_releases_only_the_commits_made_before_it_started() {
    let (_core, relay, tab) = offline_worker_and_tab();
    let first = insert(&tab, "first");
    tick(&[&tab, &relay]);
    // A persist starts here; a commit after it is not covered by it.
    let point = relay.persist_point().unwrap();
    let second = insert(&tab, "second");
    tick(&[&tab, &relay]);
    assert!(relay.persisted(point));
    tick(&[&relay, &tab]);
    assert_eq!(
        ready(tab.wait_for_transaction(first, DurabilityTier::Local)).map(Result::unwrap),
        Some(first)
    );
    assert!(
        ready(tab.wait_for_transaction(second, DurabilityTier::Local)).is_none(),
        "a commit made after the persist started stays unacknowledged"
    );

    let point = relay.persist_point().unwrap();
    assert!(relay.persisted(point));
    tick(&[&relay, &tab]);
    assert_eq!(
        ready(tab.wait_for_transaction(second, DurabilityTier::Local)).map(Result::unwrap),
        Some(second)
    );
}

#[test]
fn a_persist_that_never_succeeds_never_acknowledges() {
    let (_core, relay, tab) = offline_worker_and_tab();
    let tx = insert(&tab, "photo");
    // A host whose persists keep failing (storage full) never reports one.
    for _ in 0..5 {
        tick(&[&tab, &relay, &tab]);
    }
    assert!(relay.persist_point().is_some(), "the ack stays held");
    assert!(ready(tab.wait_for_transaction(tx, DurabilityTier::Local)).is_none());
}

#[test]
fn a_relay_without_deferred_acks_acknowledges_at_once() {
    let core = open(0xc0);
    let relay = open(0x10);
    let tab = open(0x11);
    tab.set_non_durable_client();
    let (client_side, server_side) = duplex();
    block_on(relay.connect_upstream(client_side));
    core.accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    relay
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let tx = insert(&tab, "photo");
    tick(&[&tab, &relay, &tab]);
    assert_eq!(relay.persist_point(), None);
    assert_eq!(
        ready(tab.wait_for_transaction(tx, DurabilityTier::Local)).map(Result::unwrap),
        Some(tx)
    );
}

/// A worker with no server holds its tab's commits for upload, like an
/// offline one: their acks wait for a persist too, and arrive in order.
#[test]
fn a_serverless_workers_acks_wait_for_a_persist() {
    let author = AuthorSubject::for_test_bytes([0x63; 16]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let open = |node: u8| {
        block_on(Db::open(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )))
        .unwrap()
    };
    let worker = open(0x70);
    worker.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    worker.defer_durability_acks();
    let tab = open(0x71);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    worker.accept_subscriber(server_side, author).unwrap();

    let first = insert(&tab, "first");
    let second = insert(&tab, "second");
    tick(&[&tab, &worker, &tab]);
    for tx in [first, second] {
        assert!(ready(tab.wait_for_transaction(tx, DurabilityTier::Local)).is_none());
    }
    let point = worker.persist_point().unwrap();
    assert!(worker.persisted(point));
    tick(&[&worker, &tab]);
    for tx in [first, second] {
        assert_eq!(
            ready(tab.wait_for_transaction(tx, DurabilityTier::Local)).map(Result::unwrap),
            Some(tx)
        );
    }
}

/// A tab that goes away while its acks wait is dropped from them.
#[test]
fn a_persist_drops_the_acks_of_a_tab_that_went_away() {
    let core = open(0xc0);
    let relay = open(0x10);
    relay.defer_durability_acks();
    let tab = open(0x11);
    tab.set_non_durable_client();
    let (client_side, server_side) = duplex();
    block_on(relay.connect_upstream(client_side));
    core.accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    let subscriber = relay
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap();
    insert(&tab, "photo");
    tick(&[&tab, &relay]);
    relay.disconnect_subscriber(subscriber);
    let point = relay.persist_point().unwrap();
    assert!(
        !relay.persisted(point),
        "nothing is sent to a closed session"
    );
    assert_eq!(relay.persist_point(), None);
}
