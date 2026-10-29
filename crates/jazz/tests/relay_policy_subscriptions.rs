//! A second device of the same account subscribes through its worker to rows
//! its first device wrote, under a read policy on `$createdBy.account`: the
//! authority's subscription wakes on the commits that write them, though no
//! row image carries that column.

use crate::common;

use std::collections::BTreeMap;

use jazz::db::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{compile_schema, read_and_allow_all_writes, session_eq};

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq(
                        "$createdBy.account",
                        &["user", "account"],
                    ))),
            )
            .build(),
    );
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

/// A worker connected to `core`, and one tab over it.
fn device(core: &Db, node: u8, author: AuthorSubject) -> (Db, Db) {
    let worker = open(node, author);
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    let (client_side, server_side) = duplex();
    block_on(worker.connect_upstream(client_side));
    core.accept_subscriber(server_side, author);
    let tab = open(node + 1, author);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    worker.accept_subscriber(server_side, author);
    (worker, tab)
}

#[test]
fn a_second_device_of_the_account_hydrates_its_rows_through_its_worker() {
    let account = AuthorSubject::for_test_bytes([0x41; 16]);
    let core = open(0xc0, AuthorSubject::SYSTEM);
    let (worker_a, tab_a) = device(&core, 0x10, account);
    let (worker_b, tab_b) = device(&core, 0x20, account);
    let all = [&tab_a, &worker_a, &core, &worker_b, &tab_b];
    let pump = || {
        for _ in 0..4 {
            for db in all {
                block_on(db.tick()).unwrap();
            }
            for db in all.iter().rev() {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let query = tab_a.prepare_query(&Query::from("todos")).unwrap();
    let _watch_a = block_on(tab_a.subscribe(&query, ReadOpts::default())).unwrap();
    for title in ["one", "two"] {
        let handle = block_on(tab_a.insert(
            "todos",
            BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
            Default::default(),
        ))
        .unwrap();
        let mut wait = std::pin::pin!(
            tab_a.wait_for_transaction(handle.mergeable_tx_id(), DurabilityTier::Global)
        );
        loop {
            let mut context = std::task::Context::from_waker(std::task::Waker::noop());
            if let std::task::Poll::Ready(result) = wait.as_mut().poll(&mut context) {
                result.unwrap();
                break;
            }
            pump();
        }
    }
    let query = tab_b.prepare_query(&Query::from("todos")).unwrap();
    let mut watch_b = block_on(tab_b.subscribe(&query, ReadOpts::default())).unwrap();
    pump();
    let mut rows = 0i64;
    while let Some(event) = watch_b.try_next_event() {
        if let jazz::db::SubscriptionEvent::Delta {
            reset,
            added,
            removed,
            ..
        } = event
        {
            if reset {
                rows = 0;
            }
            rows += added.len() as i64 - removed.len() as i64;
        }
    }
    let local = |db: &Db| {
        let q = db.prepare_query(&Query::from("todos")).unwrap();
        block_on(db.all(&q, ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly)))
            .unwrap()
            .len()
    };
    eprintln!(
        "DBG core={} worker_b={} tab_b={} worker_a={}",
        local(&core),
        local(&worker_b),
        local(&tab_b),
        local(&worker_a)
    );
    assert_eq!(rows, 2, "the second device's tab hydrates both rows");
}
