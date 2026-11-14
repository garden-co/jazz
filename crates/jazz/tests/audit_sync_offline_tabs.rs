//! Audit (sync-offline): two non-durable foregrounds (tabs) over one durable
//! storage owner (the browser worker) while the owner is offline, and what a
//! tab shows across a replacement of its link to the owner (worker recovery,
//! a follower reopening after an unresponsive worker).
//!
//! SPEC 8 §8.13: "On a new link a session drops the relay versions it shows
//! (pending versions other writers wrote) back to the newest accepted version
//! it holds ... the one it reaches ships its unsettled versions again as it
//! answers the session's reads." The intermediate state between the drop and
//! the reship is observable unless it is withheld.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use crate::common;

use duplex_transport::duplex;
use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, RemoteLinkHint, SubscriptionEvent,
    Upstream,
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

fn config(node: u8, author: AuthorSubject) -> DbConfig {
    DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

const WORKER: u8 = 0x40;

fn author() -> AuthorSubject {
    AuthorSubject::for_test_bytes([WORKER; 16])
}

fn attach(tab: &Db, worker: &Db) -> Upstream {
    let (tab_side, worker_side) = duplex();
    let upstream = block_on(tab.connect_upstream(tab_side));
    worker.accept_subscriber(worker_side, author()).unwrap();
    upstream
}

fn turn(dbs: &[&Db]) {
    for db in dbs {
        block_on(db.tick()).unwrap();
    }
}

/// Every event of `stream` while driving `turns` owner turns, as
/// `(reset, added ids, removed ids)`.
fn drain(
    stream: &mut jazz::db::SubscriptionStream,
    dbs: &[&Db],
    turns: usize,
) -> Vec<(bool, Vec<RowUuid>, Vec<RowUuid>)> {
    let mut events = Vec::new();
    for _ in 0..turns {
        turn(dbs);
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta {
                reset,
                added,
                removed,
                ..
            } = event
            {
                events.push((
                    reset,
                    added.iter().map(|r| r.row_uuid()).collect(),
                    removed.iter().map(|r| r.row_uuid).collect(),
                ));
            }
        }
    }
    events
}

fn local_ids(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("items")).unwrap();
    block_on(db.all(&prepared, ReadOpts::for_read_tier(ReadTier::LocalFirst)))
        .unwrap()
        .into_iter()
        .map(|r| r.row_uuid())
        .collect()
}

/// While the worker is offline, a row tab A wrote is shown in tab B. When
/// tab B's link to the same worker is replaced, the row must not drop out of
/// B's live result (and B's one-shot reads) while the worker reships it.
///
/// ```text
/// worker (offline, hint Failed)
///   ├── tab A ── insert X (pending at the worker) ──┐
///   └── tab B ── subscribe items ◄── X (relay version)
/// tab B: link replaced (detach, reattach to the same worker)
///   expected: B's stream never removes X; B's local read keeps X
/// ```
#[test]
fn a_tab_relinking_to_its_offline_worker_keeps_other_tabs_pending_rows() {
    let worker = block_on(Db::open(config(WORKER, author()))).expect("open worker");
    worker.set_remote_link_hint(RemoteLinkHint::Failed);

    let tab_a = block_on(Db::open(config(0x41, author()))).unwrap();
    tab_a.set_non_durable_client();
    let _a_link = attach(&tab_a, &worker);
    let tab_b = block_on(Db::open(config(0x42, author()))).unwrap();
    tab_b.set_non_durable_client();
    let b_link = attach(&tab_b, &worker);

    let prepared = tab_b.prepare_query(&Query::from("items")).unwrap();
    let mut stream =
        block_on(tab_b.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::LocalFirst)))
            .unwrap();
    let all = [&tab_a, &worker, &tab_b];
    drain(&mut stream, &all, 10);

    let handle = block_on(tab_a.insert(
        "items",
        BTreeMap::from([("label".to_owned(), Value::String("x".to_owned()))]),
        InsertOptions::default(),
    ))
    .unwrap();
    let x = handle.row_uuid();
    let before = drain(&mut stream, &all, 20);
    assert!(
        before.iter().any(|(_, added, _)| added.contains(&x)),
        "tab B sees tab A's offline row: {before:?}"
    );
    assert!(local_ids(&tab_b).contains(&x));

    // B's link to the worker is replaced (worker recovery / follower reopen).
    assert!(tab_b.detach_connection(&b_link));
    let _b_link = attach(&tab_b, &worker);

    // Inspect B's state at every turn of the transition.
    let mut flickered_local = false;
    let mut events = Vec::new();
    for _ in 0..20 {
        turn(&[&tab_b]);
        if !local_ids(&tab_b).contains(&x) {
            flickered_local = true;
        }
        events.extend(drain(&mut stream, &[&worker, &tab_b], 1));
        if !local_ids(&tab_b).contains(&x) {
            flickered_local = true;
        }
    }
    eprintln!("tab B events across relink: {events:?}");
    let removed = events.iter().any(|(_, _, removed)| removed.contains(&x));
    assert!(
        !removed && !flickered_local,
        "tab A's pending row dropped out of tab B across a relink to the same worker \
         (stream removed: {removed}, local read lost it: {flickered_local})"
    );
    assert!(local_ids(&tab_b).contains(&x), "and it is back afterwards");
}
