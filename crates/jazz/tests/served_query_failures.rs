//! A prepared-membership query can fail after it has opened, and its failure
//! must reach the browser through Edge without changing the write's fate.
//!
//! ```text
//! Browser -- subscription + write --> Edge -- relayed subscription + write --> Core
//!    ^                                    ^                                      |
//!    +--------- typed rejection ----------+-------------- typed rejection ------+
//! ```

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, SubscriptionEvent,
    SubscriptionServerFailureCode, SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

const MAX_TURNS: usize = 200;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("posts")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("postId", "posts")
                    .column("role", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("profiles")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config)).expect("open Core")
    } else {
        block_on(Db::open(config)).expect("open replica")
    }
}

fn id(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn cells(values: &[(&str, Value)]) -> BTreeMap<String, Value> {
    values
        .iter()
        .map(|(name, value)| ((*name).to_owned(), value.clone()))
        .collect()
}

fn pump(core: &Db, edge: &Db, browser: &Db) {
    for _ in 0..4 {
        block_on(browser.tick()).expect("tick Browser");
        block_on(edge.tick()).expect("tick Edge");
        block_on(core.tick()).expect("tick Core");
        block_on(edge.tick()).expect("tick Edge");
        block_on(browser.tick()).expect("tick Browser");
    }
}

fn drive<T>(core: &Db, edge: &Db, browser: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..MAX_TURNS {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        pump(core, edge, browser);
    }
    panic!("operation did not complete within {MAX_TURNS} turns");
}

fn drain_opening(stream: &mut SubscriptionStream) {
    let mut opened = false;
    while let Some(event) = stream.try_next_event() {
        match event {
            SubscriptionEvent::Delta { reset: true, .. } => opened = true,
            SubscriptionEvent::Rejected { code } => {
                panic!("subscription failed before opening: {code:?}")
            }
            _ => {}
        }
    }
    assert!(opened, "subscription opening was not delivered");
}

fn wait_for_rejection(
    core: &Db,
    edge: &Db,
    browser: &Db,
    stream: &mut SubscriptionStream,
) -> SubscriptionServerFailureCode {
    for _ in 0..MAX_TURNS {
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Rejected { code } = event {
                return code;
            }
        }
        pump(core, edge, browser);
    }
    panic!("subscription rejection did not reach Browser within {MAX_TURNS} turns");
}

#[test]
fn prepared_membership_failure_reaches_browser_and_leaves_write_fate_independent() {
    let author = AuthorSubject::for_test_bytes([0x55; 16]);
    let core = open(0, AuthorSubject::SYSTEM);
    core.seed_settled_mergeable_for_bootstrap(
        "posts",
        id(1),
        AuthorSubject::SYSTEM,
        cells(&[("title", Value::String("seed post".into()))]),
    )
    .expect("seed post");
    core.seed_settled_mergeable_for_bootstrap(
        "memberships",
        id(2),
        AuthorSubject::SYSTEM,
        cells(&[
            ("postId", Value::Uuid(id(1).0)),
            ("role", Value::String("reader".into())),
        ]),
    )
    .expect("seed membership");
    core.seed_settled_mergeable_for_bootstrap(
        "profiles",
        id(3),
        AuthorSubject::SYSTEM,
        cells(&[("name", Value::String("seed profile".into()))]),
    )
    .expect("seed profile");

    let edge = open(1, author);
    let (edge_upstream, core_downstream) = duplex();
    let _edge_upstream = block_on(edge.connect_upstream(edge_upstream));
    core.accept_subscriber(core_downstream, author);

    let browser = open(2, author);
    browser.set_non_durable_client();
    let (browser_upstream, edge_downstream) = duplex();
    let _browser_upstream = block_on(browser.connect_upstream(browser_upstream));
    edge.accept_subscriber(edge_downstream, author);

    let membership_query = Query::from("posts").array_subquery(ArraySubquery::new(
        "memberships",
        "memberships",
        "postId",
        "id",
    ));
    let membership_prepared = browser
        .prepare_query(&membership_query)
        .expect("prepare membership query");
    let mut membership_stream =
        block_on(browser.subscribe(&membership_prepared, ReadOpts::default()))
            .expect("subscribe to memberships");
    let profiles_prepared = browser
        .prepare_query(&Query::from("profiles"))
        .expect("prepare profiles");
    let mut profiles_stream = block_on(browser.subscribe(&profiles_prepared, ReadOpts::default()))
        .expect("subscribe to profiles");

    pump(&core, &edge, &browser);
    drain_opening(&mut membership_stream);
    drain_opening(&mut profiles_stream);
    assert_eq!(core.runtime_stats_for_test().active_subscriptions, 2);
    assert_eq!(edge.runtime_stats_for_test().active_subscriptions, 2);
    assert_eq!(browser.runtime_stats_for_test().active_subscriptions, 2);

    assert!(core.fail_next_membership_refresh_for_test(
        "posts",
        Error::new(ErrorCode::Protocol, "private malformed membership payload"),
    ));
    let write = block_on(browser.insert(
        "posts",
        cells(&[("title", Value::String("write with failing query".into()))]),
        Default::default(),
    ))
    .expect("write locally");
    let tx = write.mergeable_tx_id();
    drive(
        &core,
        &edge,
        &browser,
        browser.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .expect("the write was accepted independently of the subscription failure");

    assert_eq!(
        wait_for_rejection(&core, &edge, &browser, &mut membership_stream),
        SubscriptionServerFailureCode::QueryResultProtocol,
    );
    assert_eq!(
        membership_stream.try_next_event(),
        None,
        "rejection is terminal"
    );
    pump(&core, &edge, &browser);
    assert_eq!(core.runtime_stats_for_test().active_subscriptions, 1);
    assert_eq!(edge.runtime_stats_for_test().active_subscriptions, 1);
    assert_eq!(browser.runtime_stats_for_test().active_subscriptions, 1);

    let profile = block_on(browser.insert(
        "profiles",
        cells(&[("name", Value::String("still live".into()))]),
        Default::default(),
    ))
    .expect("write profile");
    pump(&core, &edge, &browser);
    let mut saw_profile = false;
    while let Some(event) = profiles_stream.try_next_event() {
        if let SubscriptionEvent::Delta { added, .. } = event {
            saw_profile |= added.iter().any(|row| row.row_uuid() == profile.row_uuid());
        }
    }
    assert!(
        saw_profile,
        "the unrelated profile subscription remains live"
    );
}
