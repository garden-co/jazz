//! The test-only refresh hook models failures that cannot be caused through
//! valid public writes without corrupting the store. All outcomes are asserted
//! through public reads, streams, write settlement and runtime diagnostics.
//!
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
    Db, DbConfig, DbIdentity, Error, ErrorCode, ReadOpts, SubscriptionEvent, SubscriptionStream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes};

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
            SubscriptionEvent::Rejected { reason } => {
                panic!("subscription failed before opening: {reason:?}")
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
) -> String {
    for _ in 0..MAX_TURNS {
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Rejected { reason } = event {
                return reason;
            }
        }
        pump(core, edge, browser);
    }
    panic!("subscription rejection did not reach Browser within {MAX_TURNS} turns");
}

fn prepared_membership_failure(write_at_core: bool) {
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
    let writer = if write_at_core { &core } else { &browser };
    let write = block_on(writer.insert(
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
        writer.wait_for_transaction(tx, DurabilityTier::Global),
    )
    .expect("the write was accepted independently of the subscription failure");

    let reason = wait_for_rejection(&core, &edge, &browser, &mut membership_stream);
    assert!(reason.contains("Subscription query failed"), "{reason}");
    assert!(!reason.contains("private malformed"));
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

/// Alice's browser receives Core's query failure through Edge while her write
/// settles independently and her unrelated profile subscription remains live.
#[test]
fn prepared_membership_failure_reaches_browser_and_leaves_write_fate_independent() {
    prepared_membership_failure(false);
}

/// Core's write fails Alice's query while her browser and Edge are idle:
/// Core -- rejection --> idle Edge -- rejection --> Alice.
#[test]
fn prepared_membership_failure_reaches_idle_browser() {
    prepared_membership_failure(true);
}

enum Retirement {
    Unsubscribe,
    UnsubscribeAfterUpdate,
    RejectAfterInsert,
    RejectAfterUpdate,
}

fn retained_rows_are_revoked(retirement: Retirement, revoke_before_drain: bool) {
    let reject = matches!(
        retirement,
        Retirement::RejectAfterInsert | Retirement::RejectAfterUpdate
    );
    let update = matches!(
        retirement,
        Retirement::UnsubscribeAfterUpdate | Retirement::RejectAfterUpdate
    );
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("posts")
                    .column("title", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "grants",
                        vec![outer_eq("postId", "id")],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("postId", "posts")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("grants")
                    .fk_column("postId", "posts")
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let alice_author = AuthorSubject::for_test_bytes([0x57; 16]);
    let config = |node, author| {
        DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )
    };
    let core = block_on(Db::open_history_complete(config(0, AuthorSubject::SYSTEM))).unwrap();
    let alice = block_on(Db::open(config(1, alice_author))).unwrap();
    core.seed_settled_mergeable_for_bootstrap(
        "posts",
        id(1),
        AuthorSubject::SYSTEM,
        cells(&[("title", Value::String("protected post".into()))]),
    )
    .unwrap();
    core.seed_settled_mergeable_for_bootstrap(
        "grants",
        id(3),
        AuthorSubject::SYSTEM,
        cells(&[("postId", Value::Uuid(id(1).0))]),
    )
    .unwrap();
    let (upstream, downstream) = duplex();
    let _upstream = block_on(alice.connect_upstream(upstream));
    core.accept_subscriber(downstream, alice_author);
    let pump = || {
        for _ in 0..4 {
            block_on(alice.tick()).unwrap();
            block_on(core.tick()).unwrap();
        }
        block_on(alice.tick()).unwrap();
    };
    let query = alice
        .prepare_query(&Query::from("posts").array_subquery(ArraySubquery::new(
            "memberships",
            "memberships",
            "postId",
            "id",
        )))
        .unwrap();
    let mut stream = block_on(alice.subscribe(&query, ReadOpts::default())).unwrap();
    pump();
    drain_opening(&mut stream);
    let local = || {
        let query = alice.prepare_query(&Query::from("posts")).unwrap();
        block_on(alice.all(
            &query,
            ReadOpts::for_read_tier(jazz::db::ReadTier::LocalOnly),
        ))
        .unwrap()
        .into_iter()
        .map(|row| row.row_uuid())
        .collect::<Vec<_>>()
    };
    assert_eq!(local(), [id(1)]);
    if reject {
        assert!(core.fail_next_membership_refresh_for_test(
            "posts",
            Error::new(ErrorCode::Protocol, "injected membership failure"),
        ));
    }
    if update {
        block_on(core.update(
            "posts",
            id(1),
            cells(&[("title", Value::String("updated protected post".into()))]),
            Default::default(),
        ))
        .unwrap();
    } else if reject {
        block_on(core.insert(
            "posts",
            cells(&[("title", Value::String("trigger".into()))]),
            Default::default(),
        ))
        .unwrap();
    }
    if revoke_before_drain {
        // Queue the removal behind the rejection before Core drains either.
        block_on(core.delete("grants", id(3), Default::default())).unwrap();
    }
    pump();
    if reject {
        assert!(matches!(
            stream.try_next_event(),
            Some(SubscriptionEvent::Rejected { .. })
        ));
        assert_eq!(stream.try_next_event(), None);
    }
    drop(stream);
    pump();
    assert_eq!(core.runtime_stats_for_test().active_subscriptions, 0);
    if !revoke_before_drain {
        assert_eq!(local(), [id(1)], "Alice retains the still-readable post");
        block_on(core.delete("grants", id(3), Default::default())).unwrap();
        pump();
    }
    assert_eq!(
        local(),
        [],
        "Alice loses the cached post when its permission grant is deleted",
    );
}

/// Alice's cached post remains subject to Core's policy after normal unsubscribe.
#[test]
fn unsubscribe_retains_policy_tracking() {
    retained_rows_are_revoked(Retirement::Unsubscribe, false);
}

/// Alice's cached post remains subject to Core's policy after a failed query:
/// Core rejects --> Alice ends subscription --> Core revokes grant --> post leaves.
#[test]
fn rejection_retains_policy_tracking() {
    retained_rows_are_revoked(Retirement::RejectAfterInsert, false);
}

/// Alice loses her cached post even when Core revokes access before sending
/// the query failure: reject --> revoke grant --> drain --> Alice loses post.
#[test]
fn rejection_preserves_queued_permission_revocations() {
    retained_rows_are_revoked(Retirement::RejectAfterInsert, true);
}

/// Alice's updated post stays subject to Core's policy after a healthy query
/// ends: update --> unsubscribe --> revoke grant --> cached post leaves.
#[test]
fn unsubscribe_retains_updated_row_policy_tracking() {
    retained_rows_are_revoked(Retirement::UnsubscribeAfterUpdate, false);
}

/// Alice loses an updated post after the update triggers a query failure:
/// update --> reject --> revoke grant --> cached post leaves. The old result
/// sequence must not prevent tracking the current version's permissions.
#[test]
fn rejection_retains_updated_row_policy_tracking() {
    retained_rows_are_revoked(Retirement::RejectAfterUpdate, false);
}

/// Alice loses the updated post when its grant is revoked before the failure
/// is delivered: update --> reject --> revoke grant --> drain --> post leaves.
#[test]
fn rejection_preserves_updated_row_queued_permission_revocations() {
    retained_rows_are_revoked(Retirement::RejectAfterUpdate, true);
}
