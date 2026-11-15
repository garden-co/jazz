//! Bounded subscription backlogs: a local reader that falls behind a fast
//! writer holds at most its backlog limit of undelivered changed rows (plus
//! its result). Past the limit its queued deltas are dropped and it is told
//! the current result instead, as one reset marked `coalesced`. A result is
//! a set, so that reset supersedes every delta it replaces, and the reader
//! converges to exactly what a fresh read returns.
//!
//! ```text
//! writer ──update×N──► store ──delta──► fast reader   (drains every write)
//!                        │
//!                        └──delta──► slow reader       (never drains)
//!                                      queue > limit ──► one coalesced reset
//! ```

use std::num::NonZeroUsize;

use jazz::db::{Db, ReadOpts, SubscriptionEvent};
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};
use crate::fanout_correlation::{
    Live, apply, apply_event, check, fresh, id, insert, open, subscribe, subscribe_with, text,
    update,
};

const NOTES: u8 = 9;

fn notes_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .column("n", ColumnType::BigInt)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn rows(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

fn note(db: &Db, n: usize) {
    insert(
        db,
        "notes",
        id(NOTES, n),
        &[("title", text(&format!("note {n}"))), ("n", Value::I64(0))],
    );
}

fn bump(db: &Db, n: usize, to: i64) {
    update(db, "notes", id(NOTES, n), &[("n", Value::I64(to))]);
}

/// Every queued event, in order.
fn events(live: &mut Live) -> Vec<SubscriptionEvent> {
    std::iter::from_fn(|| live.stream.try_next_event()).collect()
}

/// `alice` reads every change as it happens while `bob`'s subscription to
/// the same query is never drained during 600 writes. Bob's backlog never
/// exceeds his limit plus one write's changes; when he reads, he gets one
/// coalesced reset that equals a fresh read. Alice, sharing the group, is
/// told every delta and never a coalesced one.
#[test]
fn a_slow_reader_holds_at_most_its_limit_and_converges() {
    let store = open(notes_schema(), 1);
    for n in 0..20 {
        note(&store, n);
    }
    let all = Query::from("notes").order_by("title", OrderDirection::Asc);
    let mut alice = subscribe(&store, "alice", all.clone(), None);
    let mut bob = subscribe_with(
        &store,
        "bob",
        all,
        None,
        ReadOpts::default().with_backlog_rows(rows(50)),
    );
    check(&store, std::slice::from_mut(&mut alice), "subscribing");
    for step in 0..600 {
        bump(&store, step % 20, step as i64);
        // One write changes one row here: the backlog stays within the
        // limit plus that write.
        assert!(
            bob.stream.backlog_rows() <= 51,
            "bob's backlog grew to {} rows after {step} writes",
            bob.stream.backlog_rows()
        );
        for event in events(&mut alice) {
            let SubscriptionEvent::Delta { coalesced, .. } = &event else {
                panic!("alice: unexpected event {event:?}");
            };
            assert!(!coalesced, "alice keeps up: she is told every change");
            apply_event(&mut alice, event);
        }
        apply(&mut alice);
    }
    check(&store, std::slice::from_mut(&mut alice), "600 writes");
    assert!(store.fanout_stats().coalesced >= 1);

    let queued = events(&mut bob);
    let SubscriptionEvent::Delta {
        reset: true,
        coalesced: true,
        settled: true,
        ..
    } = &queued[0]
    else {
        panic!(
            "bob: expected one coalesced reset first, got {:?}",
            queued[0]
        );
    };
    assert!(
        queued.len() <= 51,
        "bob: {} events queued past the reset",
        queued.len()
    );
    for event in queued {
        apply_event(&mut bob, event);
    }
    apply(&mut bob);
    let now = fresh(&store, &bob, ReadOpts::default());
    assert_eq!(bob.rows, now, "bob converges to the current result");
}

/// The store-wide limit applies to a subscription that sets none, and a
/// reader that falls behind again after reading a coalesced reset is
/// coalesced again: each time, what it reads converges.
#[test]
fn the_store_limit_bounds_every_reader_each_time_it_falls_behind() {
    let store = open(notes_schema(), 2);
    store.set_subscription_backlog_limit(rows(10));
    let mut carol = subscribe(
        &store,
        "carol",
        Query::from("notes").order_by("title", OrderDirection::Asc),
        None,
    );
    for round in 0..3 {
        for n in 0..40 {
            note(&store, round * 100 + n);
            assert!(carol.stream.backlog_rows() <= 11);
        }
        let first = carol.stream.try_next_event().unwrap();
        assert!(
            matches!(
                first,
                SubscriptionEvent::Delta {
                    reset: true,
                    coalesced: true,
                    ..
                }
            ),
            "round {round}: expected a coalesced reset, got {first:?}"
        );
        apply_event(&mut carol, first);
        check(
            &store,
            std::slice::from_mut(&mut carol),
            &format!("round {round}"),
        );
    }
    assert_eq!(store.fanout_stats().coalesced, 3);
}

/// A result bigger than the limit is not a backlog: its opening is the
/// size of the result anyway, so it is delivered as an ordinary reset and
/// only the deltas after it count toward the limit.
#[test]
fn an_opening_bigger_than_the_limit_is_not_coalesced() {
    let store = open(notes_schema(), 3);
    for n in 0..30 {
        note(&store, n);
    }
    let mut dave = subscribe_with(
        &store,
        "dave",
        Query::from("notes").order_by("title", OrderDirection::Asc),
        None,
        ReadOpts::default().with_backlog_rows(rows(5)),
    );
    bump(&store, 1, 1);
    let queued = events(&mut dave);
    assert!(matches!(
        queued[0],
        SubscriptionEvent::Delta {
            reset: true,
            coalesced: false,
            ..
        }
    ));
    assert_eq!(queued.len(), 2, "the opening, then the one update");
    assert_eq!(store.fanout_stats().coalesced, 0);
}
