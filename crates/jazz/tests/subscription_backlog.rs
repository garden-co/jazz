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

use jazz::db::{Db, InsertOptions, ReadOpts, SubscriptionEvent, block_on};
use jazz::query::{OrderDirection, Query};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use crate::common::{allow_all_policies, compile_schema};
use crate::fanout_correlation::{
    Live, apply, apply_event, check, delete, fresh, id, insert, open, subscribe, subscribe_with,
    text, update, uuid,
};

const NOTES: u8 = 9;

fn notes_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("folders")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .column("n", ColumnType::BigInt)
                    .nullable_fk_column("folder", "folders")
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

/// A reader that keeps up is never coalesced, even by one delta bigger
/// than its limit: `erin` reads after a bulk import of 40 rows in one
/// transaction (her limit is 10) and gets the import itself, not a
/// coalesced reset. A second write before she reads makes her a reader
/// that fell behind, and that coalesces.
#[test]
fn one_big_delta_to_a_reader_that_keeps_up_is_delivered_as_it_is() {
    let store = open(notes_schema(), 4);
    let mut erin = subscribe_with(
        &store,
        "erin",
        Query::from("notes").order_by("title", OrderDirection::Asc),
        None,
        ReadOpts::default().with_backlog_rows(rows(10)),
    );
    apply(&mut erin);
    let import = |from: usize| {
        block_on(store.transaction(async |tx| {
            for n in from..from + 40 {
                tx.insert(
                    "notes",
                    [
                        ("title".to_owned(), text(&format!("note {n:03}"))),
                        ("n".to_owned(), Value::I64(0)),
                    ]
                    .into(),
                    InsertOptions {
                        row_id: Some(id(NOTES, n)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
        .unwrap();
    };
    import(0);
    let queued = events(&mut erin);
    assert_eq!(queued.len(), 1);
    let SubscriptionEvent::Delta {
        reset: false,
        coalesced: false,
        added,
        ..
    } = &queued[0]
    else {
        panic!(
            "erin: expected the import as one delta, got {:?}",
            queued[0]
        );
    };
    assert_eq!(added.len(), 40);
    for event in queued {
        apply_event(&mut erin, event);
    }
    assert_eq!(store.fanout_stats().coalesced, 0);

    import(100);
    bump(&store, 0, 1);
    let first = erin.stream.try_next_event().unwrap();
    assert!(
        matches!(
            first,
            SubscriptionEvent::Delta {
                reset: true,
                coalesced: true,
                ..
            }
        ),
        "two writes unread: coalesced, got {first:?}"
    );
    apply_event(&mut erin, first);
    check(&store, std::slice::from_mut(&mut erin), "two writes");
}

/// A windowed read with an include, through updates that move rows in and
/// out of the window, deletes, and changes to included rows: a slow reader
/// coalesced several times converges to a fresh read, as does a reader
/// that keeps up.
#[test]
fn a_coalesced_window_with_includes_converges_through_deletes() {
    let store = open(notes_schema(), 5);
    const FOLDERS: u8 = 8;
    for f in 0..3 {
        insert(
            &store,
            "folders",
            id(FOLDERS, f),
            &[("name", text(&format!("folder {f}")))],
        );
    }
    for n in 0..30 {
        insert(
            &store,
            "notes",
            id(NOTES, n),
            &[
                ("title", text(&format!("note {n:02}"))),
                ("n", Value::I64(n as i64)),
                ("folder", uuid(id(FOLDERS, n % 3))),
            ],
        );
    }
    let window = Query::from("notes")
        .include("folder")
        .order_by("n", OrderDirection::Asc)
        .limit(5);
    let mut fred = subscribe(&store, "fred", window.clone(), None);
    let mut gina = subscribe_with(
        &store,
        "gina",
        window,
        None,
        ReadOpts::default().with_backlog_rows(rows(3)),
    );
    check(&store, std::slice::from_mut(&mut fred), "subscribing");
    for step in 0..120usize {
        match step % 4 {
            // Move a row to the front of the window, or past its end.
            // Notes 12.. are never deleted.
            0 | 1 => bump(&store, 12 + (step * 7) % 18, ((step * 13) % 40) as i64 - 5),
            2 => update(
                &store,
                "folders",
                id(FOLDERS, step % 3),
                &[("name", text(&format!("folder {step}")))],
            ),
            _ if step % 8 == 3 && step / 8 < 12 => delete(&store, "notes", id(NOTES, step / 8)),
            _ => bump(&store, 12 + step % 18, -10),
        }
        check(
            &store,
            std::slice::from_mut(&mut fred),
            &format!("step {step}"),
        );
        if step % 30 == 29 {
            check(
                &store,
                std::slice::from_mut(&mut gina),
                &format!("step {step}"),
            );
        }
    }
    assert!(
        store.fanout_stats().coalesced >= 3,
        "gina fell behind each round"
    );
}
