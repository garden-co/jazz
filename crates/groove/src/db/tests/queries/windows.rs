//! Arg-min/max and ordered-window behavior.

use super::*;

#[test]
fn arg_max_by_hydrates_and_tracks_winner_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "old"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe_one_sink(history_arg_max()).unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[0] {
        Value::U64(row) => row,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 20, 1, "winner"), 1),
            (history_values(2, 5, 1, "other"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "new"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "winner"), -1),
            (history_values(1, 30, 1, "new"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "new"), -1),
            (history_values(1, 20, 1, "winner"), 1),
        ]
    );
}

#[test]
fn arg_max_by_suppresses_non_winner_and_net_zero_deltas() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_max()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "winner"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "winner"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "loser"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 1, 1, "temporary"));
    batch.delete("history", history_key(2, 1, 1));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());
}

#[test]
fn arg_max_by_handles_multi_delta_same_group_and_tie_by_pk_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_max()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "a"));
    batch.insert("history", history_values(1, 10, 2, "b"));
    batch.insert("history", history_values(1, 9, 9, "c"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 2, "b"), 1)]
    );
}

#[test]
fn arg_min_by_hydrates_initial_snapshot_winner() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "later"));
    batch.insert("history", history_values(1, 10, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).unwrap();

    let subscription = database.subscribe_one_sink(history_arg_min()).unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[0] {
        Value::U64(row) => row,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "winner"), 1),
            (history_values(2, 5, 1, "other"), 1),
        ]
    );
}

#[test]
fn arg_min_by_tracks_lower_insert_and_current_winner_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_min()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "first"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "first"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "lower"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "first"), -1),
            (history_values(1, 10, 1, "lower"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "lower"), -1),
            (history_values(1, 20, 1, "first"), 1),
        ]
    );
}

#[test]
fn arg_min_by_handles_same_tick_replacement_and_tie_by_pk_order() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database.subscribe_one_sink(history_arg_min()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 2, "old"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 2, "old"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 2));
    batch.insert("history", history_values(1, 10, 1, "replacement"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 2, "old"), -1),
            (history_values(1, 10, 1, "replacement"), 1),
        ]
    );
}

#[test]
fn top_by_hydrates_limit_two() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "third"));
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => stamp,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_finite_zero_stays_empty() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(0))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());
}

#[test]
fn top_by_boundary_insert_and_delete_updates_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 15, 1, "middle"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 15, 1, "middle"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 15, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 15, 1, "middle"), -1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_suppresses_outside_window_changes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "outside"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());
}

#[test]
fn top_by_descending_order_keeps_largest_values() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_desc(2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => std::cmp::Reverse(stamp),
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 30, 1, "third"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_offset_keeps_requested_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc_offset(1, 1))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "second"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 5, 1, "zeroth"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 10, 1, "first"), 1),
        ]
    );
}

#[test]
fn top_by_orders_nullable_sort_keys_null_first() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        TestStorage::open(temp_dir.path().join("groove-test.btree"), &["scores"]).unwrap();
    let mut database = Database::new(nullable_scores_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "scores",
        vec![
            // The non-null row deliberately has the earlier tie key: this
            // verifies NULL ordering rather than accidentally passing through
            // the id tie-breaker.
            Value::U64(1),
            Value::Nullable(Some(Box::new(Value::U64(10)))),
            Value::String("ten".to_owned()),
        ],
    );
    batch.insert(
        "scores",
        vec![
            Value::U64(2),
            Value::Nullable(None),
            Value::String("null".to_owned()),
        ],
    );
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::top_by(
            GraphBuilder::table("scores"),
            std::iter::empty::<&str>(),
            [TopByOrder::asc("score")],
            ["id"],
            0,
            TopByLimit::Finite(1),
        ))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![
                Value::U64(2),
                Value::Nullable(None),
                Value::String("null".to_owned()),
            ],
            1,
        )]
    );
}

#[test]
fn top_by_uses_stable_tie_field() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "rows", "blockers"],
    )
    .unwrap();
    let mut database = Database::new(history_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(1))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 2, "later tie"));
    batch.insert("history", history_values(1, 10, 1, "stable tie"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "stable tie"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 0, "earlier tie"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "stable tie"), -1),
            (history_values(1, 10, 0, "earlier tie"), 1),
        ]
    );
}

fn union_history_top_by(offset: u64, limit: u64) -> GraphBuilder {
    GraphBuilder::top_by(
        GraphBuilder::union([
            GraphBuilder::table("history"),
            GraphBuilder::table("history_shadow"),
        ]),
        ["row"],
        [TopByOrder::asc("stamp")],
        ["node"],
        offset,
        TopByLimit::Finite(limit),
    )
}

#[test]
fn top_by_counts_duplicate_multiplicity_toward_window_occupancy() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "history_shadow"],
    )
    .unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 2))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "first"), 2)]
    );
}

#[test]
fn top_by_offset_splits_duplicate_copies_across_boundary() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "history_shadow"],
    )
    .unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => stamp,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[test]
fn top_by_emits_weighted_diff_when_duplicate_copy_enters_window() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "history_shadow"],
    )
    .unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match values[1] {
        Value::U64(stamp) => stamp,
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 10, 1, "first"), 1),
        ]
    );
}

#[test]
fn top_by_replaces_window_tie_with_distinct_record_on_delete() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "history_shadow"],
    )
    .unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "alpha"));
    batch.insert("history_shadow", history_values(1, 10, 1, "beta"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 1))
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "alpha"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "alpha"), -1),
            (history_values(1, 10, 1, "beta"), 1),
        ]
    );
}

#[test]
fn top_by_maintains_weighted_window_across_duplicate_lifecycle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["history", "history_shadow"],
    )
    .unwrap();
    let mut database = Database::new(two_history_tables_schema(), storage).unwrap();

    // Row-1 partition starts as first×2, second×1, third×1; the offset-1,
    // limit-2 window over the ordinal stream `f f s t` retains one copy of
    // first (straddling the offset) plus second.
    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .unwrap();
    let mut initial = subscription.recv().unwrap().to_values().unwrap();
    initial.sort_by_key(|(values, _)| match (&values[0], &values[1]) {
        (Value::U64(row), Value::U64(stamp)) => (*row, *stamp),
        _ => unreachable!(),
    });
    assert_eq!(
        initial,
        [
            (history_values(1, 10, 1, "first"), 1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );

    // A second partition gets its own window; row-1 must stay silent.
    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 5, 1, "r2a"));
    batch.insert("history", history_values(2, 6, 1, "r2b"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(2, 6, 1, "r2b"), 1)]
    );

    // A duplicate copy of second lands on the window's outer edge: the stream
    // becomes `f f s s t` but the retained ordinals [1, 3) still hold one
    // first and one second, so nothing may emit.
    let mut batch = database.open_batch();
    batch.insert("history_shadow", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).unwrap();
    assert!(subscription.try_recv().is_err());

    // Dropping one copy of first shifts the straddle: `f s s t` retains
    // second twice, so first leaves and second gains a copy.
    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "first"), -1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );

    // A third partition with two distinct records tied on (stamp, node):
    // record bytes order alpha before beta, so [1, 3) retains beta and gamma.
    let mut batch = database.open_batch();
    batch.insert("history", history_values(3, 10, 1, "alpha"));
    batch.insert("history_shadow", history_values(3, 10, 1, "beta"));
    batch.insert("history", history_values(3, 20, 1, "gamma"));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(3, 10, 1, "beta"), 1),
            (history_values(3, 20, 1, "gamma"), 1),
        ]
    );

    // Deleting alpha rebuilds the tie group's before-window from records that
    // share its sort key; beta slides into the offset and leaves the window.
    let mut batch = database.open_batch();
    batch.delete("history", history_key(3, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(3, 10, 1, "beta"), -1)]
    );

    // Deleting first's last copy resurrects it in the before-window from the
    // delta alone; second drops to one retained copy and third re-enters.
    let mut batch = database.open_batch();
    batch.delete("history_shadow", history_key(1, 10, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 30, 1, "third"), 1),
        ]
    );

    // Shrinking below offset + limit: `s s` retains one second only.
    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(1, 30, 1, "third"), -1)]
    );

    // Removing both copies of second in one tick empties the partition.
    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 20, 1));
    batch.delete("history_shadow", history_key(1, 20, 1));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "second"), -1)]
    );

    // The maintained end state must equal a fresh hydration of the same graph.
    let rehydrated = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .unwrap();
    let mut rehydrated_initial = rehydrated.recv().unwrap().to_values().unwrap();
    rehydrated_initial.sort_by_key(|(values, _)| match &values[0] {
        Value::U64(row) => *row,
        _ => unreachable!(),
    });
    assert_eq!(
        rehydrated_initial,
        [
            (history_values(2, 6, 1, "r2b"), 1),
            (history_values(3, 20, 1, "gamma"), 1),
        ]
    );
}
