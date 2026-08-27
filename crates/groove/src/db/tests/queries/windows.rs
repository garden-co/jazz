//! Arg-min/max and ordered-window behavior.

use super::*;

#[futures_test::test]
async fn arg_max_by_hydrates_and_tracks_winner_changes() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "old"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_arg_max())
        .await
        .unwrap();
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
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "winner"), -1),
            (history_values(1, 30, 1, "new"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 30, 1, "new"), -1),
            (history_values(1, 20, 1, "winner"), 1),
        ]
    );
}

#[futures_test::test]
async fn arg_max_by_suppresses_non_winner_and_net_zero_deltas() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_arg_max())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "winner"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "winner"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "loser"));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 1, 1, "temporary"));
    batch.delete("history", history_key(2, 1, 1));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn arg_max_by_handles_multi_delta_same_group_and_tie_by_pk_order() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_arg_max())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "a"));
    batch.insert("history", history_values(1, 10, 2, "b"));
    batch.insert("history", history_values(1, 9, 9, "c"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 2, "b"), 1)]
    );
}

#[futures_test::test]
async fn arg_min_by_hydrates_initial_snapshot_winner() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "later"));
    batch.insert("history", history_values(1, 10, 1, "winner"));
    batch.insert("history", history_values(2, 5, 1, "other"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_arg_min())
        .await
        .unwrap();
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

#[futures_test::test]
async fn arg_min_by_tracks_lower_insert_and_current_winner_delete() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_arg_min())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "first"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "first"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "lower"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "first"), -1),
            (history_values(1, 10, 1, "lower"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "lower"), -1),
            (history_values(1, 20, 1, "first"), 1),
        ]
    );
}

#[futures_test::test]
async fn arg_min_by_handles_same_tick_replacement_and_tie_by_pk_order() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_arg_min())
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 2, "old"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 2, "old"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 2));
    batch.insert("history", history_values(1, 10, 1, "replacement"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 2, "old"), -1),
            (history_values(1, 10, 1, "replacement"), 1),
        ]
    );
}

#[futures_test::test]
async fn top_by_hydrates_limit_two() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "third"));
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .await
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

#[futures_test::test]
async fn top_by_releases_departed_groups_from_runtime_state() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(1))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    for row in 1..=1_024 {
        batch.insert("history", history_values(row, 1, 1, "temporary"));
    }
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();
    assert_eq!(database.ivm_runtime.top_by_retained_group_count(), 1_024);

    let mut batch = database.open_batch();
    for row in 1..=1_024 {
        batch.delete("history", history_key(row, 1, 1));
    }
    database.commit_batch(batch).await.unwrap();
    let _removed = subscription.recv().unwrap();

    assert_eq!(database.ivm_runtime.top_by_retained_group_count(), 0);
}

#[futures_test::test]
async fn unbounded_top_by_propagates_payload_replacement_with_stable_order_key() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "before"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc_unbounded())
        .await
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.update("history", history_values(1, 10, 1, "after"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "before"), -1),
            (history_values(1, 10, 1, "after"), 1),
        ]
    );
}

#[futures_test::test]
async fn finite_top_by_applies_stable_payload_replacement_only_inside_window() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "inside"));
    batch.insert("history", history_values(1, 20, 1, "outside"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(1))
        .await
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.update("history", history_values(1, 10, 1, "inside-after"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "inside"), -1),
            (history_values(1, 10, 1, "inside-after"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.update("history", history_values(1, 20, 1, "outside-after"));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn top_by_finite_zero_stays_empty() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(0))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn top_by_boundary_insert_and_delete_updates_window() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 15, 1, "middle"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 15, 1, "middle"), 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 15, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 15, 1, "middle"), -1),
            (history_values(1, 20, 1, "second"), 1),
        ]
    );
}

#[futures_test::test]
async fn top_by_suppresses_outside_window_changes() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(2))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 30, 1, "outside"));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 30, 1));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());
}

#[futures_test::test]
async fn top_by_descending_order_keeps_largest_values() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_desc(2))
        .await
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

#[futures_test::test]
async fn top_by_offset_keeps_requested_window() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc_offset(1, 1))
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "second"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 5, 1, "zeroth"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 10, 1, "first"), 1),
        ]
    );
}

#[futures_test::test]
async fn top_by_orders_nullable_sort_keys_null_first() {
    let storage = MemoryStorage::new(&["scores"]).expect("valid memory storage families");
    let mut database = Database::new(nullable_scores_schema(), storage)
        .await
        .unwrap();

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
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(GraphBuilder::top_by(
            GraphBuilder::table("scores"),
            std::iter::empty::<&str>(),
            [TopByOrder::asc("score")],
            ["id"],
            0,
            TopByLimit::Finite(1),
        ))
        .await
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

#[futures_test::test]
async fn top_by_uses_stable_tie_field() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let subscription = database
        .subscribe_one_sink(history_top_by_stamp_asc(1))
        .await
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 2, "later tie"));
    batch.insert("history", history_values(1, 10, 1, "stable tie"));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "stable tie"), 1)]
    );

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 0, "earlier tie"));
    database.commit_batch(batch).await.unwrap();
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

#[futures_test::test]
async fn top_by_counts_duplicate_multiplicity_toward_window_occupancy() {
    let storage =
        MemoryStorage::new(&["history", "history_shadow"]).expect("valid memory storage families");
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 2))
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "first"), 2)]
    );
}

#[futures_test::test]
async fn top_by_offset_splits_duplicate_copies_across_boundary() {
    let storage =
        MemoryStorage::new(&["history", "history_shadow"]).expect("valid memory storage families");
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .await
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

#[futures_test::test]
async fn top_by_emits_weighted_diff_when_duplicate_copy_enters_window() {
    let storage =
        MemoryStorage::new(&["history", "history_shadow"]).expect("valid memory storage families");
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 2))
        .await
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
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 20, 1, "second"), -1),
            (history_values(1, 10, 1, "first"), 1),
        ]
    );
}

#[futures_test::test]
async fn top_by_replaces_window_tie_with_distinct_record_on_delete() {
    let storage =
        MemoryStorage::new(&["history", "history_shadow"]).expect("valid memory storage families");
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "alpha"));
    batch.insert("history_shadow", history_values(1, 10, 1, "beta"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(0, 1))
        .await
        .unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(history_values(1, 10, 1, "alpha"), 1)]
    );

    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [
            (history_values(1, 10, 1, "alpha"), -1),
            (history_values(1, 10, 1, "beta"), 1),
        ]
    );
}

#[futures_test::test]
async fn top_by_maintains_weighted_window_across_duplicate_lifecycle() {
    let storage =
        MemoryStorage::new(&["history", "history_shadow"]).expect("valid memory storage families");
    let mut database = Database::new(two_history_tables_schema(), storage)
        .await
        .unwrap();

    // Row-1 partition starts as first×2, second×1, third×1; the offset-1,
    // limit-2 window over the ordinal stream `f f s t` retains one copy of
    // first (straddling the offset) plus second.
    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "first"));
    batch.insert("history_shadow", history_values(1, 10, 1, "first"));
    batch.insert("history", history_values(1, 20, 1, "second"));
    batch.insert("history", history_values(1, 30, 1, "third"));
    database.commit_batch(batch).await.unwrap();

    let subscription = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .await
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
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(2, 6, 1, "r2b"), 1)]
    );

    // A duplicate copy of second lands on the window's outer edge: the stream
    // becomes `f f s s t` but the retained ordinals [1, 3) still hold one
    // first and one second, so nothing may emit.
    let mut batch = database.open_batch();
    batch.insert("history_shadow", history_values(1, 20, 1, "second"));
    database.commit_batch(batch).await.unwrap();
    assert!(subscription.try_recv().is_err());

    // Dropping one copy of first shifts the straddle: `f s s t` retains
    // second twice, so first leaves and second gains a copy.
    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 10, 1));
    database.commit_batch(batch).await.unwrap();
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
    database.commit_batch(batch).await.unwrap();
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
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(3, 10, 1, "beta"), -1)]
    );

    // Deleting first's last copy resurrects it in the before-window from the
    // delta alone; second drops to one retained copy and third re-enters.
    let mut batch = database.open_batch();
    batch.delete("history_shadow", history_key(1, 10, 1));
    database.commit_batch(batch).await.unwrap();
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
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(1, 30, 1, "third"), -1)]
    );

    // Removing both copies of second in one tick empties the partition.
    let mut batch = database.open_batch();
    batch.delete("history", history_key(1, 20, 1));
    batch.delete("history_shadow", history_key(1, 20, 1));
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        subscription.try_recv().unwrap().to_values().unwrap(),
        [(history_values(1, 20, 1, "second"), -1)]
    );

    // The maintained end state must equal a fresh hydration of the same graph.
    let rehydrated = database
        .subscribe_one_sink(union_history_top_by(1, 2))
        .await
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

#[futures_test::test]
async fn top_by_incremental_window_matches_fresh_hydration_across_seeded_updates() {
    let storage = MemoryStorage::new(&["history", "rows", "blockers"])
        .expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();
    let graph = history_top_by_stamp_asc_offset(1, 2);
    let subscription = database.subscribe_one_sink(graph.clone()).await.unwrap();
    assert!(subscription.recv().unwrap().is_empty());
    let mut materialized = std::collections::BTreeMap::new();
    let mut known = std::collections::BTreeMap::<(u64, u64, u64), String>::new();

    // Seed a full window in one partition so the oracle cannot accidentally
    // spend its whole generated trace below the offset boundary.
    let mut batch = database.open_batch();
    for (stamp, title) in [(1, "seed-first"), (2, "seed-second"), (3, "seed-third")] {
        known.insert((1, stamp, 1), title.to_owned());
        batch.insert("history", history_values(1, stamp, 1, title));
    }
    database.commit_batch(batch).await.unwrap();
    apply_top_by_deltas(&mut materialized, subscription.recv().unwrap());
    let mut seed = 0x70_b9_u64;

    for step in 0..72 {
        seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let key = (
            (seed >> 4) % 3 + 1,
            (seed >> 12) % 8 + 1,
            (seed >> 20) % 3 + 1,
        );
        let mut batch = database.open_batch();
        match ((seed >> 28) % 3, known.get(&key)) {
            (0, None) => {
                let title = format!("insert-{step}");
                known.insert(key, title.clone());
                batch.insert("history", history_values(key.0, key.1, key.2, &title));
            }
            (1, Some(_)) => {
                let title = format!("replace-{step}");
                known.insert(key, title.clone());
                batch.update("history", history_values(key.0, key.1, key.2, &title));
            }
            (_, Some(_)) => {
                known.remove(&key);
                batch.delete("history", history_key(key.0, key.1, key.2));
            }
            _ => continue,
        }
        database.commit_batch(batch).await.unwrap();

        while let Ok(deltas) = subscription.try_recv() {
            apply_top_by_deltas(&mut materialized, deltas);
        }

        // A separately hydrated consumer must agree with the explicit reference
        // model, as must the incrementally maintained consumer.
        let hydrated = database.subscribe_one_sink(graph.clone()).await.unwrap();
        let mut expected = std::collections::BTreeMap::new();
        apply_top_by_deltas(&mut expected, hydrated.recv().unwrap());
        assert!(database.unsubscribe(hydrated.id()));
        let oracle = top_by_offset_window_oracle(&known);
        assert_eq!(
            expected, oracle,
            "fresh hydration disagreed with the reference window after seed step {step}"
        );
        assert_eq!(
            materialized, oracle,
            "incremental TopBy differed from fresh hydration after seed step {step}"
        );
    }
}

fn top_by_offset_window_oracle(
    known: &std::collections::BTreeMap<(u64, u64, u64), String>,
) -> std::collections::BTreeMap<(u64, u64, u64, String), i64> {
    let mut per_row = std::collections::BTreeMap::<u64, Vec<(u64, u64, String)>>::new();
    for (&(row, stamp, node), title) in known {
        per_row
            .entry(row)
            .or_default()
            .push((stamp, node, title.clone()));
    }
    let mut window = std::collections::BTreeMap::new();
    for (row, records) in &mut per_row {
        records.sort();
        for (stamp, node, title) in records.iter().skip(1).take(2) {
            window.insert((*row, *stamp, *node, title.clone()), 1);
        }
    }
    window
}

fn apply_top_by_deltas(
    materialized: &mut std::collections::BTreeMap<(u64, u64, u64, String), i64>,
    deltas: RecordDeltas,
) {
    for (values, weight) in deltas.to_values().unwrap() {
        let [
            Value::U64(row),
            Value::U64(stamp),
            Value::U64(node),
            Value::String(title),
        ] = values.as_slice()
        else {
            panic!("expected history delta, got {values:?}");
        };
        *materialized
            .entry((*row, *stamp, *node, title.clone()))
            .or_default() += weight;
    }
    materialized.retain(|_, weight| *weight != 0);
}
