use std::time::Duration;

use jazz::db::ReadOpts;
use jazz::query::{OrderDirection, Query, col, contains, eq, gt, gte, is_null, lit, lt, lte, ne};
use jazz::tools::test_support::{disconnect_client, reconnect_client};
use jazz::tools::{DurabilityTier, JazzClient, OrderedRowDelta, ResultKey, Value};
use jazz_server::JazzServer;

use crate::common::{
    ClientPair, NO_DELTA_WINDOW, QUERY_TIMEOUT, READY_TIMEOUT, TodoSeed, create_todo,
    last_row_bearing_todo_title, last_updated_todo_title, subscription_schema,
};
use crate::support::{
    TestingClient, collect_stream_deltas, has_added, has_added_id, has_any_change, has_removed,
    has_updated, wait_for_query, wait_for_rows, wait_for_subscription_update,
};

macro_rules! local_tokio_test {
    ($(#[$attr:meta])* async fn $name:ident() $body:block) => {
        $(#[$attr])*
        #[tokio::test(flavor = "current_thread")]
        async fn $name() {
            tokio::task::LocalSet::new()
                .run_until(async $body)
                .await;
        }
    };
}

/// Reduces the ordered public stream protocol exactly as a consumer does: a
/// frame first removes every changed occurrence, then inserts additions and
/// updates at their new positions. Update indexes are relative to the state
/// before the whole frame, so callers inspect that state before applying it.
fn apply_ordered_delta(order: &mut Vec<ResultKey>, delta: &OrderedRowDelta) {
    order.retain(|current| {
        !delta.added.iter().any(|change| change.id == *current)
            && !delta.updated.iter().any(|change| change.id == *current)
            && !delta.removed.iter().any(|change| change.id == *current)
    });
    let mut placements = delta
        .added
        .iter()
        .map(|change| (&change.id, change.index))
        .chain(
            delta
                .updated
                .iter()
                .map(|change| (&change.id, change.new_index)),
        )
        .collect::<Vec<_>>();
    placements.sort_by_key(|(_, index)| *index);
    for (id, index) in placements {
        order.insert(index.min(order.len()), id.clone());
    }
}

local_tokio_test! {
/// Verifies that a subscription emits add, update, and remove deltas as a row
/// enters, changes within, and leaves the query result set, and that the
/// materialized query result stays consistent throughout.
///
/// The writer creates a todo filtered by `done=false`, updates its title, then
/// marks it done. The subscriber observes the full lifecycle. Setting `done=true`
/// moves the row out of the filter, which must produce a remove delta.
///
/// ```text
/// writer ──insert (done=false)──► server ──► subscriber (add ✓)
/// writer ──update title──────────► server ──► subscriber (update ✓)
/// writer ──update done=true──────► server ──► subscriber (remove ✓)
///                                               query result: empty
/// ```
async fn subscribe_all_emits_add_update_remove_and_tracks_current_results() {
    let pair = ClientPair::start().await;
    let query = jazz::query::Query::from("todos")
        .filter(eq(col("done"), lit(false)));

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to todos");
    let mut log = Vec::new();

    let todo_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "watch-me",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "todo add delta",
        |log| {
            has_added(
                log,
                &[
                    ("title", Value::Text("watch-me".to_owned())),
                    ("done", Value::Boolean(false)),
                ],
            )
        },
    )
    .await;

    pair.writer
        .update(
            todo_id,
            vec![(
                "title".to_string(),
                Value::Text("watch-me-updated".to_string()),
            )],
        )
        .expect("update todo title");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "todo update delta",
        |log| has_updated(log, todo_id),
    )
    .await;

    pair.writer
        .update(todo_id, vec![("done".to_string(), Value::Boolean(true))])
        .expect("mark todo done");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "todo remove delta",
        |log| has_removed(log, todo_id),
    )
    .await;

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "todo removed from current results",
        |rows| (!rows.iter().any(|(id, _)| *id == todo_id)).then_some(rows),
    )
    .await;
    assert!(
        !rows.iter().any(|(id, _)| *id == todo_id),
        "latest query results should no longer include the removed todo"
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Alice seeds three todos before Bob subscribes. Bob subscribes to
/// `priority > 50` and receives only the two matching rows in his initial
/// subscription result.
async fn subscribe_all_only_returns_rows_that_match_query() {
    let schema = subscription_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("cold-filter-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let alice_id = create_todo(
        &writer,
        TodoSeed {
            title: "Alice",
            done: false,
            priority: Some(75),
            tags: &["score"],
            payload: None,
        },
    )
    .await;
    let bob_id = create_todo(
        &writer,
        TodoSeed {
            title: "Bob",
            done: false,
            priority: Some(30),
            tags: &["score"],
            payload: None,
        },
    )
    .await;
    let charlie_id = create_todo(
        &writer,
        TodoSeed {
            title: "Charlie",
            done: false,
            priority: Some(90),
            tags: &["score"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &writer,
        jazz::query::Query::from("todos"),
        "writer sees all seeded todos before cold subscriber connects",
        |rows| {
            (rows.iter().any(|(id, _)| *id == alice_id)
                && rows.iter().any(|(id, _)| *id == bob_id)
                && rows.iter().any(|(id, _)| *id == charlie_id))
            .then_some(rows)
        },
    )
    .await;

    let subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("cold-filter-subscriber")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let query = jazz::query::Query::from("todos")
        .filter(gt(col("priority"), lit(50)));
    let mut stream = subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to cold filtered query");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "cold filtered subscription receives existing matching rows",
        |log| {
            has_added_id(log, alice_id) && has_added_id(log, charlie_id) && !has_any_change(log, bob_id)
        },
    )
    .await;

    let rows = wait_for_rows(
        &subscriber,
        query,
        "cold filtered query result contains only matching rows",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == alice_id)
                && rows.iter().any(|(id, _)| *id == charlie_id)
                && rows.iter().all(|(id, _)| *id != bob_id))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 2);

    writer.shutdown().await.expect("shutdown writer");
    subscriber.shutdown().await.expect("shutdown subscriber");
    server.shutdown().await;
}
}

local_tokio_test! {
async fn subscription_reflects_final_state_after_rapid_bulk_updates() {
    const RAPID_UPDATES: usize = 500;

    let pair = ClientPair::start().await;
    let query = jazz::query::Query::from("todos");

    let todo_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "bulk-000",
            done: false,
            priority: Some(1),
            tags: &["burst"],
            payload: None,
        },
    )
    .await;

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to bulk-updated todo");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial add before rapid updates",
        |log| has_added_id(log, todo_id),
    )
    .await;
    log.clear();

    let final_title = format!("bulk-{RAPID_UPDATES:03}");
    for revision in 1..=RAPID_UPDATES {
        pair.writer
            .update(
                todo_id,
                vec![(
                    "title".to_string(),
                    Value::Text(format!("bulk-{revision:03}")),
                )],
            )
            .expect("apply rapid bulk update");
    }

    let rows = wait_for_rows(
        &pair.subscriber,
        query.clone(),
        format!("subscriber sees final bulk title {final_title}"),
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1.first() == Some(&Value::Text(final_title.clone())))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Value::Text(final_title.clone()));

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "final row-bearing bulk update delta",
        |log| {
            last_updated_todo_title(log, todo_id).as_deref() == Some(final_title.as_str())
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    let latest_delta_title = last_updated_todo_title(&log, todo_id);
    assert_eq!(
        latest_delta_title.as_deref(),
        Some(final_title.as_str()),
        "last row-bearing update delta should decode to the final rapid-update title"
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
async fn reset_replacement_preserves_update_category_and_prior_order() {
    const RAPID_UPDATES: usize = 500;

    let pair = ClientPair::start().await;
    let query = Query::from("todos").order_by("title", OrderDirection::Asc);
    let moving_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "a-moving",
            done: false,
            priority: Some(1),
            tags: &["burst"],
            payload: None,
        },
    )
    .await;
    let anchor_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "m-anchor",
            done: false,
            priority: Some(1),
            tags: &["burst"],
            payload: None,
        },
    )
    .await;

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to ordered todos");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "both initial ordered rows",
        |log| has_added_id(log, moving_id) && has_added_id(log, anchor_id),
    )
    .await;
    let mut facade_order = Vec::new();
    for delta in &log {
        apply_ordered_delta(&mut facade_order, delta);
    }
    assert_eq!(
        facade_order,
        vec![ResultKey::from(moving_id), ResultKey::from(anchor_id)],
        "initial public stream order"
    );
    log.clear();

    for revision in 1..=RAPID_UPDATES {
        let prefix = if revision % 2 == 0 { 'z' } else { 'a' };
        pair.writer
            .update(
                moving_id,
                vec![(
                    "title".to_owned(),
                    Value::Text(format!("{prefix}-{revision:03}")),
                )],
            )
            .expect("apply rapid ordered update");
    }
    let final_title = format!("z-{RAPID_UPDATES:03}");
    let rows = wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "final ordered rows before stream drain",
        |rows| {
            (rows.len() == 2 && rows[0].0 == anchor_id && rows[1].0 == moving_id).then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1].1.first(), Some(&Value::Text(final_title.clone())));
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "final ordered reset replacement",
        |log| last_updated_todo_title(log, moving_id).as_deref() == Some(final_title.as_str()),
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    assert!(
        log.iter().all(|delta| delta.added.is_empty() && delta.removed.is_empty()),
        "a reset replacement of existing occurrences remains an update: {log:#?}"
    );
    let final_update = log
        .iter()
        .rev()
        .flat_map(|delta| delta.updated.iter().rev())
        .find(|update| {
            update.id == moving_id
                && update
                    .row
                    .as_ref()
                    .and_then(|row| row.get("title"))
                    == Some(&Value::Text(final_title.clone()))
        })
        .expect("final moving-row update");
    let mut final_update_prior_index = None;
    for delta in &log {
        if delta.updated.iter().any(|update| {
            update.id == moving_id
                && update
                    .row
                    .as_ref()
                    .and_then(|row| row.get("title"))
                    == Some(&Value::Text(final_title.clone()))
        }) {
            assert!(
                final_update_prior_index.is_none(),
                "final title must have one normalized update frame: {log:#?}"
            );
            final_update_prior_index = facade_order
                .iter()
                .position(|id| *id == moving_id);
        }
        apply_ordered_delta(&mut facade_order, delta);
    }
    assert_eq!(
        final_update.old_index,
        final_update_prior_index.expect("final moving row exists before its update"),
        "normalized update must preserve the public stream's immediately prior position"
    );
    assert_eq!(final_update.new_index, 1);
    assert_eq!(
        facade_order,
        vec![ResultKey::from(anchor_id), ResultKey::from(moving_id)],
        "final normalized stream order"
    );

    pair.shutdown().await;
}
}

local_tokio_test! {
/// A reconnect replaces an already-observed remote result with the authority's
/// full current snapshot. The public stream has no reset bit, so an omitted
/// member must become an ordinary removal rather than remaining stale in the
/// facade's reduction.
///
/// subscriber(initial: keep + removed) ──disconnect──► local stale view
/// writer(delete removed) ──► authority
/// subscriber ──reconnect/reset(keep)──► public `removed`
async fn authoritative_reconnect_reset_omits_prior_facade_member() {
    let pair = ClientPair::start().await;
    let query = Query::from("todos");
    let keep_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "survives-authoritative-reset",
            done: false,
            priority: Some(1),
            tags: &["reset"],
            payload: None,
        },
    )
    .await;
    let removed_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "omitted-by-authoritative-reset",
            done: false,
            priority: Some(1),
            tags: &["reset"],
            payload: None,
        },
    )
    .await;

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe before authoritative reset");
    let mut log = Vec::new();
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial public reduction has both rows",
        |log| has_added_id(log, keep_id) && has_added_id(log, removed_id),
    )
    .await;
    log.clear();

    assert!(
        disconnect_client(&pair.subscriber),
        "detach subscriber before authority changes its result"
    );
    let deleted_tx = pair
        .writer
        .delete(removed_id)
        .expect("delete row at authority")
        .expect("ordinary delete commits immediately");
    pair.writer
        .wait_for_transaction(deleted_tx, DurabilityTier::GlobalServer)
        .await
        .expect("authority accepts deletion before subscriber reconnects");

    assert!(
        reconnect_client(&pair.subscriber)
            .await
            .expect("reconnect preserved subscriber"),
        "subscriber transport reconnects"
    );
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "public reduction removes the member absent from authoritative reset",
        |log| has_removed(log, removed_id),
    )
    .await;
    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "reconnected query replaces stale local membership",
        |rows| {
            (rows.len() == 1 && rows[0].0 == keep_id && rows.iter().all(|(id, _)| *id != removed_id))
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, keep_id);

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that each supported filter operator emits an add delta and returns
/// the inserted row in query results when a matching row is written.
///
/// Operators covered: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `is_null`,
/// `contains` on an array column, `contains` on a text column (substring),
/// `contains` with an empty string (matches all), and `eq` on a bytea column.
///
/// Each case uses a dedicated subscriber so subscriptions are isolated. A shared
/// writer is reused to reduce server startup overhead. The writer's inserts
/// accumulate across cases, so each subscriber's query result may contain rows
/// from earlier cases — assertions only check for the presence of the expected row,
/// not an exact total count.
async fn subscribe_all_supports_condition_filters() {
    struct ConditionCase {
        name: &'static str,
        query: Query,
        insert: TodoSeed,
    }

    let schema = subscription_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("condition-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let cases = vec![
        ConditionCase {
            name: "eq",
            query: jazz::query::Query::from("todos")
                .filter(eq(col("title"), lit("eq-hit"))),
            insert: TodoSeed {
                title: "eq-hit",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "ne",
            query: jazz::query::Query::from("todos")
                .filter(ne(col("title"), lit("blocked"))),
            insert: TodoSeed {
                title: "ne-hit",
                done: false,
                priority: Some(2),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "gt",
            query: jazz::query::Query::from("todos")
                .filter(gt(col("priority"), lit(10))),
            insert: TodoSeed {
                title: "gt-hit",
                done: false,
                priority: Some(11),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "gte",
            query: jazz::query::Query::from("todos")
                .filter(gte(col("priority"), lit(10))),
            insert: TodoSeed {
                title: "gte-hit",
                done: false,
                priority: Some(10),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "lt",
            query: jazz::query::Query::from("todos")
                .filter(lt(col("priority"), lit(0))),
            insert: TodoSeed {
                title: "lt-hit",
                done: false,
                priority: Some(-1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "lte",
            query: jazz::query::Query::from("todos")
                .filter(lte(col("priority"), lit(0))),
            insert: TodoSeed {
                title: "lte-hit",
                done: false,
                priority: Some(0),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "is_null",
            query: jazz::query::Query::from("todos").filter(is_null(col("priority"))),
            insert: TodoSeed {
                title: "null-hit",
                done: false,
                priority: None,
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "contains_array",
            query: jazz::query::Query::from("todos")
                .filter(contains(col("tags"), lit("needle"))),
            insert: TodoSeed {
                title: "contains-array-hit",
                done: false,
                priority: Some(1),
                tags: &["needle", "hay"],
                payload: None,
            },
        },
        ConditionCase {
            name: "contains_text",
            query: jazz::query::Query::from("todos")
                .filter(contains(col("title"), lit("needle"))),
            insert: TodoSeed {
                title: "hay-needle-title",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "contains_text_empty",
            query: jazz::query::Query::from("todos")
                .filter(contains(col("title"), lit(String::new()))),
            insert: TodoSeed {
                title: "any-title",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: None,
            },
        },
        ConditionCase {
            name: "eq_bytea",
            query: jazz::query::Query::from("todos")
                .filter(eq(col("payload"), lit(vec![1_u8, 2, 3]))),
            insert: TodoSeed {
                title: "eq-bytea-hit",
                done: false,
                priority: Some(1),
                tags: &["x"],
                payload: Some(&[1, 2, 3]),
            },
        },
    ];

    for case in cases {
        let subscriber = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id(format!("condition-subscriber-{}", case.name))
            .ready_on("todos", READY_TIMEOUT)
            .connect()
            .await;
        let mut stream = subscriber
            .subscribe(case.query.clone())
            .await
            .expect("subscribe for condition case");
        let mut log = Vec::new();

        let inserted_id = create_todo(&writer, case.insert).await;

        wait_for_subscription_update(
            &mut stream,
            &mut log,
            QUERY_TIMEOUT,
            format!("condition {} add delta", case.name),
            |log| has_added_id(log, inserted_id),
        )
        .await;

        let rows = wait_for_rows(
            &subscriber,
            case.query,
            format!("condition {} query rows", case.name),
            |rows| {
                rows.iter()
                    .any(|(id, _)| *id == inserted_id)
                    .then_some(rows)
            },
        )
        .await;
        assert!(
            rows.iter().any(|(id, _)| *id == inserted_id),
            "condition {} should include the inserted row",
            case.name
        );

        subscriber
            .shutdown()
            .await
            .expect("shutdown condition subscriber");
    }

    writer.shutdown().await.expect("shutdown condition writer");
    server.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that a rapid burst of local updates still leaves the subscription
/// stream carrying the final row state.
///
/// This test uses a single local client so it isolates the subscription
/// delivery path from server sync ordering.
async fn local_subscription_preserves_final_state_under_rapid_updates() {
    const RAPID_UPDATES: usize = 100;

    let client = JazzClient::test_client(subscription_schema()).await;
    let query = jazz::query::Query::from("todos");

    let mut stream = client
        .subscribe_with_opts(query.clone(), ReadOpts::default())
        .await
        .expect("subscribe to local todos");
    let mut log = Vec::new();

    let todo_id = create_todo(
        &client,
        TodoSeed {
            title: "local-bulk-000",
            done: false,
            priority: Some(1),
            tags: &["burst"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "initial local add before rapid updates",
        |log| has_added_id(log, todo_id),
    )
    .await;
    log.clear();

    let final_title = format!("local-bulk-{RAPID_UPDATES:03}");
    for revision in 1..=RAPID_UPDATES {
        client
            .update(
                todo_id,
                vec![(
                    "title".to_string(),
                    Value::Text(format!("local-bulk-{revision:03}")),
                )],
            )
            .expect("apply rapid local update");
        tokio::time::sleep(Duration::from_millis(1)).await;
    }

    let rows = wait_for_query(
        &client,
        query.clone(),
        None,
        QUERY_TIMEOUT,
        format!("local client sees final bulk title {final_title}"),
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1.first() == Some(&Value::Text(final_title.clone())))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Value::Text(final_title.clone()));

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "local stream carries the final row-bearing delta after rapid updates",
        |log| {
            last_row_bearing_todo_title(log, todo_id).as_deref() == Some(final_title.as_str())
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    let latest_delta_title = last_row_bearing_todo_title(&log, todo_id);
    assert_eq!(
        latest_delta_title.as_deref(),
        Some(final_title.as_str()),
        "last row-bearing local delta should converge to the final title after rapid updates"
    );

    client.shutdown().await.expect("shutdown local client");
}
}

local_tokio_test! {
/// Verifies that a `Bytea` column value, including interior zero bytes, survives
/// the write → sync → subscription delta → query result round-trip unmodified.
///
/// The writer inserts a todo with `payload = [9, 8, 7, 0]`. The subscriber
/// receives the add delta and queries the row. The payload byte sequence must
/// be identical at every stage — zero bytes must not truncate the value.
async fn subscribe_all_preserves_bytea_values() {
    let pair = ClientPair::start().await;
    let query = jazz::query::Query::from("todos")
        .filter(eq(col("title"), lit("bytes-hit")));

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to bytea query");
    let mut log = Vec::new();

    let todo_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "bytes-hit",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: Some(&[9, 8, 7, 0]),
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "bytea add delta",
        |log| has_added_id(log, todo_id),
    )
    .await;

    let row = wait_for_rows(&pair.subscriber, query, "bytea query row", |rows| {
        rows.into_iter().find(|(id, _)| *id == todo_id)
    })
    .await;
    assert_eq!(row.1[5], Value::Bytea(vec![9, 8, 7, 0]));

    pair.shutdown().await;
}
}

local_tokio_test! {
/// Verifies that inserting a row whose text value does not contain the filter
/// string does not emit a spurious add delta on a `contains` subscription.
///
/// The writer inserts a todo with a title that does not include "needle". The
/// subscriber's query result must remain empty and no add delta must appear.
/// An EdgeServer query on the subscriber is used as the causal barrier before
/// draining the stream: once the server confirms the empty result set, any
/// notification it was going to send has already been sent or withheld.
///
/// ```text
/// writer ──insert "completely unrelated"──► server
///                                              │
///                              contains("needle") filter ──✗── subscriber stream (no add)
/// ```
async fn subscribe_all_does_not_emit_add_for_non_matching_contains_query() {
    let pair = ClientPair::start().await;
    let query = jazz::query::Query::from("todos")
        .filter(contains(col("title"), lit("needle")));

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to contains query");
    let mut log = Vec::new();

    let inserted_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "completely unrelated",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: None,
        },
    )
    .await;

    // The EdgeServer query returning empty is the causal barrier: by the time
    // the server confirms no matching rows, it has already decided whether to
    // send a subscription notification. The drain then flushes any buffered
    // messages before the negative assertion.
    wait_for_rows(
        &pair.subscriber,
        query,
        "empty contains query results",
        |rows| rows.is_empty().then_some(()),
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    assert!(
        !has_added_id(&log, inserted_id),
        "non-matching text contains insert should not emit an add delta"
    );

    pair.shutdown().await;
}
}
