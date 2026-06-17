#![cfg(feature = "test")]

use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{Query, QueryBuilder, Value};

use crate::common::{
    ClientPair, NO_DELTA_WINDOW, QUERY_TIMEOUT, READY_TIMEOUT, TodoSeed, create_todo,
    last_row_bearing_todo_title, last_updated_todo_title, start_local_client, subscription_schema,
    todo_descriptor,
};
use crate::support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, has_removed, has_updated,
    wait_for_query, wait_for_rows, wait_for_subscription_update,
};

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
#[tokio::test]
async fn subscribe_all_emits_add_update_remove_and_tracks_current_results() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();

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
        |log| has_added(log, todo_id),
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

/// Alice seeds three todos before Bob subscribes. Bob subscribes to
/// `priority > 50` and receives only the two matching rows in his initial
/// subscription result.
#[tokio::test]
async fn subscribe_all_only_returns_rows_that_match_query() {
    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
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
        QueryBuilder::new("todos").build(),
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
    let query = QueryBuilder::new("todos")
        .filter_gt("priority", Value::Integer(50))
        .build();
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
            has_added(log, alice_id) && has_added(log, charlie_id) && !has_any_change(log, bob_id)
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
#[tokio::test]
async fn subscription_reflects_final_state_after_rapid_bulk_updates() {
    const RAPID_UPDATES: usize = 500;

    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos").build();
    let runtime_schema = pair
        .subscriber
        .schema()
        .expect("load subscriber runtime schema");
    let descriptor = todo_descriptor(&runtime_schema);

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
        |log| has_added(log, todo_id),
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
            last_updated_todo_title(log, &descriptor, todo_id).as_deref()
                == Some(final_title.as_str())
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    let latest_delta_title = last_updated_todo_title(&log, &descriptor, todo_id);
    assert_eq!(
        latest_delta_title.as_deref(),
        Some(final_title.as_str()),
        "last row-bearing update delta should decode to the final rapid-update title"
    );

    pair.shutdown().await;
}

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
#[tokio::test]
async fn subscribe_all_supports_condition_filters() {
    struct ConditionCase {
        name: &'static str,
        query: Query,
        insert: TodoSeed,
    }

    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
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
            query: QueryBuilder::new("todos")
                .filter_eq("title", Value::Text("eq-hit".to_string()))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_ne("title", Value::Text("blocked".to_string()))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_gt("priority", Value::Integer(10))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_ge("priority", Value::Integer(10))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_lt("priority", Value::Integer(0))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_le("priority", Value::Integer(0))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_is_null("priority")
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_contains("tags", Value::Text("needle".to_string()))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_contains("title", Value::Text("needle".to_string()))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_contains("title", Value::Text(String::new()))
                .build(),
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
            query: QueryBuilder::new("todos")
                .filter_eq("payload", Value::Bytea(vec![1, 2, 3]))
                .build(),
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
            |log| has_added(log, inserted_id),
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

/// Verifies that a rapid burst of local updates still leaves the subscription
/// stream carrying the final row state.
///
/// This test uses a single local client so it isolates the subscription
/// delivery path from server sync ordering.
#[tokio::test]
async fn local_subscription_preserves_final_state_under_rapid_updates() {
    const RAPID_UPDATES: usize = 100;

    let (_temp_dir, client) = start_local_client(subscription_schema()).await;
    let query = QueryBuilder::new("todos").build();
    let runtime_schema = client.schema().expect("load local runtime schema");
    let descriptor = todo_descriptor(&runtime_schema);

    let mut stream = client
        .subscribe(query.clone())
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
        |log| has_added(log, todo_id),
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
            last_row_bearing_todo_title(log, &descriptor, todo_id).as_deref()
                == Some(final_title.as_str())
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;

    let latest_delta_title = last_row_bearing_todo_title(&log, &descriptor, todo_id);
    assert_eq!(
        latest_delta_title.as_deref(),
        Some(final_title.as_str()),
        "last row-bearing local delta should converge to the final title after rapid updates"
    );

    client.shutdown().await.expect("shutdown local client");
}

/// Verifies that a `Bytea` column value, including interior zero bytes, survives
/// the write → sync → subscription delta → query result round-trip unmodified.
///
/// The writer inserts a todo with `payload = [9, 8, 7, 0]`. The subscriber
/// receives the add delta and queries the row. The payload byte sequence must
/// be identical at every stage — zero bytes must not truncate the value.
#[tokio::test]
async fn subscribe_all_preserves_bytea_values() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .filter_eq("title", Value::Text("bytes-hit".to_string()))
        .build();

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
        |log| has_added(log, todo_id),
    )
    .await;

    let row = wait_for_rows(&pair.subscriber, query, "bytea query row", |rows| {
        rows.into_iter().find(|(id, _)| *id == todo_id)
    })
    .await;
    assert_eq!(row.1[5], Value::Bytea(vec![9, 8, 7, 0]));

    pair.shutdown().await;
}

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
#[tokio::test]
async fn subscribe_all_does_not_emit_add_for_non_matching_contains_query() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .filter_contains("title", Value::Text("needle".to_string()))
        .build();

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
        !has_added(&log, inserted_id),
        "non-matching text contains insert should not emit an add delta"
    );

    pair.shutdown().await;
}
