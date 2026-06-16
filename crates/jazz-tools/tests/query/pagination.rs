#![cfg(feature = "test")]

use jazz_tools::server::TestingServer;
use jazz_tools::{QueryBuilder, Value};

use crate::common::{
    ClientPair, NO_DELTA_WINDOW, QUERY_TIMEOUT, READY_TIMEOUT, TodoSeed, create_todo,
    subscription_schema,
};
use crate::support::{
    TestingClient, collect_stream_deltas, has_added, has_any_change, wait_for_rows,
    wait_for_subscription_update,
};

/// Verifies that subscriptions with both `OFFSET` and `LIMIT`
/// only return rows for that page.
///
/// ```text
/// alice ──insert priorities [1, 2, 3, 4]──► server
/// bob   ──subscribe order asc offset 2 limit 1──► stream add C
/// ```
#[tokio::test]
async fn subscribe_all_cold_ordered_subscription_supports_offset_and_limit() {
    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("cold-page-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let a_id = create_todo(
        &writer,
        TodoSeed {
            title: "A",
            done: false,
            priority: Some(1),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let b_id = create_todo(
        &writer,
        TodoSeed {
            title: "B",
            done: false,
            priority: Some(2),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let c_id = create_todo(
        &writer,
        TodoSeed {
            title: "C",
            done: false,
            priority: Some(3),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let d_id = create_todo(
        &writer,
        TodoSeed {
            title: "D",
            done: false,
            priority: Some(4),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &writer,
        QueryBuilder::new("todos").build(),
        "writer sees all rows before cold paginated subscriber connects",
        |rows| {
            (rows.iter().any(|(id, _)| *id == a_id)
                && rows.iter().any(|(id, _)| *id == b_id)
                && rows.iter().any(|(id, _)| *id == c_id)
                && rows.iter().any(|(id, _)| *id == d_id))
            .then_some(rows)
        },
    )
    .await;

    let subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("cold-page-subscriber")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("todos")
        .order_by("priority")
        .offset(2)
        .limit(1)
        .build();
    let mut stream = subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to cold paginated query");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "cold paginated subscription receives requested row",
        |log| {
            has_added(log, c_id)
                && !has_any_change(log, a_id)
                && !has_any_change(log, b_id)
                && !has_any_change(log, d_id)
        },
    )
    .await;

    let rows = wait_for_rows(
        &subscriber,
        query,
        "cold paginated query result contains only the requested row",
        |rows| (rows.len() == 1 && rows[0].0 == c_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("C".to_string()));
    assert_eq!(rows[0].1[2], Value::Integer(3));

    writer.shutdown().await.expect("shutdown writer");
    subscriber.shutdown().await.expect("shutdown subscriber");
    server.shutdown().await;
}

/// Verifies that a subscription with `OFFSET` but no `LIMIT`
/// returns all rows after the requested offset.
#[tokio::test]
async fn subscribe_all_cold_ordered_subscription_supports_offset_without_limit() {
    let schema = subscription_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("cold-offset-writer")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let a_id = create_todo(
        &writer,
        TodoSeed {
            title: "A",
            done: false,
            priority: Some(1),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;
    let b_id = create_todo(
        &writer,
        TodoSeed {
            title: "B",
            done: false,
            priority: Some(2),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;
    let c_id = create_todo(
        &writer,
        TodoSeed {
            title: "C",
            done: false,
            priority: Some(3),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;
    let d_id = create_todo(
        &writer,
        TodoSeed {
            title: "D",
            done: false,
            priority: Some(4),
            tags: &["offset"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &writer,
        QueryBuilder::new("todos").build(),
        "writer sees all rows before cold offset subscriber connects",
        |rows| {
            (rows.iter().any(|(id, _)| *id == a_id)
                && rows.iter().any(|(id, _)| *id == b_id)
                && rows.iter().any(|(id, _)| *id == c_id)
                && rows.iter().any(|(id, _)| *id == d_id))
            .then_some(rows)
        },
    )
    .await;

    let subscriber = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("cold-offset-subscriber")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;
    let query = QueryBuilder::new("todos")
        .order_by("priority")
        .offset(2)
        .build();
    let mut stream = subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to cold offset-only query");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "cold offset-only subscription receives trailing rows",
        |log| {
            has_added(log, c_id)
                && has_added(log, d_id)
                && !has_any_change(log, a_id)
                && !has_any_change(log, b_id)
        },
    )
    .await;

    let rows = wait_for_rows(
        &subscriber,
        query,
        "cold offset-only query result contains trailing rows",
        |rows| (rows.len() == 2 && rows[0].0 == c_id && rows[1].0 == d_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("C".to_string()));
    assert_eq!(rows[1].1[0], Value::Text("D".to_string()));

    writer.shutdown().await.expect("shutdown writer");
    subscriber.shutdown().await.expect("shutdown subscriber");
    server.shutdown().await;
}

/// Verifies that a live ordered/limited subscription evicts the old tail row
/// when a new row sorts ahead of the existing page.
#[tokio::test]
async fn subscribe_all_sorted_limited_subscription_reorders_when_new_top_row_arrives() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .order_by_desc("priority")
        .limit(2)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to sorted limited query");
    let mut log = Vec::new();

    let alice_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Alice",
            done: false,
            priority: Some(100),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let bob_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Bob",
            done: false,
            priority: Some(50),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let charlie_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Charlie",
            done: false,
            priority: Some(75),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial sorted limited page contains Alice and Charlie",
        |rows| {
            (rows.len() == 2 && rows[0].0 == alice_id && rows[1].0 == charlie_id).then_some(rows)
        },
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    let diana_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "Diana",
            done: false,
            priority: Some(125),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "new top row replaces previous limited page tail",
        |log| {
            log.iter().any(|delta| {
                delta
                    .added
                    .iter()
                    .any(|change| change.id == diana_id && change.index == 0)
                    && delta.removed.iter().any(|change| change.id == charlie_id)
            })
        },
    )
    .await;
    assert!(
        !has_any_change(&log, bob_id),
        "Bob should remain outside the limited page"
    );

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "sorted limited page reorders around the new top row",
        |rows| (rows.len() == 2 && rows[0].0 == diana_id && rows[1].0 == alice_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("Diana".to_string()));
    assert_eq!(rows[1].1[0], Value::Text("Alice".to_string()));

    pair.shutdown().await;
}

/// Verifies that deleting a row before an offset/limited page shifts the live
/// window forward and emits the newly visible row.
#[tokio::test]
async fn subscribe_all_offset_limited_subscription_shifts_window_when_deleting_row_before_window() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .order_by("priority")
        .offset(1)
        .limit(2)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to offset limited query");
    let mut log = Vec::new();

    let a_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "A",
            done: false,
            priority: Some(1),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let b_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "B",
            done: false,
            priority: Some(2),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let c_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "C",
            done: false,
            priority: Some(3),
            tags: &["page"],
            payload: None,
        },
    )
    .await;
    let d_id = create_todo(
        &pair.writer,
        TodoSeed {
            title: "D",
            done: false,
            priority: Some(4),
            tags: &["page"],
            payload: None,
        },
    )
    .await;

    wait_for_rows(
        &pair.subscriber,
        query.clone(),
        "initial offset limited page contains B and C",
        |rows| (rows.len() == 2 && rows[0].0 == b_id && rows[1].0 == c_id).then_some(rows),
    )
    .await;
    collect_stream_deltas(&mut stream, &mut log, NO_DELTA_WINDOW).await;
    log.clear();

    pair.writer
        .delete(a_id)
        .expect("delete row before offset window");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "offset limited page shifts after deleting row before window",
        |log| {
            log.iter().any(|delta| {
                delta.removed.iter().any(|change| change.id == b_id)
                    && delta
                        .added
                        .iter()
                        .any(|change| change.id == d_id && change.index == 1)
            })
        },
    )
    .await;
    assert!(
        !has_any_change(&log, a_id),
        "A was before the window and should not appear in the page delta"
    );

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "offset limited page contains C and D after deleting A",
        |rows| (rows.len() == 2 && rows[0].0 == c_id && rows[1].0 == d_id).then_some(rows),
    )
    .await;
    assert_eq!(rows[0].1[0], Value::Text("C".to_string()));
    assert_eq!(rows[1].1[0], Value::Text("D".to_string()));

    pair.shutdown().await;
}

/// Verifies that a subscription with `ORDER BY DESC`, `OFFSET 1`, and `LIMIT 1`
/// surfaces the correct single row once enough data is inserted to fill the window.
///
/// The writer inserts todos with priorities 1, 2, and 3. Sorted descending that
/// is [3, 2, 1]; offset 1 skips the highest, limit 1 takes the next. The
/// subscriber must eventually see exactly the priority-2 row as the sole result.
#[tokio::test]
async fn subscribe_all_supports_order_by_limit_and_offset() {
    let pair = ClientPair::start().await;
    let query = QueryBuilder::new("todos")
        .order_by_desc("priority")
        .offset(1)
        .limit(1)
        .build();

    let mut stream = pair
        .subscriber
        .subscribe(query.clone())
        .await
        .expect("subscribe to ordered query");
    let mut log = Vec::new();

    create_todo(
        &pair.writer,
        TodoSeed {
            title: "p1",
            done: false,
            priority: Some(1),
            tags: &["x"],
            payload: None,
        },
    )
    .await;
    create_todo(
        &pair.writer,
        TodoSeed {
            title: "p2",
            done: false,
            priority: Some(2),
            tags: &["x"],
            payload: None,
        },
    )
    .await;
    create_todo(
        &pair.writer,
        TodoSeed {
            title: "p3",
            done: false,
            priority: Some(3),
            tags: &["x"],
            payload: None,
        },
    )
    .await;

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        QUERY_TIMEOUT,
        "ordered query delta",
        |log| log.iter().any(|delta| !delta.is_empty()),
    )
    .await;

    let rows = wait_for_rows(
        &pair.subscriber,
        query,
        "ordered page current row",
        |rows| {
            (rows.len() == 1 && rows[0].1.first() == Some(&Value::Text("p2".to_string())))
                .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[0], Value::Text("p2".to_string()));

    pair.shutdown().await;
}
