//! Redelivery of merged row states whose winning-version identity is unchanged.
//!
//! When two writers' concurrent column writes merge into one visible row, the
//! merged content can change while the newest contributing version on the
//! delivery path stays the same (the winner keeps winning). Delivery dedup that
//! keys only on the winning-version identity would then suppress the changed
//! merged row and subscribers would never observe the losing writer's column.
//! These tests pin the contract end to end: a subscriber must observe the final
//! merged row whenever ANY column changed, on the live delivery path and on the
//! reconnect path where known-state declarations drive the dedup.

use jazz_testkit as support;

use std::sync::LazyLock;
use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, ObjectId, SchemaBuilder, SubscriptionStream,
    SubscriptionStreamItem, TableSchema, Value, WriteContext,
};
use jazz_server::JazzServer;
use support::{TestingClient, has_added_id, wait_for_query, wait_for_subscription_update};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

// Merge scenarios are sensitive to scheduler load; serialize the suite the same
// way the other multi-writer merge suites do.
static MERGED_REDELIVERY_SUITE_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn test_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("status", ColumnType::Text)
                .column("assignee", ColumnType::Text),
        )
        .build()
}

async fn connect_writer(server: &JazzServer, user_id: &str) -> JazzClient {
    TestingClient::builder()
        .with_server(server)
        .with_schema(test_schema())
        .with_user_id(user_id)
        .ready_on("tasks", READY_TIMEOUT)
        .connect()
        .await
}

/// Waits until `client`'s subscription-fed local state satisfies the row
/// predicate, re-querying locally after each delivered stream event.
///
/// Querying with the local tier keeps the assertion on the delivery path: a
/// suppressed delivery leaves local state stale and the wait times out, whereas
/// a server-tier query would mask the suppression by re-fetching.
async fn wait_for_delivered_rows<F>(
    client: &JazzClient,
    stream: &mut SubscriptionStream,
    query: Query,
    description: &str,
    mut predicate: F,
) -> Vec<(ObjectId, Vec<Value>)>
where
    F: FnMut(&[(ObjectId, Vec<Value>)]) -> bool,
{
    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        let rows = client
            .query(query.clone(), None)
            .await
            .unwrap_or_else(|error| panic!("local query for {description} failed: {error}"));
        if predicate(&rows) {
            return rows;
        }

        let now = tokio::time::Instant::now();
        assert!(now < deadline, "timed out waiting for {description}");

        let item = tokio::time::timeout(deadline - now, stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for stream event: {description}"))
            .unwrap_or_else(|| {
                panic!("subscription stream closed while waiting for {description}")
            });
        if let SubscriptionStreamItem::Rejected { reason } = item {
            panic!("subscription rejected while waiting for {description}: {reason:?}");
        }
    }
}

/// A subscriber must observe the merged row even though the merge winner is the
/// column writer it already knew about.
///
/// Actors: alice writes `status` repeatedly, bob writes `assignee` once, and
/// charlie only subscribes. Bob's single write commits between two of alice's
/// status writes and alice's later write wins the merge ordering, so the newest
/// version identity on charlie's delivery path is an alice write both before
/// and after the merge; the merged content still changed (bob's column) and
/// must reach charlie.
///
/// ```text
/// alice ──status-1..status-3──► server ──deltas──► charlie (subscribed)
/// bob   ──assignee=assigned───► server      (between alice's writes)
/// alice ──status-4────────────► server      (concurrent with bob, later ts)
///                                 │ per-column merge
///                                 └──deliver──► charlie: (status-4, assigned)
/// ```
#[tokio::test]
async fn concurrent_column_writes_merge_and_reach_a_third_subscriber() {
    tokio::task::LocalSet::new()
        .run_until(concurrent_column_writes_merge_and_reach_a_third_subscriber_impl())
        .await
}

async fn concurrent_column_writes_merge_and_reach_a_third_subscriber_impl() {
    let _suite_guard = MERGED_REDELIVERY_SUITE_LOCK.lock().await;
    let server = JazzServer::start_with_schema(test_schema()).await;

    let alice = connect_writer(&server, "alice-merge-subscriber").await;
    let bob = connect_writer(&server, "bob-merge-subscriber").await;
    let charlie = connect_writer(&server, "charlie-merge-subscriber").await;

    let (task_id, _, _) = alice
        .insert(
            "tasks",
            row_input!("status" => "status-0", "assignee" => "unassigned"),
        )
        .expect("alice creates task");

    let query = Query::from("tasks");

    // Charlie's subscription must be materialized before the writes under test
    // so the assertion exercises post-activation delivery, not the initial
    // snapshot.
    let mut charlie_stream = charlie
        .subscribe(query.clone())
        .await
        .expect("charlie subscribes");
    let mut charlie_log = Vec::new();
    wait_for_subscription_update(
        &mut charlie_stream,
        &mut charlie_log,
        QUERY_TIMEOUT,
        "charlie receives the initial task",
        |log| has_added_id(log, task_id),
    )
    .await;

    // Alice's periodic status writes; bob must know the row and alice's latest
    // status before committing his single assignee write.
    for status in ["status-1", "status-2", "status-3"] {
        alice
            .update(
                task_id,
                vec![("status".to_string(), Value::Text(status.to_string()))],
            )
            .expect("alice status write");
    }
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees status-3 before writing assignee",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == task_id
                && rows[0].1[0] == Value::Text("status-3".to_string()))
            .then_some(())
        },
    )
    .await;

    // Bob commits, and alice immediately writes status again without syncing
    // bob's write: two concurrent heads on different columns, with alice's
    // later write winning the merge ordering.
    bob.update(
        task_id,
        vec![("assignee".to_string(), Value::Text("assigned".to_string()))],
    )
    .expect("bob assignee write");
    alice
        .update(
            task_id,
            vec![("status".to_string(), Value::Text("status-4".to_string()))],
        )
        .expect("alice final status write");

    let delivered = wait_for_delivered_rows(
        &charlie,
        &mut charlie_stream,
        query,
        "charlie observes the merged row with both columns",
        |rows| {
            rows.len() == 1
                && rows[0].0 == task_id
                && rows[0].1
                    == vec![
                        Value::Text("status-4".to_string()),
                        Value::Text("assigned".to_string()),
                    ]
        },
    )
    .await;
    assert_eq!(
        delivered[0].1,
        vec![
            Value::Text("status-4".to_string()),
            Value::Text("assigned".to_string()),
        ],
        "the merged row must carry bob's assignee alongside alice's latest status"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    charlie.shutdown().await.expect("shutdown charlie");
    server.shutdown().await;
}

/// A reconnecting subscriber must receive the merged row even though its
/// known-state declaration already names a version by the merge winner's
/// author.
///
/// Actors: alice and bob write different columns while charlie (persistent
/// storage) is disconnected; the reconnect replay runs through known-state
/// declaration dedup and must still ship the merged content.
///
/// ```text
/// charlie syncs (status-0, unassigned), persists, disconnects
/// bob   ──assignee=assigned──► server
/// alice ──status=status-1────► server   (concurrent, later ts)
///                                │ per-column merge
/// charlie reconnects ──declare known state──► server
///                                └──deliver──► charlie: (status-1, assigned)
/// ```
#[tokio::test]
async fn offline_merge_redelivers_after_reconnect() {
    tokio::task::LocalSet::new()
        .run_until(offline_merge_redelivers_after_reconnect_impl())
        .await
}

async fn offline_merge_redelivers_after_reconnect_impl() {
    let _suite_guard = MERGED_REDELIVERY_SUITE_LOCK.lock().await;
    let server = JazzServer::start_with_schema(test_schema()).await;

    let alice = connect_writer(&server, "alice-offline-merge").await;
    let bob = connect_writer(&server, "bob-offline-merge").await;
    let (charlie_ctx, charlie) = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("charlie-offline-merge")
        .with_persistent_storage()
        .ready_on("tasks", READY_TIMEOUT)
        .connect_with_context()
        .await;

    let (task_id, _, _) = alice
        .insert(
            "tasks",
            row_input!("status" => "status-0", "assignee" => "unassigned"),
        )
        .expect("alice creates task");

    let query = Query::from("tasks");
    for (client, who) in [(&bob, "bob"), (&charlie, "charlie")] {
        wait_for_query(
            client,
            query.clone(),
            Some(DurabilityTier::EdgeServer),
            QUERY_TIMEOUT,
            format!("{who} sees the initial task"),
            |rows| {
                (rows.len() == 1
                    && rows[0].0 == task_id
                    && rows[0].1[0] == Value::Text("status-0".to_string()))
                .then_some(())
            },
        )
        .await;
    }

    charlie.shutdown().await.expect("charlie disconnects");

    // Bob then alice write different columns back to back, without syncing each
    // other, so the server merges two concurrent heads with alice's later
    // status write winning the merge ordering.
    bob.update(
        task_id,
        vec![("assignee".to_string(), Value::Text("assigned".to_string()))],
    )
    .expect("bob assignee write while charlie is offline");
    alice
        .update(
            task_id,
            vec![("status".to_string(), Value::Text("status-1".to_string()))],
        )
        .expect("alice status write while charlie is offline");

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "server holds the merged row before charlie reconnects",
        |rows| {
            (rows.len() == 1
                && rows[0].1
                    == vec![
                        Value::Text("status-1".to_string()),
                        Value::Text("assigned".to_string()),
                    ])
            .then_some(())
        },
    )
    .await;

    let charlie_online = jazz_testkit::connect(charlie_ctx)
        .await
        .expect("charlie reconnects");
    let mut charlie_stream = charlie_online
        .subscribe(query.clone())
        .await
        .expect("charlie resubscribes after reconnect");

    wait_for_delivered_rows(
        &charlie_online,
        &mut charlie_stream,
        query,
        "charlie converges to the merged row after reconnect",
        |rows| {
            rows.len() == 1
                && rows[0].0 == task_id
                && rows[0].1
                    == vec![
                        Value::Text("status-1".to_string()),
                        Value::Text("assigned".to_string()),
                    ]
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    charlie_online.shutdown().await.expect("shutdown charlie");
    server.shutdown().await;
}

/// A write that leaves every user column at its current value still changes row
/// metadata (`$updatedAt`) and must reach subscribers.
///
/// Actors: alice rewrites `status` to the value it already has; charlie's
/// subscription-fed local state must observe the newer `$updatedAt`. Content
/// dedup that drops the delivery because no user column changed would leave
/// charlie's metadata stale.
///
/// ```text
/// alice ──status=status-0 (unchanged value)──► server
///                                                │ $updatedAt advances
/// charlie (subscribed) ◄────────deliver──────────┘
/// ```
#[tokio::test]
async fn same_value_write_still_advances_visible_row_metadata() {
    tokio::task::LocalSet::new()
        .run_until(same_value_write_still_advances_visible_row_metadata_impl())
        .await
}

async fn same_value_write_still_advances_visible_row_metadata_impl() {
    let _suite_guard = MERGED_REDELIVERY_SUITE_LOCK.lock().await;
    let server = JazzServer::start_with_schema(test_schema()).await;

    let alice = connect_writer(&server, "alice-metadata-only").await;
    let charlie = connect_writer(&server, "charlie-metadata-only").await;

    let (task_id, _, _) = alice
        .insert(
            "tasks",
            row_input!("status" => "status-0", "assignee" => "unassigned"),
        )
        .expect("alice creates task");

    let query = Query::from("tasks");
    let metadata_query = Query::from("tasks").select(["$updatedAt"]);

    let mut charlie_stream = charlie
        .subscribe(query.clone())
        .await
        .expect("charlie subscribes");
    let mut charlie_log = Vec::new();
    wait_for_subscription_update(
        &mut charlie_stream,
        &mut charlie_log,
        QUERY_TIMEOUT,
        "charlie receives the initial task",
        |log| has_added_id(log, task_id),
    )
    .await;

    // Snapshot the delivered $updatedAt before the same-value write.
    let initial_rows = charlie
        .query(metadata_query.clone(), None)
        .await
        .expect("charlie reads initial metadata");
    assert_eq!(initial_rows.len(), 1, "charlie holds the delivered task");
    let Value::Timestamp(initial_updated_at) = initial_rows[0].1[0] else {
        panic!("$updatedAt should decode as timestamp");
    };

    // Write contexts and public query provenance both use physical Unix
    // milliseconds. Inject a distinct timestamp so this metadata-only delivery
    // remains observable through `$updatedAt`, regardless of the client's
    // synthetic HLC counter.
    let explicit_updated_at = 1_700_000_000_001;
    alice
        .with_write_context(WriteContext::default().with_updated_at(explicit_updated_at))
        .update(
            task_id,
            vec![("status".to_string(), Value::Text("status-0".to_string()))],
        )
        .expect("alice rewrites status to its current value");

    let deadline = tokio::time::Instant::now() + QUERY_TIMEOUT;
    loop {
        let rows = charlie
            .query(metadata_query.clone(), None)
            .await
            .expect("charlie reads metadata after same-value write");
        if rows.len() == 1 {
            if let Value::Timestamp(updated_at) = rows[0].1[0] {
                if updated_at > initial_updated_at {
                    assert_eq!(
                        updated_at, explicit_updated_at,
                        "$updatedAt exposes the requested physical Unix millisecond"
                    );
                    break;
                }
            }
        }

        let now = tokio::time::Instant::now();
        assert!(
            now < deadline,
            "timed out waiting for charlie to observe the newer $updatedAt"
        );
        let item = tokio::time::timeout(deadline - now, charlie_stream.next())
            .await
            .expect("timed out waiting for the metadata-only delivery")
            .expect("charlie's subscription stream closed");
        if let SubscriptionStreamItem::Rejected { reason } = item {
            panic!("charlie's subscription rejected: {reason:?}");
        }
    }

    alice.shutdown().await.expect("shutdown alice");
    charlie.shutdown().await.expect("shutdown charlie");
    server.shutdown().await;
}

/// A late subscriber that only ever received the merged current row (never the
/// contributing history) must still be able to write to that row, and the write
/// must round-trip to the original writers.
///
/// Actors: alice and bob produce a merged row, then charlie connects fresh,
/// sees only the merged state, updates `status`, and everyone converges.
/// Downstream nodes hold partial history by design; this pins that a write on
/// top of a merged row with an undelivered parent chain is accepted.
///
/// ```text
/// alice ──status=status-1──► server ◄──assignee=assigned── bob
///                              │ merge: (status-1, assigned)
/// charlie connects late ◄──current row only (no history)
/// charlie ──status=status-final──► server ──► alice, bob converge
/// ```
#[tokio::test]
async fn late_subscriber_updates_merged_row_without_full_history() {
    tokio::task::LocalSet::new()
        .run_until(late_subscriber_updates_merged_row_without_full_history_impl())
        .await
}

async fn late_subscriber_updates_merged_row_without_full_history_impl() {
    let _suite_guard = MERGED_REDELIVERY_SUITE_LOCK.lock().await;
    let server = JazzServer::start_with_schema(test_schema()).await;

    let alice = connect_writer(&server, "alice-late-writer").await;
    let bob = connect_writer(&server, "bob-late-writer").await;

    let (task_id, _, _) = alice
        .insert(
            "tasks",
            row_input!("status" => "status-0", "assignee" => "unassigned"),
        )
        .expect("alice creates task");

    let query = Query::from("tasks");
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the initial task",
        |rows| (rows.len() == 1 && rows[0].0 == task_id).then_some(()),
    )
    .await;

    // Concurrent single-column writes create the merged row charlie will later
    // receive as his only state.
    bob.update(
        task_id,
        vec![("assignee".to_string(), Value::Text("assigned".to_string()))],
    )
    .expect("bob assignee write");
    alice
        .update(
            task_id,
            vec![("status".to_string(), Value::Text("status-1".to_string()))],
        )
        .expect("alice status write");

    let merged = vec![
        Value::Text("status-1".to_string()),
        Value::Text("assigned".to_string()),
    ];
    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees the merged row",
        |rows| (rows.len() == 1 && rows[0].1 == merged).then_some(()),
    )
    .await;

    // Charlie comes online after the merge; his store never held the
    // contributing heads, only the merged current row.
    let charlie = connect_writer(&server, "charlie-late-writer").await;
    wait_for_query(
        &charlie,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "charlie sees only the merged current row",
        |rows| (rows.len() == 1 && rows[0].0 == task_id && rows[0].1 == merged).then_some(()),
    )
    .await;

    charlie
        .update(
            task_id,
            vec![(
                "status".to_string(),
                Value::Text("status-final".to_string()),
            )],
        )
        .expect("charlie writes on top of the merged row");

    let expected = vec![
        Value::Text("status-final".to_string()),
        Value::Text("assigned".to_string()),
    ];
    for (client, who) in [(&alice, "alice"), (&bob, "bob"), (&charlie, "charlie")] {
        wait_for_query(
            client,
            query.clone(),
            Some(DurabilityTier::EdgeServer),
            QUERY_TIMEOUT,
            format!("{who} sees charlie's write on the merged row"),
            |rows| (rows.len() == 1 && rows[0].0 == task_id && rows[0].1 == expected).then_some(()),
        )
        .await;
    }

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    charlie.shutdown().await.expect("shutdown charlie");
    server.shutdown().await;
}
