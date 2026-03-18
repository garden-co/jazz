use std::future::Future;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    AppContext, DurabilityTier, JazzClient, ObjectId, OrderedRowDelta, Query, QueryBuilder, Schema,
    SubscriptionStream, Value,
};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(8);
#[allow(dead_code)]
const DEFAULT_ROWS_TIMEOUT: Duration = Duration::from_secs(25);
#[allow(dead_code)]
const DEFAULT_STREAM_POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Convenience shape for query results returned by test helpers.
pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[allow(dead_code)]
/// Polls an async predicate until it returns a value or the timeout expires.
///
/// This is the lowest-level waiting primitive used by the test helpers in this
/// module.
pub async fn wait_for<T, F, Fut>(
    timeout: Duration,
    description: impl Into<String>,
    mut check: F,
) -> T
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Option<T>>,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if let Some(value) = check().await {
            return value;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {description}");
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

/// Re-runs a query until its rows satisfy the provided matcher or the timeout
/// expires.
///
/// Per-attempt query timeouts and transient query errors are retried until the
/// outer deadline is reached.
pub async fn wait_for_query<T, F>(
    client: &JazzClient,
    query: Query,
    durability_tier: Option<DurabilityTier>,
    timeout: Duration,
    description: impl Into<String>,
    mut check_rows: F,
) -> T
where
    F: FnMut(QueryRows) -> Option<T>,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + timeout;

    let mut last_error: Option<String> = None;

    loop {
        match tokio::time::timeout(
            DEFAULT_QUERY_TIMEOUT,
            client.query(query.clone(), durability_tier),
        )
        .await
        {
            Ok(Ok(rows)) => {
                if let Some(value) = check_rows(rows) {
                    return value;
                }
                last_error = None;
            }
            Ok(Err(e)) => last_error = Some(e.to_string()),
            Err(_) => {} // per-attempt timeout, will retry
        }

        if tokio::time::Instant::now() >= deadline {
            match last_error {
                Some(e) => panic!("timed out waiting for {description}: last query error: {e}"),
                None => panic!("timed out waiting for {description}"),
            }
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

#[allow(dead_code)]
/// Waits until a trivial EdgeServer query against `table` succeeds.
///
/// Tests use this after connecting a client so subscription and query checks do
/// not race the initial schema/catalogue sync.
pub async fn wait_for_edge_query_ready(client: &JazzClient, table: &str, timeout: Duration) {
    wait_for_query(
        client,
        QueryBuilder::new(table).build(),
        Some(DurabilityTier::EdgeServer),
        timeout,
        format!("EdgeServer query readiness for {table}"),
        |_| Some(()),
    )
    .await;
}

#[allow(dead_code)]
/// Connects a test client using the default testing context and waits for the
/// requested table to become queryable on the edge server.
///
/// This keeps the testing server's backend/admin secrets intact, which is
/// appropriate for general integration tests that are not asserting user policy
/// enforcement.
pub async fn connect_admin_client(
    server: &TestingServer,
    schema: Schema,
    user_id: &str,
    ready_table: &str,
    ready_timeout: Duration,
) -> JazzClient {
    connect_client_with_context(
        server.make_client_context_for_user(schema, user_id),
        ready_table,
        ready_timeout,
    )
    .await
}

#[allow(dead_code)]
/// Connects a test client using only its JWT session and waits for the
/// requested table to become queryable on the edge server.
///
/// This strips the testing server's backend/admin secrets so policy tests
/// exercise the same authorization path as a normal user client.
pub async fn connect_client(
    server: &TestingServer,
    schema: Schema,
    user_id: &str,
    ready_table: &str,
    ready_timeout: Duration,
) -> JazzClient {
    let mut context = server.make_client_context_for_user(schema, user_id);
    context.backend_secret = None;
    context.admin_secret = None;

    connect_client_with_context(context, ready_table, ready_timeout).await
}

#[allow(dead_code)]
/// Shared implementation for test client connection helpers.
async fn connect_client_with_context(
    context: AppContext,
    ready_table: &str,
    ready_timeout: Duration,
) -> JazzClient {
    let client = JazzClient::connect(context)
        .await
        .expect("connect test client");
    wait_for_edge_query_ready(&client, ready_table, ready_timeout).await;
    client
}

#[allow(dead_code)]
/// Re-runs an EdgeServer query until its rows satisfy the matcher, using the
/// module's default row timeout.
pub async fn wait_for_rows<T, F>(
    client: &JazzClient,
    query: Query,
    description: impl Into<String>,
    check_rows: F,
) -> T
where
    F: FnMut(QueryRows) -> Option<T>,
{
    wait_for_query(
        client,
        query,
        Some(DurabilityTier::EdgeServer),
        DEFAULT_ROWS_TIMEOUT,
        description,
        check_rows,
    )
    .await
}

#[allow(dead_code)]
/// Reads subscription deltas until the accumulated log satisfies the provided
/// predicate or the timeout expires.
///
/// The matching delta is appended to `log` before the predicate is checked
/// again, so callers can assert against the full sequence of observed changes.
pub async fn wait_for_subscription_update<F>(
    stream: &mut SubscriptionStream,
    log: &mut Vec<OrderedRowDelta>,
    timeout: Duration,
    description: impl Into<String>,
    mut predicate: F,
) where
    F: FnMut(&[OrderedRowDelta]) -> bool,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + timeout;

    loop {
        if predicate(log) {
            return;
        }

        let now = tokio::time::Instant::now();
        if now >= deadline {
            panic!("timed out waiting for {description}");
        }

        let delta = tokio::time::timeout(deadline - now, stream.next())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {description}"))
            .unwrap_or_else(|| {
                panic!("subscription stream closed while waiting for {description}")
            });

        log.push(delta);
    }
}

#[allow(dead_code)]
/// Collects any subscription deltas that arrive within a fixed window.
///
/// This is useful for asserting that no extra updates were broadcast after an
/// operation, while still recording any unexpected deltas for debug output.
pub async fn collect_stream_deltas(
    stream: &mut SubscriptionStream,
    log: &mut Vec<OrderedRowDelta>,
    duration: Duration,
) {
    let deadline = tokio::time::Instant::now() + duration;

    loop {
        let now = tokio::time::Instant::now();
        if now >= deadline {
            return;
        }

        let next_wait = (deadline - now).min(DEFAULT_STREAM_POLL_INTERVAL);
        match tokio::time::timeout(next_wait, stream.next()).await {
            Ok(Some(delta)) => log.push(delta),
            Ok(None) => return,
            Err(_) => continue,
        }
    }
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta contains `id` in its added set.
pub fn has_added(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    log.iter()
        .any(|delta| delta.added.iter().any(|change| change.id == id))
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta contains `id` in its removed set.
pub fn has_removed(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    log.iter()
        .any(|delta| delta.removed.iter().any(|change| change.id == id))
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta contains `id` in its updated set.
pub fn has_updated(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    log.iter()
        .any(|delta| delta.updated.iter().any(|change| change.id == id))
}

#[allow(dead_code)]
/// Returns true if any logged subscription delta references `id` as an add,
/// update, or removal.
pub fn has_any_change(log: &[OrderedRowDelta], id: ObjectId) -> bool {
    has_added(log, id) || has_updated(log, id) || has_removed(log, id)
}
