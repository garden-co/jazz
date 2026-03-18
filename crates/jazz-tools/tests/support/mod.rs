use std::future::Future;
use std::time::Duration;

use jazz_tools::{DurabilityTier, JazzClient, ObjectId, Query, Value};

const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(250);
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(8);

pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[allow(dead_code)]
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

    loop {
        if let Ok(Ok(rows)) = tokio::time::timeout(
            DEFAULT_QUERY_TIMEOUT,
            client.query(query.clone(), durability_tier),
        )
        .await
            && let Some(value) = check_rows(rows)
        {
            return value;
        }

        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for {description}");
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}
