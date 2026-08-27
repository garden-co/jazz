#[cfg(feature = "testing")]
use std::time::Duration;

#[cfg(feature = "testing")]
use crate::query::Query;
use crate::tools::object::ObjectId;
#[cfg(feature = "testing")]
use crate::tools::public_api::types::Value;
#[cfg(feature = "testing")]
use crate::tools::{DurabilityTier, JazzClient};

#[cfg(feature = "testing")]
pub use crate::tools::admin_catalogue_row_format::decode_row;

pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[cfg(feature = "testing")]
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[cfg(feature = "testing")]
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(8);

#[cfg(feature = "testing")]
const DEFAULT_WAIT_TIMEOUT_MULTIPLIER: u32 = 8;

/// Sanctioned test-support reconnect control: mirrors the public client's
/// upstream detach without clearing local known-state or pending writes.
#[cfg(feature = "testing")]
pub fn disconnect_client(client: &JazzClient) -> bool {
    client.disconnect_upstream_for_test()
}

/// Sanctioned test-support reconnect control: reattaches the preserved client
/// state to the original upstream transport.
#[cfg(feature = "testing")]
pub async fn reconnect_client(client: &JazzClient) -> crate::tools::Result<bool> {
    client.reconnect_upstream_for_test().await
}

#[cfg(feature = "testing")]
fn load_tolerant_wait_timeout(timeout: Duration) -> Duration {
    let multiplier = std::env::var("JAZZ_TOOLS_TEST_WAIT_TIMEOUT_MULTIPLIER")
        .ok()
        .and_then(|value| value.parse::<u32>().ok())
        .filter(|value| *value > 0)
        .unwrap_or(DEFAULT_WAIT_TIMEOUT_MULTIPLIER);
    timeout.checked_mul(multiplier).unwrap_or(timeout)
}

/// Re-runs a query until its rows satisfy the provided matcher or the timeout
/// expires.
///
/// Per-attempt query timeouts and transient query errors are retried until the
/// outer deadline is reached.
#[cfg(feature = "testing")]
#[allow(deprecated)] // Intentionally exercises legacy DurabilityTier read controls.
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
    #[cfg(feature = "sync-autopsy")]
    crate::db::sync_autopsy::enable();
    let deadline = tokio::time::Instant::now() + load_tolerant_wait_timeout(timeout);

    let mut last_error: Option<String> = None;
    let mut last_rows: Option<QueryRows> = None;

    loop {
        match tokio::time::timeout(
            DEFAULT_QUERY_TIMEOUT,
            client.query(query.clone(), durability_tier),
        )
        .await
        {
            Ok(Ok(rows)) => {
                if let Some(value) = check_rows(rows.clone()) {
                    return value;
                }
                last_rows = Some(rows);
                last_error = None;
            }
            Ok(Err(e)) => last_error = Some(e.to_string()),
            Err(_) => {}
        }

        if tokio::time::Instant::now() >= deadline {
            #[cfg(feature = "sync-autopsy")]
            let autopsy = crate::db::sync_autopsy::dump();
            #[cfg(not(feature = "sync-autopsy"))]
            let autopsy = String::new();
            match last_error {
                Some(e) => {
                    panic!("timed out waiting for {description}: last query error: {e}\n{autopsy}")
                }
                None => panic!(
                    "timed out waiting for {description}: last rows: {:?}\n{}",
                    last_rows, autopsy
                ),
            }
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

/// Re-runs an identity-bearing query until its ResultKey rows satisfy the matcher.
#[cfg(feature = "testing")]
#[allow(deprecated)] // Intentionally exercises legacy DurabilityTier read controls.
pub async fn wait_for_query_results<T, F>(
    client: &JazzClient,
    query: Query,
    durability_tier: Option<DurabilityTier>,
    timeout: Duration,
    description: impl Into<String>,
    mut check_results: F,
) -> T
where
    F: FnMut(Vec<crate::tools::QueryResult>) -> Option<T>,
{
    let description = description.into();
    let deadline = tokio::time::Instant::now() + load_tolerant_wait_timeout(timeout);
    let mut last_error = None;
    let mut last_results = None;
    loop {
        match tokio::time::timeout(
            DEFAULT_QUERY_TIMEOUT,
            client.query_results(query.clone(), durability_tier),
        )
        .await
        {
            Ok(Ok(results)) => {
                if let Some(value) = check_results(results.clone()) {
                    return value;
                }
                last_results = Some(results);
                last_error = None;
            }
            Ok(Err(error)) => last_error = Some(error.to_string()),
            Err(_) => {}
        }
        if tokio::time::Instant::now() >= deadline {
            match last_error {
                Some(error) => {
                    panic!("timed out waiting for {description}: last query error: {error}")
                }
                None => {
                    panic!("timed out waiting for {description}: last results: {last_results:?}")
                }
            }
        }
        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}
