#[cfg(feature = "testing")]
use std::time::Duration;

#[cfg(feature = "testing")]
use crate::query::Query;
#[cfg(feature = "testing")]
use crate::tools::object::ObjectId;
#[cfg(feature = "testing")]
use crate::tools::public_api::types::Value;
#[cfg(feature = "testing")]
use crate::tools::{JazzClient, QueryResult, ReadTier};

#[cfg(feature = "testing")]
pub use crate::tools::admin_catalogue_row_format::decode_row;

#[cfg(feature = "testing")]
pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

/// Project ordinary query results into row-ID/value fixtures for assertions.
/// Joined results must be asserted as QueryResult values to retain every source ID.
#[cfg(feature = "testing")]
pub fn ordinary_rows(results: Vec<QueryResult>) -> QueryRows {
    results
        .into_iter()
        .map(|result| {
            let id = result
                .key
                .row_id()
                .expect("expected an ordinary row result; use QueryResult assertions for joins");
            (id, result.into_values())
        })
        .collect()
}

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
pub async fn wait_for_query<T, F>(
    client: &JazzClient,
    query: Query,
    read_tier: ReadTier,
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
            client.query(query.clone(), read_tier),
        )
        .await
        {
            Ok(Ok(rows)) => {
                let rows = ordinary_rows(rows);
                if let Some(value) = check_rows(rows.clone()) {
                    return value;
                }
                last_rows = Some(rows);
                last_error = None;
            }
            Ok(Err(e)) => {
                if crate::debug_env::covered_input_trace() {
                    eprintln!(
                        "JAZZ_COVERED_INPUT_TRACE stage=wait_for_query_error description={description} error={e}"
                    );
                }
                last_error = Some(e.to_string());
            }
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
pub async fn wait_for_query_results<T, F>(
    client: &JazzClient,
    query: Query,
    read_tier: ReadTier,
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
            client.query(query.clone(), read_tier),
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

/// Explicit unrestricted grants for fixtures, never a production default.
#[cfg(any(test, feature = "testing"))]
pub fn allow_all_policies() -> crate::tools::TablePolicies {
    crate::tools::permissions(|p| {
        p.allow_read().where_(crate::tools::policy_expr::always());
        p.allow_insert().where_(crate::tools::policy_expr::always());
        p.allow_update().where_(crate::tools::policy_expr::always());
        p.allow_delete().where_(crate::tools::policy_expr::always());
    })
}

/// Test-only opt-in for fixtures whose subject is unrelated to authorization.
/// Replaces existing policies; apply specific policies after this helper.
#[cfg(any(test, feature = "testing"))]
pub trait AllowAll: Sized {
    fn allow_all(self) -> Self;
}

#[cfg(any(test, feature = "testing"))]
impl AllowAll for crate::tools::TableSchemaBuilder {
    fn allow_all(self) -> Self {
        self.policies(allow_all_policies())
    }
}

#[cfg(any(test, feature = "testing"))]
impl AllowAll for crate::tools::Schema {
    fn allow_all(mut self) -> Self {
        for table in self.values_mut() {
            table.policies = allow_all_policies();
        }
        self
    }
}

#[cfg(any(test, feature = "testing"))]
impl AllowAll for crate::schema::JazzSchema {
    fn allow_all(self) -> Self {
        Self::new(&self.public_schema().clone().allow_all())
            .expect("allow-all fixture policies compile")
    }
}
