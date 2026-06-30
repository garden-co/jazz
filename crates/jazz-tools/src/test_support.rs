#[cfg(feature = "test-utils")]
use std::time::Duration;

#[cfg(feature = "test-utils")]
use crate::AppId;
use crate::object::ObjectId;
#[cfg(feature = "test-utils")]
use crate::public_api::query::Query;
#[cfg(feature = "test-utils")]
use crate::public_api::types::Value;
#[cfg(feature = "test-utils")]
use crate::public_schema::SchemaHash;
#[cfg(feature = "test-utils")]
use crate::schema_lens::Lens;
#[cfg(feature = "test-utils")]
use crate::server::ServerState;
#[cfg(feature = "test-utils")]
use crate::{DurabilityTier, JazzClient, Schema};

#[cfg(feature = "test-utils")]
pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[cfg(feature = "test-utils")]
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[cfg(feature = "test-utils")]
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(8);

/// Re-runs a query until its rows satisfy the provided matcher or the timeout
/// expires.
///
/// Per-attempt query timeouts and transient query errors are retried until the
/// outer deadline is reached.
#[cfg(feature = "test-utils")]
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
            match last_error {
                Some(e) => panic!("timed out waiting for {description}: last query error: {e}"),
                None => panic!(
                    "timed out waiting for {description}: last rows: {:?}",
                    last_rows
                ),
            }
        }

        tokio::time::sleep(DEFAULT_POLL_INTERVAL).await;
    }
}

/// Publishes schemas and lenses directly into an in-process test server's
/// catalogue store.
///
/// This helper is intentionally scoped to `test-utils`: integration tests need
/// to seed catalogue state before exercising public client behavior, but the
/// catalogue storage itself remains a server-internal implementation detail.
#[cfg(feature = "test-utils")]
pub async fn push_catalogue_in_memory(
    state: std::sync::Arc<ServerState>,
    app_id: AppId,
    env: &str,
    user_branch: &str,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut schema_by_hash: std::collections::HashMap<SchemaHash, &Schema> =
        std::collections::HashMap::with_capacity(schemas.len());
    for schema in schemas {
        schema_by_hash.insert(SchemaHash::compute(schema), schema);
        state
            .catalogue
            .publish_schema(&state.catalogue_store, schema.clone())
            .map_err(|error| format!("publish schema to server catalogue: {error}"))?;
    }

    for lens in lenses {
        let source_schema = schema_by_hash.get(&lens.source_hash).ok_or_else(|| {
            format!(
                "No schema provided for lens source hash {}",
                lens.source_hash
            )
        })?;
        let _ = (source_schema, app_id, env, user_branch);
        state
            .catalogue
            .publish_lens(&state.catalogue_store, lens)
            .map_err(|error| format!("publish lens to server catalogue: {error}"))?;
    }

    state
        .catalogue
        .flush(&state.catalogue_store)
        .map_err(|error| format!("flush server catalogue: {error}"))?;

    Ok(())
}
