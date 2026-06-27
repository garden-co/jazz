use std::collections::HashMap;
#[cfg(feature = "test-utils")]
use std::time::Duration;

use crate::catalogue::CatalogueEntry;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
#[cfg(feature = "test-utils")]
use crate::query_manager::query::Query;
#[cfg(feature = "test-utils")]
use crate::query_manager::types::Value;
use crate::query_manager::types::{Schema, SchemaHash};
use crate::schema_manager::encoding::encode_schema;
use crate::storage::Storage;
#[cfg(feature = "test-utils")]
use crate::{DurabilityTier, JazzClient};

#[cfg(feature = "test-utils")]
pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[cfg(feature = "test-utils")]
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[cfg(feature = "test-utils")]
const DEFAULT_QUERY_TIMEOUT: Duration = Duration::from_secs(8);

pub fn persist_test_schema<H: Storage + ?Sized>(storage: &mut H, schema: &Schema) -> SchemaHash {
    let schema_hash = SchemaHash::compute(schema);
    storage
        .upsert_catalogue_entry(&CatalogueEntry {
            object_id: schema_hash.to_object_id(),
            metadata: HashMap::from([
                (
                    MetadataKey::Type.to_string(),
                    ObjectType::CatalogueSchema.to_string(),
                ),
                (MetadataKey::SchemaHash.to_string(), schema_hash.to_string()),
            ]),
            content: encode_schema(schema),
        })
        .expect("test schema should persist to catalogue");
    schema_hash
}

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
