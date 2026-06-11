use std::collections::HashMap;
#[cfg(feature = "client")]
use std::time::Duration;

use crate::catalogue::CatalogueEntry;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::{BranchName, ObjectId};
#[cfg(feature = "client")]
use crate::query_manager::query::Query;
#[cfg(feature = "client")]
use crate::query_manager::types::Value;
use crate::query_manager::types::{Schema, SchemaHash};
use crate::row_histories::{
    ApplyRowBatchResult, BatchId, RowHistoryError, StoredRowBatch, apply_row_batch,
};
use crate::schema_manager::encoding::encode_schema;
use crate::storage::{
    MemoryStorage, Storage, StorageError, metadata_from_row_locator, row_locator_from_metadata,
};
#[cfg(feature = "client")]
use crate::{DurabilityTier, JazzClient};

#[cfg(feature = "client")]
pub type QueryRows = Vec<(ObjectId, Vec<Value>)>;

#[cfg(feature = "client")]
const DEFAULT_POLL_INTERVAL: Duration = Duration::from_millis(50);

#[cfg(feature = "client")]
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

pub fn seeded_memory_storage(schema: &Schema) -> MemoryStorage {
    let mut storage = MemoryStorage::new();
    persist_test_schema(&mut storage, schema);
    storage
}

pub fn create_test_row<H: Storage>(
    storage: &mut H,
    metadata: Option<HashMap<String, String>>,
) -> ObjectId {
    let object_id = ObjectId::new();
    create_test_row_with_id(storage, object_id, metadata)
}

pub fn create_test_row_with_id<H: Storage>(
    storage: &mut H,
    object_id: ObjectId,
    metadata: Option<HashMap<String, String>>,
) -> ObjectId {
    let metadata = metadata.unwrap_or_default();
    let row_locator = row_locator_from_metadata(&metadata)
        .expect("test rows should provide row-locator metadata");
    storage
        .put_row_locator(object_id, Some(&row_locator))
        .expect("test row locator should persist");
    object_id
}

pub fn put_test_row_metadata<H: Storage>(
    storage: &mut H,
    object_id: ObjectId,
    metadata: HashMap<String, String>,
) {
    let row_locator = row_locator_from_metadata(&metadata)
        .expect("test rows should provide row-locator metadata");
    storage
        .put_row_locator(object_id, Some(&row_locator))
        .expect("test row locator should persist");
}

pub fn apply_test_row_batch<H: Storage>(
    storage: &mut H,
    object_id: ObjectId,
    branch: impl AsRef<str>,
    row: StoredRowBatch,
) -> Result<ApplyRowBatchResult, RowHistoryError> {
    apply_row_batch(
        storage,
        object_id,
        &BranchName::new(branch.as_ref()),
        row,
        &[],
    )
}

pub fn load_test_row_metadata<H: Storage>(
    storage: &H,
    object_id: ObjectId,
) -> Option<HashMap<String, String>> {
    storage
        .load_row_locator(object_id)
        .expect("test row locator lookup should succeed")
        .map(|locator| metadata_from_row_locator(&locator))
}

pub fn load_test_row_tip_ids<H: Storage>(
    storage: &H,
    object_id: ObjectId,
    branch: impl ToString,
) -> Result<Vec<BatchId>, StorageError> {
    let branch = branch.to_string();
    let row_locator = storage.load_row_locator(object_id)?.ok_or_else(|| {
        StorageError::IoError(format!("missing row locator for test row {}", object_id))
    })?;
    let tips = storage.scan_row_branch_tip_ids(row_locator.table.as_str(), &branch, object_id)?;
    if tips.is_empty() {
        return Err(StorageError::IoError(format!(
            "missing row branch tips for test row {} on {}",
            object_id, branch
        )));
    }
    Ok(tips)
}

/// Re-runs a query until its rows satisfy the provided matcher or the timeout
/// expires.
///
/// Per-attempt query timeouts and transient query errors are retried until the
/// outer deadline is reached.
#[cfg(feature = "client")]
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
