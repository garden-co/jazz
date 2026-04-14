use std::collections::HashMap;

use crate::catalogue::CatalogueEntry;
use crate::commit::CommitId;
use crate::metadata::{MetadataKey, ObjectType};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::{Schema, SchemaHash};
use crate::row_histories::{
    ApplyRowVersionResult, RowHistoryError, StoredRowVersion, apply_row_version,
};
use crate::schema_manager::encoding::encode_schema;
use crate::storage::{
    MemoryStorage, Storage, StorageError, metadata_from_row_locator, row_locator_from_metadata,
};

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
    storage
        .put_metadata(object_id, metadata)
        .expect("test row metadata should persist");
    object_id
}

pub fn put_test_row_metadata<H: Storage>(
    storage: &mut H,
    object_id: ObjectId,
    metadata: HashMap<String, String>,
) {
    storage
        .put_metadata(object_id, metadata)
        .expect("test row metadata should persist");
}

pub fn apply_test_row_version<H: Storage>(
    storage: &mut H,
    object_id: ObjectId,
    branch: impl AsRef<str>,
    row: StoredRowVersion,
) -> Result<ApplyRowVersionResult, RowHistoryError> {
    apply_row_version(
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
        .or_else(|| {
            storage
                .load_metadata(object_id)
                .expect("test metadata lookup should succeed")
                .and_then(|metadata| {
                    row_locator_from_metadata(&metadata)
                        .map(|locator| metadata_from_row_locator(&locator))
                        .or(Some(metadata))
                })
        })
}

pub fn load_test_row_tip_ids<H: Storage>(
    storage: &H,
    object_id: ObjectId,
    branch: impl ToString,
) -> Result<Vec<CommitId>, StorageError> {
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
