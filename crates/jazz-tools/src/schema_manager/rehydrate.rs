use std::collections::HashMap;

use tracing::{info, warn};

use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
use crate::storage::{CatalogueManifest, Storage};

use super::{AppId, SchemaManager};

fn latest_catalogue_content<S: Storage + ?Sized>(
    storage: &S,
    object_id: ObjectId,
) -> Result<Option<Vec<u8>>, String> {
    let branch = super::catalogue_branch_name();
    let loaded = storage
        .load_branch_tips(object_id, &branch)
        .map_err(|err| format!("failed to load catalogue object branch {object_id}: {err:?}"))?;

    Ok(loaded.and_then(|branch_data| {
        branch_data
            .tips
            .into_iter()
            .max_by_key(|commit| (commit.timestamp, commit.id()))
            .map(|commit| commit.content)
            .filter(|content| !content.is_empty())
    }))
}

fn schema_metadata_for_rehydrate(app_id: AppId, schema_hash: &str) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueSchema.to_string(),
    );
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
    metadata.insert(MetadataKey::SchemaHash.to_string(), schema_hash.to_string());
    metadata
}

fn lens_metadata_for_rehydrate(
    app_id: AppId,
    source_hash: &str,
    target_hash: &str,
) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CatalogueLens.to_string(),
    );
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
    metadata.insert(MetadataKey::SourceHash.to_string(), source_hash.to_string());
    metadata.insert(MetadataKey::TargetHash.to_string(), target_hash.to_string());
    metadata
}

/// Rehydrate server schema state from persisted catalogue manifest operations.
///
/// This lets a restarted server recover known schemas/lenses before any client
/// re-syncs catalogue objects.
pub fn rehydrate_schema_manager_from_manifest<S: Storage + ?Sized>(
    schema_manager: &mut SchemaManager,
    storage: &S,
    app_id: AppId,
) -> Result<(), String> {
    let manifest = match storage.load_catalogue_manifest(app_id.as_object_id()) {
        Ok(Some(manifest)) => manifest,
        Ok(None) => return Ok(()),
        Err(err) => {
            return Err(format!(
                "failed to load catalogue manifest for app {app_id}: {err:?}"
            ));
        }
    };

    let CatalogueManifest {
        schema_seen,
        lens_seen,
    } = manifest;

    let mut schema_count = 0usize;
    let mut lens_count = 0usize;

    for (object_id, schema_hash) in schema_seen {
        let Some(content) = latest_catalogue_content(storage, object_id)? else {
            warn!(
                app_id = %app_id,
                object_id = %object_id,
                "catalogue schema in manifest missing main branch content"
            );
            continue;
        };

        let metadata = schema_metadata_for_rehydrate(app_id, &schema_hash.to_string());
        if let Err(error) = schema_manager.process_catalogue_update(object_id, &metadata, &content)
        {
            warn!(
                app_id = %app_id,
                object_id = %object_id,
                ?error,
                "failed to process schema catalogue entry from manifest"
            );
        } else {
            schema_count += 1;
        }
    }

    for (object_id, lens) in lens_seen {
        let Some(content) = latest_catalogue_content(storage, object_id)? else {
            warn!(
                app_id = %app_id,
                object_id = %object_id,
                "catalogue lens in manifest missing main branch content"
            );
            continue;
        };

        let metadata = lens_metadata_for_rehydrate(
            app_id,
            &lens.source_hash.to_string(),
            &lens.target_hash.to_string(),
        );
        if let Err(error) = schema_manager.process_catalogue_update(object_id, &metadata, &content)
        {
            warn!(
                app_id = %app_id,
                object_id = %object_id,
                ?error,
                "failed to process lens catalogue entry from manifest"
            );
        } else {
            lens_count += 1;
        }
    }

    info!(
        app_id = %app_id,
        schema_count,
        lens_count,
        "rehydrated schema manager from catalogue manifest"
    );

    Ok(())
}
