use std::collections::HashMap;

use tracing::{info, warn};

use crate::metadata::{MetadataKey, ObjectType};
use crate::object::ObjectId;
use crate::query_manager::types::QueryBranchRef;
use crate::storage::{CatalogueManifest, Storage};

use super::encoding::decode_permissions_head;
use super::{AppId, SchemaManager};

fn latest_catalogue_content<S: Storage + ?Sized>(
    storage: &S,
    object_id: ObjectId,
) -> Result<Option<Vec<u8>>, String> {
    let branch = super::catalogue_branch_name();
    let loaded = storage
        .load_branch_tips(object_id, &QueryBranchRef::from_branch_name(branch))
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

fn permissions_metadata_for_rehydrate(app_id: AppId, schema_hash: &str) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CataloguePermissions.to_string(),
    );
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
    metadata.insert(MetadataKey::SchemaHash.to_string(), schema_hash.to_string());
    metadata
}

fn permissions_bundle_metadata_for_rehydrate(app_id: AppId) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CataloguePermissionsBundle.to_string(),
    );
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
    metadata
}

fn permissions_head_metadata_for_rehydrate(app_id: AppId) -> HashMap<String, String> {
    let mut metadata = HashMap::new();
    metadata.insert(
        MetadataKey::Type.to_string(),
        ObjectType::CataloguePermissionsHead.to_string(),
    );
    metadata.insert(MetadataKey::AppId.to_string(), app_id.uuid().to_string());
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
        permissions_seen,
        lens_seen,
    } = manifest;

    let mut schema_count = 0usize;
    let mut permissions_count = 0usize;
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

    let permissions_head_object_id = SchemaManager::permissions_head_object_id_for(app_id);
    let rehydrated_permissions_head = if let Some(head_content) =
        latest_catalogue_content(storage, permissions_head_object_id)?
    {
        match decode_permissions_head(&head_content) {
            Ok((_schema_hash, _version, _parent_bundle_object_id, bundle_object_id)) => {
                match latest_catalogue_content(storage, bundle_object_id)? {
                    Some(bundle_content) => {
                        let bundle_metadata = permissions_bundle_metadata_for_rehydrate(app_id);
                        if let Err(error) = schema_manager.process_catalogue_update(
                            bundle_object_id,
                            &bundle_metadata,
                            &bundle_content,
                        ) {
                            warn!(
                                app_id = %app_id,
                                object_id = %bundle_object_id,
                                ?error,
                                "failed to process permissions bundle from rehydrated head"
                            );
                            false
                        } else {
                            let head_metadata = permissions_head_metadata_for_rehydrate(app_id);
                            if let Err(error) = schema_manager.process_catalogue_update(
                                permissions_head_object_id,
                                &head_metadata,
                                &head_content,
                            ) {
                                warn!(
                                    app_id = %app_id,
                                    object_id = %permissions_head_object_id,
                                    ?error,
                                    "failed to process permissions head during rehydrate"
                                );
                                false
                            } else {
                                permissions_count += 1;
                                true
                            }
                        }
                    }
                    None => {
                        warn!(
                            app_id = %app_id,
                            object_id = %bundle_object_id,
                            "catalogue permissions bundle referenced by head missing main branch content"
                        );
                        false
                    }
                }
            }
            Err(error) => {
                warn!(
                    app_id = %app_id,
                    object_id = %permissions_head_object_id,
                    ?error,
                    "failed to decode permissions head during rehydrate"
                );
                false
            }
        }
    } else {
        false
    };

    if !rehydrated_permissions_head {
        for (object_id, schema_hash) in permissions_seen {
            let Some(content) = latest_catalogue_content(storage, object_id)? else {
                warn!(
                    app_id = %app_id,
                    object_id = %object_id,
                    "legacy catalogue permissions in manifest missing main branch content"
                );
                continue;
            };

            let metadata = permissions_metadata_for_rehydrate(app_id, &schema_hash.to_string());
            if let Err(error) =
                schema_manager.process_catalogue_update(object_id, &metadata, &content)
            {
                warn!(
                    app_id = %app_id,
                    object_id = %object_id,
                    ?error,
                    "failed to process legacy permissions catalogue entry from manifest"
                );
            } else {
                permissions_count += 1;
            }
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
        permissions_count,
        lens_count,
        "rehydrated schema manager from catalogue manifest"
    );

    Ok(())
}
