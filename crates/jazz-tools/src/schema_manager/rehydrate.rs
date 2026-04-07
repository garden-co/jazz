use tracing::{info, warn};

use crate::metadata::{MetadataKey, ObjectType};
use crate::storage::Storage;

use super::encoding::decode_permissions_head;
use super::{AppId, SchemaManager};

fn entry_matches_app(entry: &crate::catalogue::CatalogueEntry, app_id: AppId) -> bool {
    entry.metadata.get(MetadataKey::AppId.as_str()) == Some(&app_id.uuid().to_string())
}

/// Rehydrate server schema state from persisted catalogue rows.
pub fn rehydrate_schema_manager_from_catalogue<S: Storage + ?Sized>(
    schema_manager: &mut SchemaManager,
    storage: &S,
    app_id: AppId,
) -> Result<(), String> {
    let entries = storage
        .scan_catalogue_entries()
        .map_err(|err| format!("failed to scan catalogue entries for app {app_id}: {err:?}"))?;

    let entries: Vec<_> = entries
        .into_iter()
        .filter(|entry| entry_matches_app(entry, app_id))
        .collect();

    let mut schema_count = 0usize;
    let mut permissions_count = 0usize;
    let mut lens_count = 0usize;

    for entry in entries
        .iter()
        .filter(|entry| entry.object_type() == Some(ObjectType::CatalogueSchema.as_str()))
    {
        if let Err(error) = schema_manager.process_catalogue_update(
            entry.object_id,
            &entry.metadata,
            &entry.content,
        ) {
            warn!(
                app_id = %app_id,
                object_id = %entry.object_id,
                ?error,
                "failed to process schema catalogue entry from storage"
            );
        } else {
            schema_count += 1;
        }
    }

    let permissions_head_object_id = SchemaManager::permissions_head_object_id_for(app_id);
    let rehydrated_permissions_head = entries
        .iter()
        .find(|entry| entry.object_id == permissions_head_object_id)
        .and_then(
            |head_entry| match decode_permissions_head(&head_entry.content) {
                Ok((_schema_hash, _version, _parent_bundle_object_id, bundle_object_id)) => {
                    let bundle_entry = entries
                        .iter()
                        .find(|entry| entry.object_id == bundle_object_id)?;
                    if let Err(error) = schema_manager.process_catalogue_update(
                        bundle_entry.object_id,
                        &bundle_entry.metadata,
                        &bundle_entry.content,
                    ) {
                        warn!(
                            app_id = %app_id,
                            object_id = %bundle_entry.object_id,
                            ?error,
                            "failed to process permissions bundle from rehydrated head"
                        );
                        return None;
                    }
                    if let Err(error) = schema_manager.process_catalogue_update(
                        head_entry.object_id,
                        &head_entry.metadata,
                        &head_entry.content,
                    ) {
                        warn!(
                            app_id = %app_id,
                            object_id = %head_entry.object_id,
                            ?error,
                            "failed to process permissions head during rehydrate"
                        );
                        return None;
                    }
                    Some(())
                }
                Err(error) => {
                    warn!(
                        app_id = %app_id,
                        object_id = %head_entry.object_id,
                        ?error,
                        "failed to decode permissions head during rehydrate"
                    );
                    None
                }
            },
        )
        .is_some();

    if rehydrated_permissions_head {
        permissions_count += 1;
    } else {
        for entry in entries
            .iter()
            .filter(|entry| entry.object_type() == Some(ObjectType::CataloguePermissions.as_str()))
        {
            if let Err(error) = schema_manager.process_catalogue_update(
                entry.object_id,
                &entry.metadata,
                &entry.content,
            ) {
                warn!(
                    app_id = %app_id,
                    object_id = %entry.object_id,
                    ?error,
                    "failed to process legacy permissions catalogue entry from storage"
                );
            } else {
                permissions_count += 1;
            }
        }
    }

    for entry in entries
        .iter()
        .filter(|entry| entry.object_type() == Some(ObjectType::CatalogueLens.as_str()))
    {
        if let Err(error) = schema_manager.process_catalogue_update(
            entry.object_id,
            &entry.metadata,
            &entry.content,
        ) {
            warn!(
                app_id = %app_id,
                object_id = %entry.object_id,
                ?error,
                "failed to process lens catalogue entry from storage"
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
        "rehydrated schema manager from catalogue storage"
    );

    Ok(())
}
