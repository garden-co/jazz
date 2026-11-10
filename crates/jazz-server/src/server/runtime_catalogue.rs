//! Bridge catalogue entries published in-process into the WebSocket-serving
//! core shell (embedded test servers and tests).

use std::collections::HashMap;

use jazz::schema::JazzSchema;

use jazz::tools::public_schema::{Schema, SchemaHash};
use jazz::tools::schema_lens::Lens;
use jazz::tools::schema_lens::runtime::compile_lens;

use super::{ServerRuntimeHandle, ServerState};

/// Publish newly admitted catalogue entries into the active runtime shell.
///
/// The caller persists administrative entries first. This bridge then uses
/// the first schema to bootstrap an absent runtime and admits later schemas
/// with lineage lenses. Nothing becomes current: sessions pin to bundles.
#[cfg(test)]
pub(crate) async fn publish_runtime_catalogue(
    state: &ServerState,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), String> {
    // Keep bridges ordered with every other catalogue change.
    let _publication = state.runtime_catalogue_publication.lock().await;
    publish_runtime_catalogue_locked(state, schemas, lenses).await
}

pub(crate) async fn publish_runtime_catalogue_locked(
    state: &ServerState,
    schemas: &[Schema],
    lenses: &[Lens],
) -> Result<(), String> {
    if state.runtime().is_none() && state.store.is_none() {
        return Ok(());
    }

    let mut shell = state.runtime();
    let supplied_schemas = schemas
        .iter()
        .cloned()
        .map(|schema| (SchemaHash::compute(&schema), schema))
        .collect::<HashMap<_, _>>();
    for schema in schemas {
        let runtime_schema = JazzSchema::new(schema)
            .map_err(|error| format!("convert catalogue schema for runtime: {error}"))?;
        runtime_shell(state, &mut shell, runtime_schema)?;
    }

    for lens in lenses {
        publish_runtime_lens(state, &mut shell, lens, &supplied_schemas).await?;
    }

    Ok(())
}

async fn publish_runtime_lens(
    state: &ServerState,
    shell: &mut Option<ServerRuntimeHandle>,
    lens: &Lens,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
) -> Result<(), String> {
    let source_schema = known_schema(state, supplied_schemas, lens.source_hash)?;
    let target_schema = known_schema(state, supplied_schemas, lens.target_hash)?;
    let runtime_lens = compile_lens(lens, &source_schema, &target_schema)?;
    let initial_schema = JazzSchema::new(&source_schema)
        .map_err(|error| format!("convert lens source schema for runtime: {error}"))?;
    let target_runtime = JazzSchema::new(&target_schema)
        .map_err(|error| format!("convert lens target schema: {error}"))?;
    // Same-version metadata is installed by active-schema selection, not a self-lens.
    if initial_schema.version_id() == target_runtime.version_id() {
        return Ok(());
    }
    let runtime_shell = runtime_shell(state, shell, initial_schema)?;
    runtime_shell
        .publish_schema_with_lens(target_runtime, runtime_lens)
        .await
        .map_err(|error| format!("publish schema lineage to runtime shell: {error}"))?;
    Ok(())
}

fn runtime_shell(
    state: &ServerState,
    shell: &mut Option<ServerRuntimeHandle>,
    initial_schema: jazz::schema::JazzSchema,
) -> Result<ServerRuntimeHandle, String> {
    if let Some(shell) = shell.clone() {
        return Ok(shell);
    }
    let started = state.start_core_server_shell(initial_schema)?;
    *shell = Some(started.clone());
    Ok(started)
}

fn known_schema(
    state: &ServerState,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
    hash: SchemaHash,
) -> Result<Schema, String> {
    known_schema_if_present(state, supplied_schemas, hash)?
        .ok_or_else(|| format!("catalogue schema {hash} is missing"))
}

fn known_schema_if_present(
    state: &ServerState,
    supplied_schemas: &HashMap<SchemaHash, Schema>,
    hash: SchemaHash,
) -> Result<Option<Schema>, String> {
    if let Some(schema) = state
        .catalogue
        .known_schema(&state.catalogue_store, &hash)
        .map_err(|error| format!("read catalogue schema {hash}: {error}"))?
        .or_else(|| supplied_schemas.get(&hash).cloned())
    {
        return Ok(Some(schema));
    }

    let empty_schema = Schema::new();
    if hash == SchemaHash::compute(&empty_schema) {
        return Ok(Some(empty_schema));
    }

    Ok(None)
}
