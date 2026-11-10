//! Validate a complete deployment before publishing its artifacts and activating it.
use super::{ServerRuntimeHandle, ServerState, catalogue::DeploymentResponse};
use crate::runtime::StorageConfig;
#[cfg(any(test, feature = "embedded-server"))]
pub(crate) use crate::runtime::deployment::DeploySchema;
pub(crate) use crate::runtime::deployment::{DeployError, DeployRequest};
use jazz::schema::JazzSchema;
use jazz::tools::deployment;
#[cfg(any(test, feature = "embedded-server"))]
use jazz::tools::public_schema::{Schema, TablePolicies};
use std::sync::Arc;

pub(crate) async fn deploy(
    state: Arc<ServerState>,
    request: DeployRequest,
) -> Result<DeploymentResponse, DeployError> {
    // This guard belongs to the detached task, so shutdown also waits when
    // the HTTP client disconnects while deployment is still committing.
    let _request = state
        .shutdown
        .try_enter_app_request()
        .ok_or_else(|| DeployError::unavailable("server is shutting down"))?;
    let _publication = state.runtime_catalogue_publication.lock().await;
    if state.shutdown.is_shutting_down() {
        return Err(DeployError::unavailable("server is shutting down"));
    }
    if state.runtime().is_none() && state.store.is_none() {
        return Err(DeployError::unavailable(
            "server runtime storage is not configured",
        ));
    }
    let stored = state
        .catalogue_store
        .deployment_catalogue()
        .map_err(DeployError::internal)?;
    let prepared = crate::runtime::deployment::prepare(&stored, request)?;
    let staged = state
        .catalogue_store
        .stage_deployment(&prepared)
        .map_err(DeployError::invalid)?;
    if !staged.response.changed {
        // Nothing to commit, but the runtime's view of the catalogue may be
        // behind the store's: a deployment that committed and then failed
        // before reloading it is retried here. Reloading is cheap, and a
        // backend publishes its bundle once per boot.
        if let Some(runtime) = state.runtime() {
            runtime
                .reload_catalogue()
                .await
                .map_err(DeployError::internal)?;
        }
        return Ok(staged.response);
    }
    let new_bundle = (!staged.put.bundles.is_empty())
        .then(|| jazz::catalogue::BundleId::from_hex(&staged.response.bundle))
        .flatten();
    let genesis = JazzSchema::new(&prepared.schemas()[0]).map_err(DeployError::invalid)?;
    let runtime = match state.runtime() {
        Some(runtime) => runtime,
        None => {
            // Apply on a scratch store first, so a deployment the catalogue
            // refuses never creates the app's database. The real store
            // starts as empty as the scratch one, so the same change
            // applies there; only a storage failure can still fail it, and
            // then leaves an empty store that the next deployment reuses.
            let probe =
                ServerRuntimeHandle::start_with_storage(genesis.clone(), StorageConfig::InMemory)
                    .map_err(DeployError::internal)?;
            let applied = match change(&probe, &prepared, staged.put.clone()).await {
                Ok(change) => probe
                    .apply_catalogue(change, None)
                    .await
                    .map_err(DeployError::from),
                Err(error) => Err(error),
            };
            probe.shutdown().await.map_err(DeployError::internal)?;
            applied?;
            state
                .start_core_server_shell(genesis)
                .map_err(DeployError::internal)?
        }
    };
    state
        .catalogue_store
        .check_unchanged(&staged)
        .map_err(DeployError::invalid)?;
    let change = change(&runtime, &prepared, staged.put).await?;
    // The deployment's rows and admissions commit together on the store's
    // thread, or not at all. New permissions reach every session at once:
    // with a bundle new to this store, the same job ends the sessions pinned
    // to other bundles, which reconnect and are pinned again by whichever
    // backend serves them now. Publishing a known bundle again (a cold
    // start, a rollback, a deployment running beside one that published the
    // same permissions) ends no one.
    let applied = runtime.apply_catalogue(change, new_bundle).await;
    state
        .catalogue_store
        .refresh()
        .map_err(DeployError::internal)?;
    applied?;
    Ok(staged.response)
}

/// Publish `source` (a schema carrying its permissions as table policies)
/// as a permissions bundle, as `createJazz` does on boot: no permissions
/// become current. Its hex bundle id.
#[cfg(any(test, feature = "embedded-server"))]
pub(crate) async fn publish_source_bundle(
    state: Arc<ServerState>,
    source: &Schema,
) -> Result<String, String> {
    let mut structural = source.clone();
    for table in structural.values_mut() {
        table.policies = TablePolicies::default();
    }
    let hash = jazz::tools::public_schema::SchemaHash::compute(&structural).to_string();
    let permissions = source
        .iter()
        .map(|(name, table)| (*name, table.policies.clone()))
        .collect();
    let response = deploy(
        state,
        DeployRequest {
            target_schema_hash: hash.clone(),
            schemas: vec![DeploySchema {
                hash,
                schema: structural,
            }],
            migrations: Vec::new(),
            permissions,
            allow_dropping_history: false,
        },
    )
    .await
    .map_err(|error| error.body.to_string())?;
    Ok(response.bundle)
}

/// What applying `prepared` to `runtime`'s catalogue changes: its rows,
/// and the admission of its schemas. Nothing becomes current.
async fn change(
    runtime: &ServerRuntimeHandle,
    prepared: &deployment::PreparedDeployment,
    put: jazz::catalogue::CataloguePut,
) -> Result<jazz::catalogue::CatalogueChange, DeployError> {
    let stored = runtime.catalogue().await.map_err(DeployError::internal)?;
    deployment::prepare_catalogue_change(prepared, &stored, put).map_err(DeployError::invalid)
}

#[cfg(test)]
mod tests;

#[cfg(test)]
use {
    http::StatusCode,
    jazz::tools::public_schema::{SchemaHash, TableName},
    std::collections::HashMap,
};
