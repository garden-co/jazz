//! Validate a complete deployment before publishing its artifacts and activating it.
use super::{ServerRuntimeHandle, ServerState};
pub(crate) use crate::portable::catalogue::DeploymentResponse;
#[cfg(any(test, feature = "embedded-server"))]
pub(crate) use crate::portable::deployment::DeploySchema;
use crate::portable::deployment::{self as portable, Deployment};
pub(crate) use crate::portable::deployment::{DeployBody, DeployError, DeployRequest};
use crate::runtime::StorageConfig;
#[cfg(any(test, feature = "embedded-server"))]
use jazz::tools::public_schema::{Schema, TablePolicies};
use std::sync::Arc;
// The HTTP tests (`tests`, through `use super::*`) name these.
#[cfg(test)]
use axum::http::StatusCode;
#[cfg(test)]
use jazz::schema::JazzSchema;
#[cfg(test)]
use jazz::tools::public_schema::{SchemaHash, TableName};
#[cfg(test)]
use std::collections::HashMap;

/// The host table the server keeps each deployed schema version's
/// encryption declarations in, keyed by the version's schema hash, in the
/// form [`jazz::tools::deployment::artifact::encryption::STORED_DECLARATIONS_V1`]:
/// a later deployment of the version may not change them in a way that
/// breaks its sealed cells (migrations §3.1), even though the schema is
/// unchanged.
pub(crate) const DECLARATIONS: jazz::host::HostTable =
    jazz::host::HostTable::new("e2ee_declarations");

pub(crate) async fn deploy(
    state: Arc<ServerState>,
    request: DeployRequest,
) -> Result<DeploymentResponse, DeployError> {
    deploy_body(state, request.into()).await
}

/// [`deploy`] with the app's encryption declarations (see
/// [`portable::prepare_encrypted`]). The response carries the deploy's
/// warnings.
pub(crate) async fn deploy_body(
    state: Arc<ServerState>,
    body: DeployBody,
) -> Result<DeploymentResponse, DeployError> {
    let kept = state
        .store
        .as_ref()
        .map(|store| store.kv(DECLARATIONS))
        .transpose()
        .map_err(DeployError::internal)?;
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
    let index = state
        .catalogue_store
        .index()
        .map_err(DeployError::internal)?;
    let deployment = portable::prepare_encrypted(
        &index,
        body,
        super::catalogue::unix_timestamp_millis(),
        |version| match &kept {
            Some(kept) => kept.get(&version.0),
            None => Ok(None),
        },
    )?;
    let staged = &deployment.staged;
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
        keep_declarations(kept.as_ref(), &deployment.first_declared)?;
        return Ok(deployment.staged.response);
    }
    let new_bundle = staged.new_bundle();
    let genesis = deployment.genesis()?;
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
            let applied = match change(&probe, &deployment).await {
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
        .check_unchanged(staged)
        .map_err(DeployError::invalid)?;
    let change = change(&runtime, &deployment).await?;
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
    keep_declarations(kept.as_ref(), &deployment.first_declared)?;
    Ok(deployment.staged.response)
}

/// Keep the declarations of each version deployed with them for the first
/// time, once the deployment committed. A version whose declarations are
/// kept already keeps them.
fn keep_declarations(
    kept: Option<&crate::kv::HostKv>,
    first: &[(jazz::tools::public_schema::SchemaHash, Vec<u8>)],
) -> Result<(), DeployError> {
    let Some(kept) = kept else {
        return Ok(());
    };
    for (version, stored) in first {
        kept.put_if_absent(&version.0, stored)
            .map_err(DeployError::internal)?;
    }
    Ok(())
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

/// What applying `deployment` to `runtime`'s catalogue changes: its rows,
/// and the admission of its schemas. Nothing becomes current.
async fn change(
    runtime: &ServerRuntimeHandle,
    deployment: &Deployment,
) -> Result<jazz::catalogue::CatalogueChange, DeployError> {
    let stored = runtime.catalogue().await.map_err(DeployError::internal)?;
    deployment.catalogue_change(&stored)
}

#[cfg(test)]
mod encryption_tests;
#[cfg(test)]
mod tests;
