//! Validate a complete deployment before publishing its artifacts and activating it.
use super::{ServerRuntimeHandle, ServerState};
pub(crate) use crate::portable::catalogue::DeploymentResponse;
use crate::portable::deployment::{self as portable, Deployment};
pub(crate) use crate::portable::deployment::{DeployBody, DeployError};
#[cfg(any(test, feature = "embedded-server"))]
pub(crate) use crate::portable::deployment::{DeployRequest, DeploySchema};
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

pub(crate) use crate::portable::deployment::DECLARATIONS;

/// Validate and apply a deployment, with the app's encryption declarations
/// when it has them (see [`portable::prepare_encrypted`]). The response
/// carries the deploy's warnings.
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
    // The kept declarations, read once on the store's own thread (a read on
    // another connection fails as locked on a memory store whose connection
    // is inside a write): the checks see one moment of the table.
    let kept_now: std::collections::HashMap<Vec<u8>, Vec<u8>> = match &kept {
        Some(kept) => kept
            .all_on_store(|| {
                #[cfg(any(test, feature = "test"))]
                state.deploy_hooks.kept_read_queued();
            })
            .map_err(DeployError::internal)?
            .into_iter()
            .collect(),
        None => Default::default(),
    };
    let deployment = portable::prepare_encrypted(
        &index,
        body,
        super::catalogue::unix_timestamp_millis(),
        |version| Ok(kept_now.get(version.0.as_slice()).cloned()),
    )?;
    let staged = &deployment.staged;
    // An encrypted app needs the account registry its checks read, as when
    // a runtime opens (`Owner::open`): a deployment that brings one to a
    // runtime without it is refused before anything is applied.
    if let Some(runtime) = state.runtime()
        && deployment.encrypts()
        && !runtime
            .has_account_registry()
            .await
            .map_err(DeployError::internal)?
    {
        return Err(DeployError::invalid(portable::REGISTRY_REQUIRED));
    }
    let kept_values = if kept.is_some() {
        deployment.kept_values()
    } else {
        Vec::new()
    };
    if !staged.response.changed && kept_values.is_empty() {
        // A deployment may have committed before its runtime reloaded the
        // catalogue. Reload on retry even when there is nothing to keep.
        if let Some(runtime) = state.runtime() {
            runtime
                .reload_catalogue()
                .await
                .map_err(DeployError::internal)?;
        }
        return Ok(deployment.staged.response);
    }
    let new_bundle = staged.new_bundle();
    let genesis = deployment.genesis()?;
    let runtime = match state.runtime() {
        Some(runtime) => runtime,
        None => {
            let store = state.store.as_ref().ok_or_else(|| {
                DeployError::unavailable("server runtime storage is not configured")
            })?;
            state
                .catalogue_store
                .check_unchanged(staged)
                .map_err(DeployError::invalid)?;
            let stored = store
                .catalogue()
                .map_err(DeployError::internal)?
                .read()
                .map_err(DeployError::internal)?;
            let change = deployment.catalogue_change(&stored)?;
            // Initialization and publication commit on the real store together.
            // Do not expose a runtime until that transaction has succeeded.
            let started = store.start_deployment(genesis, change, kept_values);
            if let Ok(runtime) = &started {
                *state.core_server_shell.write().unwrap() = Some(runtime.clone());
            }
            state
                .catalogue_store
                .refresh()
                .map_err(DeployError::internal)?;
            started?;
            return Ok(deployment.staged.response);
        }
    };
    state
        .catalogue_store
        .check_unchanged(staged)
        .map_err(DeployError::invalid)?;
    let change = if staged.response.changed {
        change(&runtime, &deployment).await?
    } else {
        // Declarations for an already admitted version need the same
        // stored-column check as those kept during admission.
        Default::default()
    };
    // The deployment's rows and admissions commit together on the store's
    // thread, or not at all. New permissions reach every session at once:
    // with a bundle new to this store, the same job ends the sessions pinned
    // to other bundles, which reconnect and are pinned again by whichever
    // backend serves them now. Publishing a known bundle again (a cold
    // start, a rollback, a deployment running beside one that published the
    // same permissions) ends no one.
    // The first-declared declarations commit with the catalogue, in the one
    // transaction the apply owns: a conflicting kept value refuses the whole
    // deployment before anything is live, however many instances share the
    // store.
    #[cfg(any(test, feature = "test"))]
    state.deploy_hooks.before_apply().await;
    let applied = runtime
        .apply_catalogue_keeping(change, new_bundle, kept_values)
        .await;
    state
        .catalogue_store
        .refresh()
        .map_err(DeployError::internal)?;
    applied?;
    Ok(deployment.staged.response)
}

/// Tests: points where the next deployment lets its test act between its
/// steps, so a race is staged in a fixed order instead of with sleeps.
#[cfg(any(test, feature = "test"))]
#[derive(Default)]
pub(crate) struct DeployHooks {
    kept_read_queued: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
    #[allow(clippy::type_complexity)]
    before_apply: std::sync::Mutex<
        Option<(
            tokio::sync::oneshot::Sender<()>,
            tokio::sync::oneshot::Receiver<()>,
        )>,
    >,
}

#[cfg(any(test, feature = "test"))]
#[cfg_attr(not(test), allow(dead_code))]
impl DeployHooks {
    /// The next deployment's read of the kept declarations signals the
    /// receiver once it is queued on the store's thread, before it waits
    /// for that thread.
    pub(crate) fn signal_kept_read_queued(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (queued, signal) = tokio::sync::oneshot::channel();
        *self.kept_read_queued.lock().unwrap() = Some(queued);
        signal
    }

    /// The next deployment that applies a change on a running runtime
    /// stops after its checks, just before the change applies: it signals
    /// the receiver, and goes on when the sender is sent to or dropped.
    pub(crate) fn pause_before_apply(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (reached, paused) = tokio::sync::oneshot::channel();
        let (resume, resumed) = tokio::sync::oneshot::channel();
        *self.before_apply.lock().unwrap() = Some((reached, resumed));
        (paused, resume)
    }

    fn kept_read_queued(&self) {
        if let Some(queued) = self.kept_read_queued.lock().unwrap().take() {
            let _ = queued.send(());
        }
    }

    async fn before_apply(&self) {
        let pause = self.before_apply.lock().unwrap().take();
        if let Some((reached, resumed)) = pause {
            let _ = reached.send(());
            let _ = resumed.await;
        }
    }
}

/// Publish `source` (a schema carrying its permissions as table policies)
/// as a permissions bundle, as `createJazz` does on boot: no permissions
/// become current. An app that declares encryption sends its declarations
/// (`encryption`), as its deployment does (see [`deploy_body`]): the server
/// derives the system tables' policies into the bundle and keeps the
/// version's declarations. Its hex bundle id.
#[cfg(any(test, feature = "embedded-server"))]
pub(crate) async fn publish_source_bundle(
    state: Arc<ServerState>,
    source: &Schema,
    encryption: Option<jazz::tools::deployment::artifact::DeclaredEncryption>,
) -> Result<String, String> {
    let mut structural = source.clone();
    for table in structural.values_mut() {
        table.policies = TablePolicies::default();
    }
    let hash = jazz::tools::public_schema::SchemaHash::compute(&structural).to_string();
    // Jazz derives its system tables' policies, and a deployment's
    // permissions may not name them: the app's own tables only, as its
    // permissions file has them. A system table that carries policies of
    // its own is still sent, and refused.
    let permissions = source
        .iter()
        .filter(|(name, table)| {
            !jazz::tools::deployment::artifact::system_tables::is_system_table(name.as_str())
                || table.policies != TablePolicies::default()
        })
        .map(|(name, table)| (*name, table.policies.clone()))
        .collect();
    let mut body: DeployBody = DeployRequest {
        target_schema_hash: hash.clone(),
        schemas: vec![DeploySchema {
            hash,
            schema: structural,
        }],
        migrations: Vec::new(),
        permissions,
        allow_dropping_history: false,
    }
    .into();
    body.encryption = encryption;
    let response = deploy_body(state, body)
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
mod migrations_create_tests;
#[cfg(test)]
mod tests;
