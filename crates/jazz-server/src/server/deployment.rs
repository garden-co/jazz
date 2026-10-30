//! Validate a complete deployment before publishing its artifacts and activating it.
use super::{
    ServerRuntimeHandle, ServerState,
    catalogue::DeploymentResponse,
    routes::http::{PublishMigrationRequest, lower_migration},
};
use crate::runtime::{CatalogueApplyError, StorageConfig};
use axum::http::StatusCode;
use jazz::schema::JazzSchema;
use jazz::tools::{
    deployment::{self, DeploymentError},
    public_schema::{Schema, SchemaHash, TableName, TablePolicies},
};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeployRequest {
    pub target_schema_hash: String,
    pub schemas: Vec<DeploySchema>,
    pub migrations: Vec<PublishMigrationRequest>,
    pub permissions: HashMap<TableName, TablePolicies>,
    /// Confirms switching existing tables to latest-only history.
    #[serde(default)]
    pub allow_dropping_history: bool,
}
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct DeploySchema {
    pub hash: String,
    pub schema: Schema,
}

pub(crate) struct DeployError {
    pub status: StatusCode,
    pub body: serde_json::Value,
}
impl DeployError {
    fn new(status: StatusCode, code: &str, message: impl ToString) -> Self {
        Self {
            status,
            body: serde_json::json!({"code":code,"error":message.to_string()}),
        }
    }
    fn internal(message: impl ToString) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
    fn invalid(message: impl ToString) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_deployment",
            message,
        )
    }
    fn unavailable(message: impl ToString) -> Self {
        Self::new(
            StatusCode::SERVICE_UNAVAILABLE,
            "deployment_unavailable",
            message,
        )
    }
}
impl From<DeploymentError> for DeployError {
    fn from(error: DeploymentError) -> Self {
        use DeploymentError::*;
        let (code, details) = match &error {
            SchemaHashMismatch { claimed, actual } => (
                "schema_hash_mismatch",
                serde_json::json!({"claimed":claimed.to_string(),"actual":actual.to_string()}),
            ),
            MissingSchema { schema } => (
                "missing_schema",
                serde_json::json!({"schema":schema.to_string()}),
            ),
            InvalidSchema { schema, .. } => (
                "invalid_schema",
                serde_json::json!({"schema":schema.to_string()}),
            ),
            InvalidPermissions { schema, .. } => (
                "invalid_permissions",
                serde_json::json!({"schema":schema.to_string()}),
            ),
            ConflictingMigration { from, to } => (
                "conflicting_migration",
                serde_json::json!({"fromHash":from.to_string(),"toHash":to.to_string()}),
            ),
            InvalidMigration { from, to, .. } => (
                "invalid_migration",
                serde_json::json!({"fromHash":from.to_string(),"toHash":to.to_string()}),
            ),
            Cycle { schemas } => (
                "migration_cycle",
                serde_json::json!({"schemas":schemas.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            ),
            UnreachableTarget { active, target } => (
                "unreachable_deployment_target",
                serde_json::json!({"active":active.to_string(),"target":target.to_string()}),
            ),
            DisconnectedGraph { roots } => (
                "disconnected_migration_graph",
                serde_json::json!({"roots":roots.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            ),
            ConflictingPaths { first, second } => (
                "conflicting_paths",
                serde_json::json!({"first":first.iter().map(ToString::to_string).collect::<Vec<_>>(),"second":second.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            ),
            DropsHistory { target, tables } => (
                "drops_history",
                serde_json::json!({"target":target.to_string(),"tables":tables.iter().map(|t| t.as_str()).collect::<Vec<_>>()}),
            ),
        };
        Self {
            status: StatusCode::UNPROCESSABLE_ENTITY,
            body: serde_json::json!({"code":code,"error":error.to_string(),"details":details}),
        }
    }
}
fn hash(text: &str) -> Result<SchemaHash, DeployError> {
    SchemaHash::from_hex(text).ok_or_else(|| {
        DeployError::new(
            StatusCode::BAD_REQUEST,
            "bad_request",
            format!("invalid schema hash: {text}"),
        )
    })
}
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
    let schemas = request
        .schemas
        .into_iter()
        .map(|entry| Ok((hash(&entry.hash)?, entry.schema)))
        .collect::<Result<Vec<_>, DeployError>>()?;
    let all_schemas = stored
        .schemas
        .iter()
        .map(|schema| (SchemaHash::compute(schema), schema))
        .chain(schemas.iter().map(|(hash, schema)| (*hash, schema)))
        .collect::<HashMap<_, _>>();
    let migrations = request
        .migrations
        .into_iter()
        .map(|migration| {
            let from = hash(&migration.from_hash)?;
            let to = hash(&migration.to_hash)?;
            let source = all_schemas
                .get(&from)
                .ok_or(DeploymentError::MissingSchema { schema: from })?;
            let target = all_schemas
                .get(&to)
                .ok_or(DeploymentError::MissingSchema { schema: to })?;
            lower_migration(migration, source, target).map_err(DeployError::invalid)
        })
        .collect::<Result<Vec<_>, DeployError>>()?;
    let prepared = deployment::prepare_deployment(
        &stored,
        deployment::DeploymentRequest {
            target_schema_hash: hash(&request.target_schema_hash)?,
            schemas,
            migrations,
            permissions: request.permissions,
            allow_dropping_history: request.allow_dropping_history,
        },
    )?;
    let active = state
        .catalogue
        .active_schema_summary(&state.catalogue_store)
        .map_err(DeployError::internal)?;
    let revision = active
        .map(|head| head.version)
        .unwrap_or(0)
        .checked_add(1)
        .ok_or_else(|| DeployError::invalid("deployment revision overflow"))?;
    let staged = state
        .catalogue_store
        .stage_deployment(&prepared, revision)
        .map_err(DeployError::invalid)?;
    if !staged.response.changed {
        return Ok(staged.response);
    }
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
            let applied = match change(&probe, &prepared, staged.put.clone(), revision).await {
                Ok(change) => probe
                    .apply_catalogue(change)
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
    let change = change(&runtime, &prepared, staged.put, revision).await?;
    // The deployment's rows, admissions and activation commit together on
    // the store's thread, or not at all.
    let applied = runtime.apply_catalogue(change).await;
    state
        .catalogue_store
        .refresh()
        .map_err(DeployError::internal)?;
    applied?;
    Ok(staged.response)
}

/// What applying `prepared` to `runtime`'s catalogue changes.
async fn change(
    runtime: &ServerRuntimeHandle,
    prepared: &deployment::PreparedDeployment,
    put: jazz::catalogue::CataloguePut,
    revision: u64,
) -> Result<jazz::catalogue::CatalogueChange, DeployError> {
    let stored = runtime.catalogue().await.map_err(DeployError::internal)?;
    deployment::prepare_catalogue_change(prepared, &stored, put, revision)
        .map_err(DeployError::invalid)
}

impl From<CatalogueApplyError> for DeployError {
    fn from(error: CatalogueApplyError) -> Self {
        match error {
            CatalogueApplyError::Rejected(message) => DeployError::invalid(message),
            CatalogueApplyError::Failed(message) => DeployError::internal(message),
        }
    }
}

#[cfg(test)]
mod tests;
