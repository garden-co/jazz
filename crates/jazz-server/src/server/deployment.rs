//! Validate a complete deployment before publishing its artifacts and activating it.
use super::{
    ServerRuntimeHandle, ServerState,
    catalogue::DeploymentResponse,
    routes::http::{PublishMigrationRequest, lower_migration},
};
use axum::http::StatusCode;
use jazz::tools::{
    deployment::{self, DeploymentError},
    public_schema::{Schema, SchemaHash, TableName, TablePolicies},
};
use jazz::{protocol::CatalogueSnapshot, schema::JazzSchema, serving::StorageConfig};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct DeployRequest {
    pub target_schema_hash: String,
    pub schemas: Vec<DeploySchema>,
    pub migrations: Vec<PublishMigrationRequest>,
    pub permissions: HashMap<TableName, TablePolicies>,
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
            NonConvergent { target, tips } => (
                "non_convergent_graph",
                serde_json::json!({"target":target.to_string(),"tips":tips.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            ),
            ConflictingPaths { first, second } => (
                "conflicting_paths",
                serde_json::json!({"first":first.iter().map(ToString::to_string).collect::<Vec<_>>(),"second":second.iter().map(ToString::to_string).collect::<Vec<_>>()}),
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
fn genesis(snapshot: &CatalogueSnapshot) -> Result<JazzSchema, String> {
    snapshot
        .schemas
        .iter()
        .find(|schema| {
            !snapshot
                .lineages
                .iter()
                .any(|(_, publication)| publication.schema.id == schema.id)
        })
        .map(|schema| schema.schema.clone())
        .ok_or_else(|| "deployment has no genesis schema".into())
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
    if state.runtime().is_none() && state.core_server_shell_storage_config.is_none() {
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
        },
    )?;
    let runtime = state.runtime();
    let current = match &runtime {
        Some(runtime) => Some(
            runtime
                .trusted_catalogue_snapshot()
                .await
                .map_err(DeployError::internal)?,
        ),
        None => None,
    };
    let active = state
        .catalogue
        .active_schema_summary(&state.catalogue_store)
        .map_err(DeployError::internal)?;
    let revision = active
        .map(|head| head.version)
        .unwrap_or(0)
        .max(
            current
                .as_ref()
                .map(|s| s.current_write_schema.revision)
                .unwrap_or(0),
        )
        .checked_add(1)
        .ok_or_else(|| DeployError::invalid("deployment revision overflow"))?;
    let snapshot = deployment::prepare_runtime_snapshot(&prepared, current, revision)
        .map_err(DeployError::invalid)?;
    let staged = state
        .catalogue_store
        .stage_deployment(&prepared, revision)
        .map_err(DeployError::invalid)?;
    if !staged.response.changed {
        return Ok(staged.response);
    }
    let initial_schema = genesis(&snapshot).map_err(DeployError::invalid)?;
    // This probe performs the same admission planning as activation, without
    // touching the persistent runtime or creating a database on a failed deploy.
    if let Some(runtime) = &runtime {
        runtime
            .validate_deployment_snapshot(snapshot.clone())
            .await
            .map_err(DeployError::invalid)?;
    } else {
        let probe = ServerRuntimeHandle::start_with_storage(
            initial_schema.clone(),
            StorageConfig::InMemory,
            None,
        )
        .map_err(DeployError::internal)?;
        let result = probe.validate_deployment_snapshot(snapshot.clone()).await;
        probe.shutdown().await.map_err(DeployError::internal)?;
        result.map_err(DeployError::invalid)?;
    }
    // All graph, schema, permission, lens, physical mapping, and encoding
    // validation has completed. Cross-store write failure recovery is not
    // implemented here; this flow currently assumes writes succeed.
    let response = state
        .catalogue_store
        .commit_deployment(staged)
        .map_err(DeployError::internal)?;
    let runtime = state
        .start_core_server_shell(initial_schema)
        .map_err(DeployError::internal)?;
    runtime
        .apply_deployment_snapshot(snapshot)
        .await
        .map_err(DeployError::internal)?;
    Ok(response)
}

#[cfg(test)]
mod tests;
