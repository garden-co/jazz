//! Shared catalogue publication request validation and error responses.
use super::CatalogueApplyError;
use http::StatusCode;
use jazz::tools::deployment::published::{PublishMigrationRequest, lower_migration};
use jazz::tools::{
    deployment::{self, DeploymentError},
    public_schema::{Schema, SchemaHash, TableName, TablePolicies},
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployRequest {
    pub target_schema_hash: String,
    pub schemas: Vec<DeploySchema>,
    pub migrations: Vec<PublishMigrationRequest>,
    pub permissions: HashMap<TableName, TablePolicies>,
    /// Confirms switching existing tables to latest-only history.
    #[serde(default)]
    pub allow_dropping_history: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeploySchema {
    pub hash: String,
    pub schema: Schema,
}

pub struct DeployError {
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
    pub(crate) fn internal(message: impl ToString) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
    pub(crate) fn invalid(message: impl ToString) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_deployment",
            message,
        )
    }
    #[cfg(feature = "native")]
    pub(crate) fn unavailable(message: impl ToString) -> Self {
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
            DisconnectedGraph { roots } => (
                "disconnected_migration_graph",
                serde_json::json!({"roots":roots.iter().map(ToString::to_string).collect::<Vec<_>>()}),
            ),
            ConflictingPaths {
                first,
                second,
                difference,
            } => (
                "conflicting_paths",
                serde_json::json!({"first":first.iter().map(ToString::to_string).collect::<Vec<_>>(),"second":second.iter().map(ToString::to_string).collect::<Vec<_>>(),"difference":difference}),
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
pub fn prepare(
    stored: &deployment::DeploymentCatalogue,
    request: DeployRequest,
) -> Result<deployment::PreparedDeployment, DeployError> {
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
        stored,
        deployment::DeploymentRequest {
            target_schema_hash: hash(&request.target_schema_hash)?,
            schemas,
            migrations,
            permissions: request.permissions,
            allow_dropping_history: request.allow_dropping_history,
        },
    )?;
    Ok(prepared)
}
impl From<CatalogueApplyError> for DeployError {
    fn from(error: CatalogueApplyError) -> Self {
        match error {
            CatalogueApplyError::Rejected(message) => DeployError::invalid(message),
            CatalogueApplyError::Failed(message) => DeployError::internal(message),
        }
    }
}

impl super::ServerCore {
    /// Publish a complete bundle on the owning database, without native threads
    /// or a second connection. The host must serialize this with other core jobs.
    pub async fn deploy(
        &mut self,
        request: DeployRequest,
        published_at: u64,
    ) -> Result<super::catalogue::DeploymentResponse, DeployError> {
        let rows = self.db.catalogue().map_err(DeployError::internal)?;
        let index = super::catalogue::CatalogueSnapshot::from_rows(&rows, None)
            .map_err(DeployError::internal)?;
        let prepared = prepare(&index.deployment_catalogue(), request)?;
        let (put, response) = index
            .stage_deployment(&prepared, published_at)
            .map_err(DeployError::invalid)?;
        if !response.changed {
            self.reload_catalogue().map_err(DeployError::internal)?;
            return Ok(response);
        }
        let supersedes = (!put.bundles.is_empty())
            .then(|| jazz::catalogue::BundleId::from_hex(&response.bundle))
            .flatten();
        let change = deployment::prepare_catalogue_change(&prepared, &rows, put)
            .map_err(DeployError::invalid)?;
        let (_, result) = self.apply_catalogue(change, supersedes).await;
        result?;
        Ok(response)
    }
}
