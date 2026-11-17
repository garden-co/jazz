//! A deployment's pure steps, as every host runs them: validate a deploy
//! request against the catalogue, stage the rows it publishes, and plan
//! the change that admits them into a runtime's catalogue. Locking,
//! committing, and starting or reaching the runtime stay with each host.

use std::collections::HashMap;

use http::StatusCode;
use jazz::catalogue::{CatalogueChange, CatalogueRows};
use jazz::schema::JazzSchema;
use jazz::tools::deployment::published::{PublishMigrationRequest, lower_migration};
use jazz::tools::deployment::{self, DeploymentError, PreparedDeployment};
use jazz::tools::public_schema::{Schema, SchemaHash, TableName, TablePolicies};
use serde::{Deserialize, Serialize};

use super::catalogue::{CatalogueIndex, StagedDeployment};
use crate::runtime::CatalogueApplyError;

/// The deploy route's request body.
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

/// Why a deployment failed: the deploy route's status and JSON body.
#[derive(Debug)]
pub struct DeployError {
    pub status: StatusCode,
    pub body: serde_json::Value,
}

impl DeployError {
    pub fn new(status: StatusCode, code: &str, message: impl ToString) -> Self {
        Self {
            status,
            body: serde_json::json!({"code":code,"error":message.to_string()}),
        }
    }
    pub fn internal(message: impl ToString) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
    }
    pub fn invalid(message: impl ToString) -> Self {
        Self::new(
            StatusCode::UNPROCESSABLE_ENTITY,
            "invalid_deployment",
            message,
        )
    }
    pub fn unavailable(message: impl ToString) -> Self {
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

impl From<CatalogueApplyError> for DeployError {
    fn from(error: CatalogueApplyError) -> Self {
        match error {
            CatalogueApplyError::Rejected(message) => DeployError::invalid(message),
            CatalogueApplyError::Failed(message) => DeployError::internal(message),
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

/// A deploy request validated against a catalogue and staged on it.
pub struct Deployment {
    pub prepared: PreparedDeployment,
    pub staged: StagedDeployment,
}

/// Validate `request` against `index` and stage what it publishes at
/// `now` (Unix ms). A host commits the staged rows (with
/// [`Deployment::catalogue_change`]) only while its catalogue still reads
/// [`StagedDeployment::expected`]; with `staged.response.changed` false
/// there is nothing to commit.
pub fn prepare(
    index: &CatalogueIndex,
    request: DeployRequest,
    now: u64,
) -> Result<Deployment, DeployError> {
    let stored = index.deployment_catalogue();
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
    let staged = index
        .stage_deployment(&prepared, now)
        .map_err(|error| DeployError::invalid(staging_error(error)))?;
    Ok(Deployment { prepared, staged })
}

/// How a staging failure reads, as the native catalogue's write error
/// always showed it.
fn staging_error(error: jazz::db::Error) -> String {
    format!("Write error: {error}")
}

impl Deployment {
    /// The schema a runtime that does not exist yet starts with: the
    /// deployment's first.
    pub fn genesis(&self) -> Result<JazzSchema, DeployError> {
        JazzSchema::new(&self.prepared.schemas()[0]).map_err(DeployError::invalid)
    }

    /// What applying this deployment to a runtime whose catalogue holds
    /// `runtime_rows` changes: the staged rows, and the admission of its
    /// schemas. Nothing becomes current.
    pub fn catalogue_change(
        &self,
        runtime_rows: &CatalogueRows,
    ) -> Result<CatalogueChange, DeployError> {
        deployment::prepare_catalogue_change(&self.prepared, runtime_rows, self.staged.put.clone())
            .map_err(DeployError::invalid)
    }
}
