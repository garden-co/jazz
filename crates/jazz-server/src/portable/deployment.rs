//! A deployment's pure steps, as every host runs them: validate a deploy
//! request against the catalogue, stage the rows it publishes, and plan
//! the change that admits them into a runtime's catalogue. Locking,
//! committing, and starting or reaching the runtime stay with each host.

use std::collections::{BTreeMap, HashMap, HashSet};

use http::StatusCode;
use jazz::catalogue::{CatalogueChange, CatalogueRows};
use jazz::db::{Db, Error, ErrorCode};
use jazz::host::{HostTable, HostTables};
use jazz::schema::JazzSchema;
use jazz::tools::deployment::artifact::{DeclaredEncryption, encryption, system_tables};
use jazz::tools::deployment::published::{PublishMigrationRequest, lower_migration};
use jazz::tools::deployment::{self, DeploymentError, PreparedDeployment};
use jazz::tools::public_schema::{Schema, SchemaHash, TableName, TablePolicies};
use serde::{Deserialize, Serialize};

use super::catalogue::{CatalogueIndex, StagedDeployment};
use crate::runtime::CatalogueApplyError;

/// Per-physical-schema declarations in [`encryption::STORED_DECLARATIONS_V1`] form.
pub(crate) const DECLARATIONS: HostTable = HostTable::new("e2ee_declarations");

type Names = BTreeMap<String, (String, BTreeMap<String, String>)>;

/// The catalogue names for a version, indexed in both directions so that
/// the transition check can follow a stored identity across public renames.
struct DeclaredVersion {
    schema: Schema,
    entries: encryption::Entries,
    stored: Names,
    public: Names,
    successors: HashSet<SchemaHash>,
}

impl DeclaredVersion {
    fn read(db: &Db, hash: SchemaHash, entries: encryption::Entries) -> Result<Self, Error> {
        let (schema, names) = db.catalogue_schema_and_stored_names(&hash)?;
        let stored = names.tables;
        let public = stored
            .iter()
            .map(|(table, (id, columns))| {
                let columns = columns
                    .iter()
                    .map(|(name, id)| (id.clone(), name.clone()))
                    .collect();
                (id.clone(), (table.clone(), columns))
            })
            .collect();
        Ok(Self {
            schema,
            entries,
            stored,
            public,
            successors: db.catalogue_successors(&hash)?,
        })
    }

    fn table_in(&self, other: &Self, table: &str) -> Option<String> {
        let (id, _) = self.stored.get(table)?;
        Some(other.public.get(id)?.0.clone())
    }

    fn column_in(&self, other: &Self, table: &str, column: &str) -> Option<String> {
        let (id, columns) = self.stored.get(table)?;
        other.public.get(id)?.1.get(columns.get(column)?).cloned()
    }

    fn check_sealed_types<'a>(
        &self,
        history: impl Iterator<Item = &'a Self> + Clone,
    ) -> Result<(), String> {
        for (table, entry) in &self.entries {
            for (name, declared) in &entry.columns {
                if declared.cell_type().same_type(&declared.plain) {
                    continue;
                }
                let declared_before = history.clone().any(|old| {
                    self.table_in(old, table)
                        .and_then(|t| old.entries.get(&t))
                        .zip(self.column_in(old, table, name))
                        .is_some_and(|(entry, column)| entry.columns.contains_key(&column))
                });
                if !declared_before {
                    return Err(format!(
                        "{table}.{name}: sealedType only pins an earlier type; on its first \
                         declaration it must match the declared type, including nullability"
                    ));
                }
            }
        }
        Ok(())
    }

    // A column's sealed encoding binds every version that shares its storage identity.
    fn check_stored_types_and_spaces(&self, next: &Self, siblings: bool) -> Result<(), String> {
        for (table, new) in &next.entries {
            let Some(was) = next.table_in(self, table) else {
                continue;
            };
            let Some(old) = self.entries.get(&was) else {
                continue;
            };
            encryption::check_stored_space(
                table,
                old,
                new,
                self.table_in(next, &old.scope),
                self.column_in(next, &was, &old.space),
            )?;
            for (name, declared) in &new.columns {
                let Some(previous) = next
                    .column_in(self, table, name)
                    .and_then(|column| old.columns.get(&column))
                else {
                    continue;
                };
                let path = format!("{table}.{name}");
                let check = encryption::check_stored_column_type(&path, previous, declared);
                // Siblings can have different lengths of the same sequence of enum variants.
                if siblings && check.is_err() {
                    encryption::check_stored_column_type(&path, declared, previous)?;
                } else {
                    check?;
                }
            }
        }
        Ok(())
    }

    fn check_transition_to(&self, next: &Self) -> Result<(), String> {
        encryption::check_mapped_transition(
            (&self.schema, &self.entries),
            (&next.schema, &next.entries),
            |table| next.table_in(self, table),
            |table| self.table_in(next, table),
            |table, name| next.column_in(self, table, name),
            |table, name| self.column_in(next, table, name),
        )
    }
}

/// Check every new declaration inside the catalogue transaction, after
/// admission assigns stored identities and before the schema becomes live.
pub(crate) fn check_stored_columns(
    db: &Db,
    kept: &[(HostTable, Vec<u8>, Vec<u8>)],
) -> Result<(), Error> {
    let invalid = |message: String| Error::new(ErrorCode::Schema, message);
    let mut new = Vec::new();
    for (table, key, bytes) in kept {
        if *table == DECLARATIONS && db.host_get(*table, key)?.is_none() {
            let hash = key.as_slice().try_into().map_err(|_| {
                invalid("invalid schema hash in new encryption declarations".into())
            })?;
            let hash = SchemaHash(hash);
            new.push((
                hash,
                DeclaredVersion::read(
                    db,
                    hash,
                    encryption::decode_stored(bytes).map_err(invalid)?,
                )?,
            ));
        }
    }
    if new.is_empty() {
        return Ok(());
    }

    let mut history = Vec::new();
    for (key, bytes) in db.host_prefix(DECLARATIONS, &[])? {
        // Only a schema-hash key can name a deployed version.
        if let Ok(hash) = key.as_slice().try_into() {
            let entries = encryption::decode_stored(&bytes)
                .map_err(|error| Error::new(ErrorCode::Storage, error))?;
            let hash = SchemaHash(hash);
            history.push(DeclaredVersion::read(db, hash, entries)?);
        }
    }
    let mut submitted: Vec<(&DeclaredVersion, HashSet<SchemaHash>)> = Vec::new();
    for (i, (hash, next)) in new.iter().enumerate() {
        let ancestors = submitted
            .iter()
            .filter_map(|(version, descendants)| descendants.contains(hash).then_some(*version));
        next.check_sealed_types(history.iter().chain(ancestors))
            .map_err(invalid)?;
        let previous = history.iter().map(|old| (old, false)).chain(
            submitted
                .iter()
                .map(|(old, descendants)| (*old, !descendants.contains(hash))),
        );
        for (old, siblings) in previous {
            if old.successors.contains(hash) {
                old.check_transition_to(next)
            } else {
                old.check_stored_types_and_spaces(next, siblings)
            }
            .map_err(invalid)?;
        }
        if i + 1 < new.len() {
            submitted.push((next, db.catalogue_descendants(hash)?));
        }
    }
    Ok(())
}

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

/// The body the deploy route takes: a [`DeployRequest`] and, for an app
/// that declares encryption, its declarations (every schema version's
/// entries and the settings, E2EE protocol §1.5).
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DeployBody {
    #[serde(flatten)]
    pub request: DeployRequest,
    #[serde(default, deserialize_with = "read_encryption")]
    pub encryption: Option<DeclaredEncryption>,
}

fn read_encryption<'de, D: serde::Deserializer<'de>>(
    reader: D,
) -> Result<Option<DeclaredEncryption>, D::Error> {
    Option::<DeclaredEncryption>::deserialize(reader).map_err(|error| {
        if error.to_string().starts_with("unknown field") {
            serde::de::Error::custom(format!("{error}; {}", encryption::COPY_GUIDE))
        } else {
            error
        }
    })
}

impl From<DeployRequest> for DeployBody {
    fn from(request: DeployRequest) -> Self {
        Self {
            request,
            encryption: None,
        }
    }
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
    /// Initial encryption declarations to persist when the deployment
    /// commits, in [`encryption::STORED_DECLARATIONS_V1`] form. An empty set
    /// records a version with system tables that declared no encryption.
    pub first_declared: Vec<(SchemaHash, Vec<u8>)>,
}

/// Validate `request` against `index` and stage what it publishes at
/// `now` (Unix ms). A host commits the staged rows (with
/// [`Deployment::catalogue_change`]) only while its catalogue still reads
/// [`StagedDeployment::expected`]; with `staged.response.changed` false
/// there is nothing to commit. A host that keeps encryption declarations
/// prepares with [`prepare_encrypted`] instead.
pub fn prepare(
    index: &CatalogueIndex,
    request: DeployRequest,
    now: u64,
) -> Result<Deployment, DeployError> {
    prepare_encrypted(index, request.into(), now, |_| Ok(None))
}

/// [`prepare`] with the app's encryption declarations: each version that
/// declares encryption must carry exactly Jazz's system tables, every
/// migration is checked against both its versions' declarations, the
/// target's space tables must take their read rule from the members table,
/// and Jazz derives the system tables' policies into the bundle. The staged
/// response carries the deploy's warnings. `kept` reads the declarations a
/// version was first deployed with, as the host keeps them.
pub fn prepare_encrypted(
    index: &CatalogueIndex,
    body: DeployBody,
    now: u64,
    kept: impl Fn(&SchemaHash) -> Result<Option<Vec<u8>>, String>,
) -> Result<Deployment, DeployError> {
    let DeployBody {
        request,
        encryption: declared,
    } = body;
    let declared = declared.unwrap_or_default();
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
    // The declarations every version this deployment touches is judged by:
    // the target, both ends of every migration, and every version it
    // submits or declares. A version deployed before keeps the
    // declarations it was deployed with. The body must repeat them.
    let target = hash(&request.target_schema_hash)?;
    let mut touched = vec![target];
    for migration in &request.migrations {
        touched.push(hash(&migration.from_hash)?);
        touched.push(hash(&migration.to_hash)?);
    }
    touched.extend(schemas.iter().map(|(hash, _)| *hash));
    for hex in declared.versions.keys() {
        touched.push(hash(hex)?);
    }
    touched.sort_by_key(|hash| hash.0);
    touched.dedup();
    let mut first_declared = HashMap::new();
    let empty = encryption::Entries::new();
    for version in touched {
        let hex = version.to_string();
        let given = declared.entries(&hex);
        let stored = kept(&version)
            .map_err(DeployError::internal)?
            .map(|bytes| encryption::decode_stored(&bytes))
            .transpose()
            .map_err(DeployError::internal)?;
        let refused = |reason: String| {
            DeployError::from(DeploymentError::InvalidSchema {
                schema: version,
                reason,
            })
        };
        let has_system_tables = all_schemas.get(&version).is_some_and(|schema| {
            schema
                .keys()
                .any(|name| system_tables::is_system_table(name.as_str()))
        });
        // Older hosts left no record when a deployed version declared nothing.
        // A published schema identifies those deployments. An unpublished
        // genesis schema can also be known to the catalogue, including on a
        // Durable Object's scratch store before it commits its first deploy.
        let previous = stored.as_ref().or_else(|| {
            (has_system_tables && index.schema_published_at(&version).is_some()).then_some(&empty)
        });
        match (previous, given) {
            (Some(stored), None) if !stored.is_empty() => {
                return Err(refused(
                    "this schema version was deployed with encryption declarations, which the \
                     deployment leaves out; send them (`encryption`) as the app's files have them"
                        .to_owned(),
                ));
            }
            (Some(stored), Some(given)) => {
                let schema = all_schemas
                    .get(&version)
                    .ok_or(DeploymentError::MissingSchema { schema: version })?;
                encryption::check_redeclaration(schema, stored, given).map_err(refused)?;
            }
            _ => {}
        }
        if stored.is_none() && (has_system_tables || given.is_some()) {
            let initial = previous.or(given).unwrap_or(&empty);
            first_declared.insert(
                version,
                encryption::encode_stored(initial).map_err(DeployError::internal)?,
            );
        }
    }
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
            // The declarations of both versions, before the lens is lowered.
            encryption::check_transition(
                (source, declared.entries(&migration.from_hash)),
                (target, declared.entries(&migration.to_hash)),
                &migration,
            )
            .map_err(|reason| {
                DeployError::from(DeploymentError::InvalidMigration { from, to, reason })
            })?;
            lower_migration(migration, source, target).map_err(DeployError::invalid)
        })
        .collect::<Result<Vec<_>, DeployError>>()?;
    let (prepared, warnings) = deployment::prepare_encrypted_deployment(
        &stored,
        deployment::DeploymentRequest {
            target_schema_hash: target,
            schemas,
            migrations,
            permissions: request.permissions,
            allow_dropping_history: request.allow_dropping_history,
        },
        &declared,
    )?;
    let mut staged = index
        .stage_deployment(&prepared, now)
        .map_err(|error| DeployError::invalid(staging_error(error)))?;
    staged.response.warnings = warnings;
    let first_declared = prepared
        .schemas()
        .iter()
        .filter_map(|schema| {
            let hash = SchemaHash::compute(schema);
            first_declared.remove(&hash).map(|entries| (hash, entries))
        })
        .collect();
    Ok(Deployment {
        prepared,
        staged,
        first_declared,
    })
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
