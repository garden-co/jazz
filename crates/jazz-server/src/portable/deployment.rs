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

use super::accounts::encrypts_or_unknown;
use super::catalogue::{CatalogueIndex, DeploymentResponse, StagedDeployment};
use crate::runtime::CatalogueApplyError;

/// Per-physical-schema declarations in [`encryption::STORED_DECLARATIONS_V1`] form.
pub const DECLARATIONS: HostTable = HostTable::new("e2ee_declarations");
pub type KeptValue = (HostTable, Vec<u8>, Vec<u8>);
pub type KeptValues = Vec<KeptValue>;

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
    async fn read(db: &Db, hash: SchemaHash, entries: encryption::Entries) -> Result<Self, Error> {
        let (schema, names) = db.catalogue_schema_and_stored_names(&hash).await?;
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
            successors: db.catalogue_successors(&hash).await?,
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

    async fn check_stored_types_and_spaces(
        &self,
        next: &Self,
        precedes: impl AsyncFnMut() -> Result<bool, Error>,
    ) -> Result<(), Error> {
        encryption::check_unlinked_transition(
            &self.entries,
            &next.entries,
            |table| next.table_in(self, table),
            |table| self.table_in(next, table),
            |table, name| next.column_in(self, table, name),
            |table, name| self.column_in(next, table, name),
            precedes,
        )
        .await?
        .map_err(|message| Error::new(ErrorCode::Schema, message))
    }

    fn check_transition_to(&self, next: &Self) -> Result<(), String> {
        encryption::check_mapped_columns(
            (&self.schema, &self.entries),
            &next.entries,
            |table| next.table_in(self, table),
            |table| self.table_in(next, table),
            |table, name| next.column_in(self, table, name),
            |table, name| self.column_in(next, table, name),
            false,
        )
    }
}

/// Check every new declaration inside the catalogue transaction, after
/// admission assigns stored identities and before the schema becomes live.
pub(crate) async fn check_stored_columns(db: &Db, kept: &[KeptValue]) -> Result<(), Error> {
    let invalid = |message: String| Error::new(ErrorCode::Schema, message);
    let mut new = Vec::new();
    for (table, key, bytes) in kept {
        if *table == DECLARATIONS && db.host_get(*table, key).await?.is_none() {
            let hash = key.as_slice().try_into().map_err(|_| {
                invalid("invalid schema hash in new encryption declarations".into())
            })?;
            let hash = SchemaHash(hash);
            new.push((
                hash,
                DeclaredVersion::read(db, hash, encryption::decode_stored(bytes).map_err(invalid)?)
                    .await?,
            ));
        }
    }
    if new.is_empty() {
        return Ok(());
    }

    let mut history = Vec::new();
    for (key, bytes) in db.host_prefix(DECLARATIONS, &[]).await? {
        // Only a schema-hash key can name a deployed version.
        if let Ok(hash) = key.as_slice().try_into() {
            let entries = encryption::decode_stored(&bytes)
                .map_err(|error| Error::new(ErrorCode::Storage, error))?;
            let hash = SchemaHash(hash);
            history.push((hash, DeclaredVersion::read(db, hash, entries).await?));
        }
    }
    let mut descendants: HashMap<SchemaHash, HashSet<SchemaHash>> = HashMap::new();
    for (i, (hash, next)) in new.iter().enumerate() {
        let submitted = &new[..i];
        next.check_sealed_types(
            history.iter().map(|(_, old)| old).chain(
                submitted
                    .iter()
                    .filter(|(old, _)| descendants[old].contains(hash))
                    .map(|(_, old)| old),
            ),
        )
        .map_err(invalid)?;
        for (old_hash, old) in history.iter().chain(submitted) {
            if old.successors.contains(hash) {
                old.check_transition_to(next).map_err(invalid)?;
            } else {
                old.check_stored_types_and_spaces(next, async || {
                    if !descendants.contains_key(old_hash) {
                        let reachable = db.catalogue_descendants(old_hash).await?;
                        descendants.insert(*old_hash, reachable);
                    }
                    Ok(descendants[old_hash].contains(hash))
                })
                .await?;
            }
        }
        if i + 1 < new.len() {
            descendants.insert(*hash, db.catalogue_descendants(hash).await?);
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
    #[serde(default)]
    pub encryption: Option<DeclaredEncryption>,
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

impl Deployment {
    pub fn kept_values(&self) -> KeptValues {
        self.first_declared
            .iter()
            .map(|(version, stored)| (DECLARATIONS, version.0.to_vec(), stored.clone()))
            .collect()
    }
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
    /// Whether any version this deployment publishes has Jazz's system
    /// tables or its system table fragment cannot be validated.
    pub fn encrypts(&self) -> bool {
        self.prepared
            .schemas()
            .iter()
            .any(|schema| encrypts_or_unknown(system_tables::check_e2ee_fragment(schema)))
    }

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

/// Why a deployment that brings an encrypting app to a runtime without the
/// account registry its checks read is refused before anything is applied.
pub const REGISTRY_REQUIRED: &str = "this app encrypts columns, which needs the account registry of an app's store; \
     the server's runtime has none";

/// Parse a deploy body (a [`DeployRequest`] and its encryption
/// declarations).
pub fn read_body(body: &[u8]) -> Result<DeployBody, DeployError> {
    serde_json::from_slice(body).map_err(DeployError::invalid)
}

/// The kept declarations, by schema hash.
type Kept = HashMap<Vec<u8>, Vec<u8>>;

/// Every kept declaration, read once from `tables` ([`DECLARATIONS`]): the
/// checks see one moment of the table, and on the connection `tables` holds
/// (a core's own, as native reads them on its store's thread), never a
/// second connection that a shared in-memory database locks out during a
/// write.
fn read_kept(tables: &impl HostTables) -> Result<Kept, DeployError> {
    Ok(jazz::db::block_on(tables.host_prefix(DECLARATIONS, &[]))
        .map_err(DeployError::internal)?
        .into_iter()
        .collect())
}

/// Stage the deploy `body` on `index` (the store's catalogue), judged by
/// the declarations kept in `kept`, and refuse an encrypting app on a core
/// that cannot install the account registry its checks read.
fn stage(
    index: &CatalogueIndex,
    kept: &Kept,
    body: &[u8],
    now: u64,
    registry: bool,
) -> Result<Deployment, DeployError> {
    let deployment = prepare_encrypted(index, read_body(body)?, now, |version| {
        Ok(kept.get(version.0.as_slice()).cloned())
    })?;
    // As native refuses it: an encrypting app needs the registry its checks
    // read, whatever brings it to the core.
    if deployment.encrypts() && !registry {
        return Err(DeployError::invalid(REGISTRY_REQUIRED));
    }
    Ok(deployment)
}

/// Stage the deploy `body` on `core`'s catalogue and apply it, for a host
/// that holds the core and its store in one place. The declarations each
/// version was first deployed with are read once from the core's own
/// connection ([`DECLARATIONS`]). The ones this deployment names for the
/// first time are checked and written in the same transaction as the
/// catalogue rows (`ServerCore::apply_catalogue_keeping`), so a version is
/// deployed with its declarations or not at all, even when the deployment
/// changes no catalogue row. A bundle new to the store ends the sessions
/// pinned to others. Nothing to commit reloads the core's catalogue.
pub fn apply_to_core(
    core: &mut crate::runtime::ServerCore,
    body: &[u8],
    now: u64,
) -> Result<DeploymentResponse, DeployError> {
    let kept = read_kept(core.db())?;
    let rows = jazz::db::block_on(core.db().catalogue()).map_err(DeployError::internal)?;
    let index = CatalogueIndex::from_rows(&rows, None);
    let deployment = stage(&index, &kept, body, now, core.has_account_registry())?;
    let kept_values = deployment.kept_values();
    if !deployment.staged.response.changed && kept_values.is_empty() {
        core.reload_catalogue().map_err(DeployError::internal)?;
        return Ok(deployment.staged.response);
    }
    let change = if deployment.staged.response.changed {
        deployment.catalogue_change(&rows)?
    } else {
        // Declarations for an already admitted version get the same
        // stored-column check as those kept during admission.
        CatalogueChange::default()
    };
    core.apply_catalogue_keeping(
        deployment.staged.expected(),
        change,
        &kept_values,
        deployment.staged.new_bundle(),
    )?;
    Ok(deployment.staged.response)
}

/// What a host that holds its core and store in one place gives
/// [`deploy`]: handles on its store, a first open of its core, and the
/// account registry's install.
pub trait CoreHost {
    /// A new handle on the host's store (a Durable Object's SQL).
    fn store(&self) -> Result<jazz::host::StoreFile, String>;
    /// Open the host's core for `app` on its first schema `genesis`,
    /// initializing the store and committing `change` and `kept` in one
    /// transaction (`ServerCore::open_deployment`): no core exists unless
    /// that transaction commits.
    fn open_deployment(
        &self,
        genesis: JazzSchema,
        app: &str,
        change: CatalogueChange,
        kept: KeptValues,
    ) -> Result<crate::runtime::ServerCore, CatalogueApplyError>;
    /// Create the account registry's tables if they are missing, and
    /// install the registry the account tables' checks read on `core`.
    fn install_registry(&self, core: &mut crate::runtime::ServerCore) -> Result<(), String>;
}

/// Install the host's account registry on `core`, and refuse a core that
/// serves an encrypting app without it ([`CoreHost::install_registry`] may
/// leave a core without one, for one opened for no app).
pub fn install_registry(
    host: &dyn CoreHost,
    core: &mut crate::runtime::ServerCore,
) -> Result<(), String> {
    host.install_registry(core)?;
    core.require_account_registry()
}

/// Publish the deploy body in `body`: validate it against the core's
/// catalogue and apply its rows, admissions and first-time declarations in
/// one transaction ([`apply_to_core`]). Without a core yet, the core is
/// opened on the deployment's first schema with the store's
/// initialization, the deployment and its first-time declarations
/// committed together, so a deployment the catalogue or the declaration
/// checks refuse leaves the store as it was; the account registry is then
/// installed before the core serves anyone.
pub fn deploy(
    core: &mut Option<crate::runtime::ServerCore>,
    host: &dyn CoreHost,
    app: &str,
    body: &[u8],
    now: u64,
) -> Result<DeploymentResponse, DeployError> {
    let Some(core) = core else {
        let store = host.store().map_err(DeployError::internal)?;
        let rows = jazz::db::block_on(store.catalogue()).map_err(DeployError::internal)?;
        let index = CatalogueIndex::from_rows(&rows, None);
        // No core holds a connection yet: this handle is the only one. A
        // core opened here has an app, so it installs the registry.
        let kept = read_kept(&store)?;
        drop(store);
        let deployment = stage(&index, &kept, body, now, true)?;
        let kept_values = deployment.kept_values();
        if !deployment.staged.response.changed && kept_values.is_empty() {
            return Ok(deployment.staged.response);
        }
        let change = if deployment.staged.response.changed {
            deployment.catalogue_change(&rows)?
        } else {
            CatalogueChange::default()
        };
        let mut opened = host.open_deployment(deployment.genesis()?, app, change, kept_values)?;
        install_registry(host, &mut opened).map_err(DeployError::internal)?;
        *core = Some(opened);
        return Ok(deployment.staged.response);
    };
    apply_to_core(core, body, now)
}
