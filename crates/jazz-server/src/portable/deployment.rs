//! A deployment's pure steps, as every host runs them: validate a deploy
//! request against the catalogue, stage the rows it publishes, and plan
//! the change that admits them into a runtime's catalogue. Locking,
//! committing, and starting or reaching the runtime stay with each host.

use std::collections::HashMap;

use http::StatusCode;
use jazz::catalogue::{CatalogueChange, CatalogueRows};
use jazz::schema::JazzSchema;
use jazz::tools::deployment::artifact::{DeclaredEncryption, encryption, system_tables};
use jazz::tools::deployment::published::{PublishMigrationRequest, lower_migration};
use jazz::tools::deployment::{self, DeploymentError, PreparedDeployment};
use jazz::tools::public_schema::{Schema, SchemaHash, TableName, TablePolicies};
use serde::{Deserialize, Serialize};

use super::accounts::encrypts_or_unknown;
use super::catalogue::{CatalogueIndex, DeploymentResponse, StagedDeployment};
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
    /// Decrypt conversions the deployer confirms, as `table.column`: the
    /// server and every replica then hold their plaintext (migrations
    /// §4.2).
    #[serde(default)]
    pub confirm_decrypt: Vec<String>,
}

impl From<DeployRequest> for DeployBody {
    fn from(request: DeployRequest) -> Self {
        Self {
            request,
            encryption: None,
            confirm_decrypt: Vec::new(),
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
    /// The encryption declarations of each version deployed with them for
    /// the first time, in [`encryption::STORED_DECLARATIONS_V1`] form: a
    /// host keeps them once the deployment committed, and keeps the ones
    /// it kept already.
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
        confirm_decrypt,
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
    // declarations it was deployed with: the body must repeat them, or
    // change them only as a migration could without a conversion; it
    // never decides them alone.
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
    let mut first_declared = Vec::new();
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
        match (&stored, given) {
            (Some(_), None) => {
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
            (None, Some(given)) => first_declared.push((
                version,
                encryption::encode_stored(given).map_err(DeployError::internal)?,
            )),
            (None, None) => {}
        }
        if let Some(given) = given
            && let Some(unconfirmed) = encryption::new_decrypts(stored.as_ref(), given)
                .into_iter()
                .find(|column| !confirm_decrypt.contains(column))
        {
            return Err(DeployError::invalid(format!(
                "decrypting {unconfirmed} puts its plaintext on the server and every replica; \
                 confirm it with `confirmDecrypt: [\"{unconfirmed}\"]` (`confirmDecrypt` \
                 on `createJazz`, `confirm_decrypt` on the Rust backend)"
            )));
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

/// The host table a store keeps each deployed schema version's encryption
/// declarations in, keyed by the version's schema hash, in the form
/// [`encryption::STORED_DECLARATIONS_V1`]: a later deployment of the
/// version may not change them in a way that breaks its sealed cells
/// (migrations §3.1), even though the schema is unchanged.
pub const DECLARATIONS: jazz::host::HostTable = jazz::host::HostTable::new("e2ee_declarations");

/// Why a deployment that brings an encrypting app to a runtime without the
/// account registry its checks read is refused before anything is applied.
pub const REGISTRY_REQUIRED: &str = "this app encrypts columns, which needs the account registry of an app's store; \
     the server's runtime has none";

/// Keep the declarations of each version a deployment that changes no
/// catalogue row names for the first time (`first`, as
/// [`Deployment::first_declared`]), each written unless its version keeps
/// some already. A deployment that changes the catalogue keeps them in the
/// transaction that commits it instead. A version whose kept declarations
/// differ from the ones named (written since the deployment read them) is
/// refused rather than ignored, as that transaction refuses it: its cells
/// are sealed under the ones kept.
/// `put_if_absent` writes a value at a version's key in the host's
/// [`DECLARATIONS`] unless one is there, and returns the one there if so.
pub fn keep_first_declared(
    first: &[(SchemaHash, Vec<u8>)],
    mut put_if_absent: impl FnMut(&[u8], &[u8]) -> Result<Option<Vec<u8>>, String>,
) -> Result<(), DeployError> {
    for (version, stored) in first {
        if let Some(existing) = put_if_absent(&version.0, stored).map_err(DeployError::internal)?
            && existing != *stored
        {
            return Err(DeployError::invalid(format!(
                "schema version {version} was deployed with other encryption declarations, \
                 which its cells are sealed under; the deployment names different ones"
            )));
        }
    }
    Ok(())
}

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
fn read_kept(tables: &dyn jazz::host::HostTables) -> Result<Kept, DeployError> {
    Ok(tables
        .host_prefix(DECLARATIONS, &[])
        .map_err(DeployError::internal)?
        .into_iter()
        .collect())
}

/// Stage the deploy `body` on `core`'s catalogue and apply it, for a host
/// that holds the core and its store in one place. The declarations each
/// version was first deployed with are read once from the core's own
/// connection ([`DECLARATIONS`]). The ones this deployment names for the
/// first time are written in the same transaction as the catalogue rows
/// (`ServerCore::apply_catalogue_keeping`), so a version is deployed with
/// its declarations or not at all; a deployment with nothing to commit
/// writes them on their own. A bundle new to the store ends the sessions
/// pinned to others. Nothing to commit reloads the core's catalogue.
pub fn apply_to_core(
    core: &mut crate::runtime::ServerCore,
    body: &[u8],
    now: u64,
) -> Result<DeploymentResponse, DeployError> {
    let kept = read_kept(core.db())?;
    apply_with_kept(core, &kept, body, now, false)
}

/// [`apply_to_core`] judged by `kept`. With `probe`, the core is a scratch
/// one that stands in for the store (a deployment the catalogue refuses
/// never creates it): nothing is kept, and a bundle new to it ends no
/// session.
fn apply_with_kept(
    core: &mut crate::runtime::ServerCore,
    kept: &Kept,
    body: &[u8],
    now: u64,
    probe: bool,
) -> Result<DeploymentResponse, DeployError> {
    use jazz::host::HostTables;
    let rows = core.db().catalogue().map_err(DeployError::internal)?;
    let index = CatalogueIndex::from_rows(&rows, None);
    let deployment = prepare_encrypted(&index, read_body(body)?, now, |version| {
        Ok(kept.get(version.0.as_slice()).cloned())
    })?;
    // As native refuses it: an encrypting app needs the registry its checks
    // read, whatever brings it to the core.
    if !probe && deployment.encrypts() && !core.has_account_registry() {
        return Err(DeployError::invalid(REGISTRY_REQUIRED));
    }
    let first = if probe {
        Vec::new()
    } else {
        deployment
            .first_declared
            .iter()
            .map(|(version, stored)| (DECLARATIONS, version.0.to_vec(), stored.clone()))
            .collect()
    };
    if !deployment.staged.response.changed {
        core.reload_catalogue().map_err(DeployError::internal)?;
        if !probe {
            // On the core's own connection, as the kept declarations were
            // read: a different value written since is refused, not kept.
            keep_first_declared(&deployment.first_declared, |key, value| {
                core.db()
                    .host_put_if_absent(DECLARATIONS, key, value)
                    .map_err(|error| error.to_string())
            })?;
        }
        return Ok(deployment.staged.response);
    }
    let change = deployment.catalogue_change(&rows)?;
    let new_bundle = deployment.staged.new_bundle().filter(|_| !probe);
    core.apply_catalogue_keeping(deployment.staged.expected(), change, &first, new_bundle)?;
    Ok(deployment.staged.response)
}

/// What a host that holds its core and store in one place gives
/// [`deploy`]: handles on its store, and the account registry's install.
pub trait CoreHost {
    /// A new handle on the host's store (a Durable Object's SQL).
    fn store(&self) -> Result<jazz::host::StoreFile, String>;
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
/// one transaction ([`apply_to_core`]). Without a core yet, the deployment
/// is probed on a scratch store first, so one the catalogue refuses never
/// opens the app's store on its schema, and the core is opened on its first
/// schema with the account registry installed; it is then staged again on
/// that core's catalogue (which admitted the schema it opened on).
pub fn deploy(
    core: &mut Option<crate::runtime::ServerCore>,
    host: &dyn CoreHost,
    app: &str,
    body: &[u8],
    now: u64,
) -> Result<DeploymentResponse, DeployError> {
    use crate::runtime::ServerCore;
    use jazz::host::{HostTables, StoreFile};
    if core.is_none() {
        let store = host.store().map_err(DeployError::internal)?;
        let rows = store.catalogue().map_err(DeployError::internal)?;
        let index = CatalogueIndex::from_rows(&rows, None);
        // No core holds a connection yet: this handle is the only one.
        let kept = read_kept(&store)?;
        let deployment = prepare_encrypted(&index, read_body(body)?, now, |version| {
            Ok(kept.get(version.0.as_slice()).cloned())
        })?;
        if !deployment.staged.response.changed {
            // Nothing to commit; the declarations of a version deployed
            // with them for the first time are kept as when it commits.
            keep_first_declared(&deployment.first_declared, |key, value| {
                store
                    .host_put_if_absent(DECLARATIONS, key, value)
                    .map_err(|error| error.to_string())
            })?;
            return Ok(deployment.staged.response);
        }
        let genesis = deployment.genesis()?;
        let scratch = StoreFile::memory().map_err(DeployError::internal)?;
        let mut probe = ServerCore::open(genesis.clone(), scratch, false, None)
            .map_err(DeployError::internal)?;
        // The scratch store keeps nothing and ends no session; the
        // declarations the deployment is judged by are the real store's.
        let probed = apply_with_kept(&mut probe, &kept, body, now, true).map(drop);
        probe.close();
        probed?;
        let mut opened = ServerCore::open(genesis, store, false, Some(app.to_owned()))
            .map_err(DeployError::internal)?;
        install_registry(host, &mut opened).map_err(DeployError::internal)?;
        *core = Some(opened);
    }
    let core = core.as_mut().expect("opened above");
    apply_to_core(core, body, now)
}
