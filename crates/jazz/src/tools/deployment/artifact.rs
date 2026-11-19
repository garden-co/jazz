//! An app's two readable files (SPEC §10.10): `jazz.app.json` (its tables,
//! relations, schema history and migrations, which the app and its clients
//! ship) and `jazz.permissions.json` (the current version's permissions,
//! which only backends read, so no client bundle carries the app's
//! policies).
//!
//! In stage 1 `jazz-tools validate` compiles the TS DSL to them through
//! [`write`], the files' only writer. A backend reads both with [`load`],
//! publishes [`LoadedApp::deployment_request`] at startup and pins its
//! sessions to [`LoadedApp::bundle`]. A client reads `jazz.app.json` alone
//! with [`load_client`]. Neither file carries a schema hash or bundle id:
//! loading computes them.
//!
//! Policies are expression strings in a small SQL-like grammar
//! ([`expr`]); errors point at the exact spot inside the string and suggest
//! the nearest names.

mod app_file;
pub mod encryption;
pub mod expr;
pub mod json;
mod permissions_file;
pub mod relations;
pub mod system_tables;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalogue::{BundleId, Permissions, bundle_id, permissions_of};
use crate::schema::JazzSchema;
use crate::tools::public_schema::{
    ColumnType, OperationPolicy, Schema, SchemaHash, TableName, TablePolicies, TableSchema,
};

use super::published::{
    ClientMigrations, PublishMigrationRequest, PublishedSchema, check_decoded_client_migrations,
    lower_client_migrations, lower_migration, parse_schema_hash,
};
use super::{DeploymentRequest, structural};
use json::Json;

pub use encryption::DeclaredEncryption;

/// The version of both files this version of Jazz reads and writes. A file
/// names its version in `$schema`, the URL of its JSON Schema
/// ([`APP_SCHEMA_URL`], [`PERMISSIONS_SCHEMA_URL`]).
pub const VERSION: u32 = 1;

/// The app's file: tables, relations, history and migrations.
pub const APP_FILE: &str = "jazz.app.json";
/// The backend's file: the current version's permissions.
pub const PERMISSIONS_FILE: &str = "jazz.permissions.json";

/// The JSON Schemas the files name in `$schema`, which carries their version.
pub const APP_SCHEMA_URL: &str = "https://jazz.tools/schemas/jazz.app.v1.json";
pub const PERMISSIONS_SCHEMA_URL: &str = "https://jazz.tools/schemas/jazz.permissions.v1.json";
/// The unversioned URLs Jazz before alpha.66 wrote, next to `"format": 1`. Such files
/// still load, as version 1.
pub const LEGACY_APP_SCHEMA_URL: &str = "https://jazz.tools/schemas/jazz.app.schema.json";
pub const LEGACY_PERMISSIONS_SCHEMA_URL: &str =
    "https://jazz.tools/schemas/jazz.permissions.schema.json";
const SCHEMA_URL_BASE: &str = "https://jazz.tools/schemas/";

/// Why a `jazz.app.json` or `jazz.permissions.json` was refused. Every
/// message names the file and the JSON path, and inside an expression the
/// exact spot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{file}: {message}")]
pub struct ArtifactError {
    file: &'static str,
    message: String,
    kind: ArtifactErrorKind,
}

/// What kind of refusal an [`ArtifactError`] is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ArtifactErrorKind {
    /// The input is wrong: a malformed or inconsistent file or input.
    Refused,
    /// The input is fine, but the files can't express it (yet): the writer
    /// refuses rather than write something that reads back differently.
    Unsupported,
}

/// The JavaScript `error.code` bindings give an
/// [`ArtifactErrorKind::Unsupported`] refusal.
pub const UNSUPPORTED_CODE: &str = "JAZZ_APP_FILES_UNSUPPORTED";

impl ArtifactError {
    fn new(message: impl Into<String>) -> Self {
        Self::in_file(APP_FILE, message)
    }

    fn in_file(file: &'static str, message: impl Into<String>) -> Self {
        Self {
            file,
            message: message.into(),
            kind: ArtifactErrorKind::Refused,
        }
    }

    /// What the files can't express, in `file`.
    fn unsupported(file: &'static str, message: impl Into<String>) -> Self {
        Self {
            kind: ArtifactErrorKind::Unsupported,
            ..Self::in_file(file, message)
        }
    }

    /// Whether the input was wrong, or the files can't express it.
    pub fn kind(&self) -> ArtifactErrorKind {
        self.kind
    }

    /// Whether the files can't express the input (yet).
    pub fn is_unsupported(&self) -> bool {
        self.kind == ArtifactErrorKind::Unsupported
    }

    fn at(file: &'static str, (path, message): (String, String)) -> Self {
        Self::in_file(file, format!("{path}: {message}"))
    }

    /// The file refused: [`APP_FILE`] or [`PERMISSIONS_FILE`].
    pub fn file(&self) -> &'static str {
        self.file
    }

    /// The refusal, without the file prefix.
    pub fn message(&self) -> &str {
        &self.message
    }
}

const REGENERATE: &str = "regenerate it with `npx jazz-tools generate`";

// ============================================================================
// Codegen metadata
// ============================================================================

/// What typed code in each language needs beyond the engine's schema: the
/// relations, the computed fields and the transforms the app declares.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Codegen {
    /// For tables of `current`.
    #[serde(default)]
    pub tables: BTreeMap<String, CodegenTable>,
    /// The file's `transforms`, by name: what every language implements.
    #[serde(default)]
    pub transforms: BTreeMap<String, TransformDecl>,
    /// Slices a .63 `schema.ts` exported. Slicing is gone in alpha.66:
    /// accepted from the .63 writer's input and ignored, never written.
    #[serde(default, skip_serializing)]
    pub slices: BTreeMap<String, Vec<String>>,
    /// The app's encryption declarations (protocol §1.5): the settings and
    /// each schema version's entries, by schema hash. A version that
    /// declares encryption has Jazz's system tables in its schema, and its
    /// hash covers them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub encryption: Option<DeclaredEncryption>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodegenTable {
    #[serde(default)]
    pub relations: BTreeMap<String, CodegenRelation>,
    /// Computed fields, in declared order: not stored, implemented in every
    /// language's code.
    #[serde(default)]
    pub computed: Vec<ComputedField>,
    /// Column → the name of the transform (in [`Codegen::transforms`]) its
    /// stored value goes through.
    #[serde(default)]
    pub transforms: BTreeMap<String, String>,
    /// .63's `tsOnly` (the TS DSL's `s.computed` and `.transform` fields).
    /// The .63 writer still emits it and loaders accept it with a warning
    /// that it is ignored: those fields are declared in schema.ts with
    /// `s.computed(…)` or `.transform(…)`.
    #[serde(default)]
    pub ts_only: Vec<TsOnlyColumn>,
}

/// A type with its nullability, as a computed field or transform has it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FieldType {
    pub column_type: ColumnType,
    #[serde(default)]
    pub nullable: bool,
}

/// `"computed": { "<name>": { "type": …, "from": [columns] } }`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ComputedField {
    pub name: String,
    #[serde(rename = "type")]
    pub field_type: FieldType,
    /// Stored columns of the same table the value is computed from, in order.
    pub from: Vec<String>,
}

/// `"transforms": { "<name>": { "stored": <type>, "view": <type> } }`: a
/// column stored as `stored` that code reads and writes as `view`. The
/// column's nullability carries over to both.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TransformDecl {
    pub stored: ColumnType,
    pub view: ColumnType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum CodegenRelation {
    /// This table's `column` references a row of `table`.
    Forward { table: String, column: String },
    /// Rows of `table` whose forward relation `relation` references this row.
    Reverse { table: String, relation: String },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum TsOnlyColumn {
    /// `s.computed(...)`: not stored.
    Computed { column: String },
    /// `.transform(...)` on a stored column.
    Transform { column: String },
}

impl Codegen {
    fn relations(&self) -> BTreeMap<String, BTreeMap<String, CodegenRelation>> {
        self.tables
            .iter()
            .map(|(table, info)| (table.clone(), info.relations.clone()))
            .collect()
    }
}

// ============================================================================
// Reading
// ============================================================================

/// An app's files that passed every check.
#[derive(Debug, Clone)]
pub struct LoadedApp {
    history: History,
    bundle: BundleId,
    permissions: Permissions,
    codegen: Codegen,
    warnings: Vec<String>,
}

impl LoadedApp {
    pub fn current_hash(&self) -> SchemaHash {
        self.history.current
    }

    /// The current schema, without permissions.
    pub fn current_schema(&self) -> &Schema {
        self.history.current_schema()
    }

    /// The permissions bundle a backend serving this app pins its sessions to.
    pub fn bundle(&self) -> BundleId {
        self.bundle
    }

    /// The app's own permissions, without the policies Jazz derives for its
    /// system tables (the bundle covers both).
    pub fn permissions(&self) -> &Permissions {
        &self.permissions
    }

    pub fn codegen(&self) -> &Codegen {
        &self.codegen
    }

    /// The app's encryption declarations, every version's.
    pub fn encryption(&self) -> &DeclaredEncryption {
        &self.history.encryption
    }

    /// What loaded but is ignored, as `jazz.app.json: <path>: <why>` lines,
    /// and what the deploy warns of an encrypted app's policies.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// Every version the file ships, oldest first, policy-free.
    pub fn schemas(&self) -> &[(SchemaHash, Schema)] {
        &self.history.schemas
    }

    /// The migrations, in the form a deployment publishes them.
    pub fn migrations(&self) -> &[PublishMigrationRequest] {
        &self.history.migrations
    }

    /// The schema versions and lenses a client opens along: what
    /// [`load_client`] gives for the same file.
    pub fn client_migrations(&self) -> crate::db::Migrations {
        self.history.client_migrations()
    }

    /// The body a backend `POST`s to `/apps/<id>/admin/bundles` at startup
    /// (the server's `DeployRequest`, camelCase): the same deployment as
    /// [`LoadedApp::deployment_request`], with migrations in their published
    /// form.
    pub fn deployment_body(&self, allow_dropping_history: bool) -> serde_json::Value {
        let mut body = serde_json::json!({
            "targetSchemaHash": self.history.current.to_string(),
            "schemas": self.history.schemas.iter().map(|(hash, schema)| PublishedSchema {
                hash: hash.to_string(),
                schema: schema.clone(),
            }).collect::<Vec<_>>(),
            "migrations": self.history.migrations,
            "permissions": self.permissions,
            "allowDroppingHistory": allow_dropping_history,
        });
        if !self.history.encryption.is_empty() {
            body["encryption"] =
                serde_json::to_value(&self.history.encryption).expect("declarations serialize");
        }
        body
    }

    /// What a backend publishes at startup.
    pub fn deployment_request(&self, allow_dropping_history: bool) -> DeploymentRequest {
        let schemas = self.history.schemas.clone();
        let schema = |hash: &str| {
            let hash = parse_schema_hash(hash).expect("checked on load");
            &schemas.iter().find(|(h, _)| *h == hash).unwrap().1
        };
        let migrations = self
            .history
            .migrations
            .iter()
            .map(|migration| {
                lower_migration(
                    migration.clone(),
                    schema(&migration.from_hash),
                    schema(&migration.to_hash),
                )
                .expect("checked on load")
            })
            .collect();
        DeploymentRequest {
            target_schema_hash: self.history.current,
            schemas: schemas.clone(),
            migrations,
            permissions: self
                .permissions
                .iter()
                .map(|(t, p)| (*t, p.clone()))
                .collect(),
            allow_dropping_history,
        }
    }
}

/// Read and check an app's two files, as a backend does.
pub fn load(app: &[u8], permissions: &[u8]) -> Result<LoadedApp, ArtifactError> {
    let app_file = read_app(app)?;
    let history = check_history(&app_file)?;
    // Lenses, as a deployment and `jazz-tools validate` check them. Where a
    // migration sits in the whole history is the store's to judge: its
    // catalogue may hold versions this file no longer ships.
    check_decoded_client_migrations(history.bundle(), history.current)
        .map_err(|error| ArtifactError::new(format!("migrations: {error}")))?;
    check_codegen(&app_file.codegen, history.current_schema())?;
    let relations = app_file.codegen.relations();
    let computed: expr::ComputedNames = app_file
        .codegen
        .tables
        .iter()
        .map(|(table, t)| {
            let names = t.computed.iter().map(|f| f.name.clone()).collect();
            (table.clone(), names)
        })
        .collect();
    let permissions_json = read_version(PERMISSIONS_FILE, permissions)?;
    let permissions = permissions_file::read(
        &permissions_json,
        history.current_schema(),
        &relations,
        &computed,
    )
    .map_err(|refusal| ArtifactError::at(PERMISSIONS_FILE, refusal))?;
    let (bundled, encryption_warnings) = with_system_policies(&history, &permissions)?;
    let bundle = compute_bundle(history.current_schema(), &bundled, &computed)?;
    let stranded = history.stranded.clone();
    let mut codegen = app_file.codegen;
    codegen.encryption = history.declared();
    Ok(LoadedApp {
        history,
        bundle,
        permissions,
        codegen,
        warnings: app_file
            .warnings
            .into_iter()
            .map(|w| format!("{APP_FILE}: {w}"))
            .chain(stranded)
            .chain(encryption_warnings)
            .collect(),
    })
}

/// `permissions` with the policies Jazz derives for its system tables
/// when the current version declares encryption, which the bundle id
/// covers, after refusing a space table whose read rule doesn't come from
/// the members table; and the deploy's warnings about what it can see.
fn with_system_policies(
    history: &History,
    permissions: &Permissions,
) -> Result<(Permissions, Vec<String>), ArtifactError> {
    let Some(entries) =
        governing_entries(&history.encryption, &history.migrations, history.current)?
    else {
        return Ok((permissions.clone(), Vec::new()));
    };
    encryption::check_space_read_rules(permissions, entries)
        .map_err(|error| ArtifactError::in_file(PERMISSIONS_FILE, error))?;
    // A policy may read a column that is plaintext in this version: only the
    // columns this version itself declares encrypted are checked.
    if let Some(own) = history.encryption.entries(&history.current.to_string()) {
        encryption::check_encrypted_reads(permissions, own)
            .map_err(|error| ArtifactError::in_file(PERMISSIONS_FILE, error))?;
    }
    let derived = system_tables::derive_e2ee_policies(
        history.current_schema(),
        entries,
        &history.encryption.settings,
    )
    .map_err(|error| ArtifactError::new(format!("encryption: {error}")))?;
    let mut bundled = permissions.clone();
    bundled.extend(derived);
    let warnings = encryption::warnings(permissions, entries)
        .into_iter()
        .map(|w| format!("{PERMISSIONS_FILE}: {w}"))
        .collect();
    Ok((bundled, warnings))
}

/// The compiled app generated bindings embed (`{ schema, relations }`, the
/// options they pass to `bindApp`), as a deployed backend has it when
/// `jazz.app.json` isn't on disk.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundApp {
    /// The bindings' `source`; the backend's own check, not this one's.
    #[serde(default)]
    #[allow(dead_code)]
    source: Option<String>,
    schema: BoundSchema,
    relations: BTreeMap<String, BTreeMap<String, BoundRelation>>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct BoundSchema {
    current: String,
    schemas: Vec<PublishedSchema>,
    migrations: Vec<PublishMigrationRequest>,
    // The runtime's: which columns computed callbacks and transforms take.
    #[serde(default)]
    computed: Option<serde_json::Value>,
    #[serde(default)]
    #[allow(dead_code)]
    transforms: Option<serde_json::Value>,
    /// The app's encryption declarations, every version's.
    #[serde(default)]
    encryption: Option<DeclaredEncryption>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged, deny_unknown_fields)]
enum BoundRelation {
    One { one: String, column: String },
    Many { many: String, relation: String },
}

/// Read and check a backend's permissions against the compiled app its
/// generated bindings embed (JSON of `{ schema, relations }`), with every
/// check [`load`] runs: each schema matches its hash, the lenses pass a
/// deployment's checks, the relations resolve and every policy compiles.
pub fn load_backend(bound: &[u8], permissions: &[u8]) -> Result<LoadedApp, ArtifactError> {
    let refuse = |message: String| {
        ArtifactError::new(format!(
            "the bindings' compiled app: {message}; regenerate them with `npx jazz-tools generate`"
        ))
    };
    let bound: BoundApp =
        serde_json::from_slice(bound).map_err(|error| refuse(error.to_string()))?;
    let current = parse_schema_hash(&bound.schema.current).map_err(refuse)?;
    let mut schemas = Vec::new();
    for entry in bound.schema.schemas {
        let hash = parse_schema_hash(&entry.hash).map_err(refuse)?;
        let schema = structural(entry.schema);
        if SchemaHash::compute(&schema) != hash {
            return Err(refuse(format!("schema {hash} doesn't match its content")));
        }
        schemas.push((hash, schema));
    }
    if !schemas.iter().any(|(hash, _)| *hash == current) {
        return Err(refuse(format!("no schema {current}, the current one")));
    }
    let encryption = bound.schema.encryption.unwrap_or_default();
    // A version after one that has the system tables keeps them whether or
    // not it declares encryption, as the loaders of the app file add them.
    let follows_carrier: Vec<SchemaHash> = bound
        .schema
        .migrations
        .iter()
        .filter_map(|migration| {
            let source = parse_schema_hash(&migration.from_hash).ok()?;
            let carries = schemas
                .iter()
                .any(|(hash, schema)| *hash == source && has_system_tables(schema));
            carries.then(|| parse_schema_hash(&migration.to_hash).ok())?
        })
        .collect();
    for (hash, schema) in &schemas {
        let entries = encryption.entries(&hash.to_string());
        let has_fragment = system_tables::check_e2ee_fragment(schema).map_err(&refuse)?;
        if has_fragment != (entries.is_some() || follows_carrier.contains(hash)) {
            return Err(refuse(format!(
                "schema {hash} {} Jazz's system tables but {} encryption",
                if has_fragment { "has" } else { "lacks" },
                if has_fragment {
                    "declares no"
                } else {
                    "declares"
                },
            )));
        }
        for (table, entry) in entries.into_iter().flatten() {
            encryption::check_entry(table, entry, schema).map_err(|(at, message)| {
                refuse(format!("tables.{table}.encryption{at}: {message}"))
            })?;
        }
    }
    let history = History {
        current,
        schemas,
        migrations: bound.schema.migrations,
        stranded: Vec::new(),
        encryption,
    };
    check_decoded_client_migrations(history.bundle(), history.current)
        .map_err(|error| refuse(format!("migrations: {error}")))?;
    let mut codegen = Codegen::default();
    for (table, relations) in bound.relations {
        let entry = codegen.tables.entry(table).or_default();
        for (name, relation) in relations {
            entry.relations.insert(
                name,
                match relation {
                    BoundRelation::One { one, column } => {
                        CodegenRelation::Forward { table: one, column }
                    }
                    BoundRelation::Many { many, relation } => CodegenRelation::Reverse {
                        table: many,
                        relation,
                    },
                },
            );
        }
    }
    codegen.tables.retain(|_, t| !t.relations.is_empty());
    check_codegen(&codegen, history.current_schema())
        .map_err(|e| refuse(e.message().to_string()))?;
    let relations = codegen.relations();
    // The bindings' `computed`: table → field → the columns it reads.
    let computed: expr::ComputedNames = bound
        .schema
        .computed
        .as_ref()
        .and_then(serde_json::Value::as_object)
        .map(|tables| {
            tables
                .iter()
                .map(|(table, fields)| {
                    let names = fields
                        .as_object()
                        .map(|f| f.keys().cloned().collect())
                        .unwrap_or_default();
                    (table.clone(), names)
                })
                .collect()
        })
        .unwrap_or_default();
    let permissions_json = read_version(PERMISSIONS_FILE, permissions)?;
    let permissions = permissions_file::read(
        &permissions_json,
        history.current_schema(),
        &relations,
        &computed,
    )
    .map_err(|refusal| ArtifactError::at(PERMISSIONS_FILE, refusal))?;
    let (bundled, warnings) = with_system_policies(&history, &permissions)?;
    let bundle = compute_bundle(history.current_schema(), &bundled, &computed)?;
    codegen.encryption = history.declared();
    Ok(LoadedApp {
        history,
        bundle,
        permissions,
        codegen,
        warnings,
    })
}

/// `jazz.app.json` checked as [`load`] checks it, without permissions: what
/// code generators and `jazz validate` read.
#[derive(Debug, Clone)]
pub struct AppDefinition {
    relations: relations::Relations,
    history: History,
    codegen: Codegen,
    warnings: Vec<String>,
}

impl AppDefinition {
    pub fn current_hash(&self) -> SchemaHash {
        self.history.current
    }

    /// The current schema, without permissions.
    pub fn current_schema(&self) -> &Schema {
        self.history.current_schema()
    }

    /// Every version the file ships, oldest first, the current one last.
    pub fn schemas(&self) -> &[(SchemaHash, Schema)] {
        &self.history.schemas
    }

    /// The migrations, in the form a deployment publishes them.
    pub fn migrations(&self) -> &[PublishMigrationRequest] {
        &self.history.migrations
    }

    pub fn codegen(&self) -> &Codegen {
        &self.codegen
    }

    /// The app's encryption declarations, every version's.
    pub fn encryption(&self) -> &DeclaredEncryption {
        &self.history.encryption
    }

    /// Every table's relations, resolved ([`relations::resolve_relations`]).
    pub fn relations(&self) -> &relations::Relations {
        &self.relations
    }

    /// What loaded but is ignored, as `jazz.app.json: <path>: <why>` lines.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
    }

    /// History versions no chain of migrations connects to the current one
    /// (and whose rows would need transforming), as refusals: also among
    /// [`AppDefinition::warnings`]. `generate` refuses a file with any, since
    /// bindings must not ship a version clients couldn't open;
    /// `migrations create` writes what connects them.
    pub fn stranded(&self) -> &[String] {
        &self.history.stranded
    }

    /// The bundle a client ships (JSON: `{schemas, migrations}`, what
    /// [`published::client_migrations`](super::published::client_migrations)
    /// lowers), exactly as [`load_client`] lowers it from the file.
    pub fn client_bundle(&self) -> ClientMigrations {
        self.history.bundle()
    }

    /// The schema versions and lenses a client opens along.
    pub fn client_migrations(&self) -> crate::db::Migrations {
        self.history.client_migrations()
    }
}

/// Read and check `jazz.app.json` alone: references, lenses as a deployment
/// checks them, relations, computed fields and transforms.
pub fn load_app(json: &[u8]) -> Result<AppDefinition, ArtifactError> {
    let app_file = read_app(json)?;
    let history = check_history(&app_file)?;
    check_decoded_client_migrations(history.bundle(), history.current)
        .map_err(|error| ArtifactError::new(format!("migrations: {error}")))?;
    let relations = check_codegen(&app_file.codegen, history.current_schema())?;
    // Lowering for a client must succeed too.
    lower_client_migrations(history.bundle())
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    let stranded = history.stranded.clone();
    let mut codegen = app_file.codegen;
    codegen.encryption = history.declared();
    Ok(AppDefinition {
        relations,
        history,
        codegen,
        warnings: app_file
            .warnings
            .into_iter()
            .map(|w| format!("{APP_FILE}: {w}"))
            .chain(stranded)
            .collect(),
    })
}

/// The `source` generated bindings record: `sha256:` and the hex SHA-256 of
/// the file's canonical text ([`json::print`] of the parsed file), so
/// reformatting it (indentation, line ends, a BOM) leaves its source alone
/// and only a change of content counts. A file that isn't JSON is hashed as
/// it is.
pub fn source_hash(file: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let file = file.strip_prefix("\u{feff}".as_bytes()).unwrap_or(file);
    let canonical = json::parse(file).map(|root| json::print(&root));
    let bytes = match &canonical {
        Ok(text) => text.as_bytes(),
        Err(_) => file,
    };
    format!("sha256:{}", hex::encode(Sha256::digest(bytes)))
}

/// The top-level key of the marker a tool writes into the files it
/// generates from TypeScript: `{"by": "<text>", "sha256": "<hex>"}`.
pub const GENERATED_KEY: &str = "$generated";

/// The `by` of the marker `npx jazz-tools generate` writes.
pub const GENERATED_BY_TS: &str =
    "jazz-tools generate from schema.ts — edit the TypeScript, not this file";

/// A file's `$generated` marker.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GeneratedMarker {
    /// What wrote the file, for people reading it.
    pub by: String,
    /// The hex SHA-256 of the file's canonical text without the marker
    /// ([`generated_digest`]) when it was written.
    pub sha256: String,
}

/// What [`verify_generated`] found in a file.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GeneratedCheck {
    /// No `$generated`: written by hand (or by an older tool).
    Unmarked,
    /// The marker's digest matches: the file is as its tool wrote it, give
    /// or take formatting.
    Matches(GeneratedMarker),
    /// The marker's digest doesn't match: the file was edited since.
    Mismatch {
        marker: GeneratedMarker,
        /// The digest of the file as it is.
        actual: String,
    },
}

/// The `$generated` marker of a parsed file, checked: `None` when there is
/// none, a refusal (at `$generated`) when it isn't
/// `{"by": "<text>", "sha256": "<64 lowercase hex>"}`.
fn generated_marker(root: &Json) -> Result<Option<GeneratedMarker>, String> {
    const SHAPE: &str =
        "must be { \"by\": \"<text>\", \"sha256\": \"<64 lowercase hex characters>\" }";
    let Some(value) = root.get(GENERATED_KEY) else {
        return Ok(None);
    };
    let refuse = |what: String| format!("{GENERATED_KEY}: {what}");
    let entries = value
        .as_object()
        .ok_or_else(|| refuse(format!("{SHAPE}, not {}", value.kind())))?;
    for (key, _) in entries {
        if key != "by" && key != "sha256" {
            return Err(refuse(format!("unknown key `{key}`; {SHAPE}")));
        }
    }
    let by = value.get("by").and_then(Json::as_str);
    let sha256 = value.get("sha256").and_then(Json::as_str);
    match (by, sha256) {
        (Some(by), Some(sha256))
            if sha256.len() == 64
                && sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b)) =>
        {
            Ok(Some(GeneratedMarker {
                by: by.to_string(),
                sha256: sha256.to_string(),
            }))
        }
        _ => Err(refuse(SHAPE.to_string())),
    }
}

/// The digest a `$generated` marker records: the hex SHA-256 of the file's
/// canonical text ([`json::print`], the writer's) with the `$generated`
/// member removed, i.e. of exactly what the writer produces without the
/// marker.
pub fn generated_digest(root: &Json) -> String {
    let unmarked = match root {
        Json::Object(entries) => Json::Object(
            entries
                .iter()
                .filter(|(key, _)| key != GENERATED_KEY)
                .cloned()
                .collect(),
        ),
        other => other.clone(),
    };
    sha256_hex(json::print(&unmarked).as_bytes())
}

fn parse_file_text(file: &[u8]) -> Result<Json, String> {
    let file = file.strip_prefix("\u{feff}".as_bytes()).unwrap_or(file);
    let root = json::parse(file)?;
    if root.as_object().is_none() {
        return Err(format!("must be a JSON object, not {}", root.kind()));
    }
    Ok(root)
}

/// `file` (a `jazz.app.json` or `jazz.permissions.json` text) printed
/// canonically with a `$generated` marker saying it was written `by`, first
/// after `$schema` (first of all when there is no `$schema`); an existing
/// marker is replaced.
pub fn mark_generated(file: &[u8], by: &str) -> Result<String, String> {
    let root = parse_file_text(file)?;
    let digest = generated_digest(&root);
    let Json::Object(mut entries) = root else {
        unreachable!("checked by parse_file_text");
    };
    entries.retain(|(key, _)| key != GENERATED_KEY);
    let at = usize::from(entries.first().is_some_and(|(key, _)| key == "$schema"));
    entries.insert(
        at,
        (
            GENERATED_KEY.to_string(),
            Json::object(vec![
                ("by", Json::string(by)),
                ("sha256", Json::string(digest)),
            ]),
        ),
    );
    Ok(json::print(&Json::Object(entries)))
}

/// Whether `file` (a `jazz.app.json` or `jazz.permissions.json` text) is as
/// the tool its `$generated` marker names wrote it: no marker, a matching
/// digest, or a mismatch (it was edited since). Formatting alone (indent,
/// line ends, a BOM) doesn't count as an edit. A file that isn't a JSON
/// object, or whose marker is malformed, is refused.
pub fn verify_generated(file: &[u8]) -> Result<GeneratedCheck, String> {
    let root = parse_file_text(file)?;
    let Some(marker) = generated_marker(&root)? else {
        return Ok(GeneratedCheck::Unmarked);
    };
    let actual = generated_digest(&root);
    Ok(if actual == marker.sha256 {
        GeneratedCheck::Matches(marker)
    } else {
        GeneratedCheck::Mismatch { marker, actual }
    })
}

/// The hex SHA-256 of `bytes`, for digests generators record (a TS
/// module's output digest).
pub fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    hex::encode(Sha256::digest(bytes))
}

/// The engine's hash of a schema in its own JSON form (`{ "tables": … }`,
/// what generated bindings embed per version), its policies dropped as a
/// deployment drops them: the one schema-hash implementation every
/// language uses.
pub fn schema_hash(schema_json: &[u8]) -> Result<SchemaHash, ArtifactError> {
    let schema: Schema = serde_json::from_slice(schema_json)
        .map_err(|error| ArtifactError::new(format!("not an engine schema: {error}")))?;
    Ok(SchemaHash::compute(&structural(schema)))
}

/// [`super::schemas_are_compatible`] of two schemas in the engine's JSON
/// form, as [`schema_hash`] reads them.
pub fn schemas_are_compatible(left_json: &[u8], right_json: &[u8]) -> Result<bool, ArtifactError> {
    let read = |json: &[u8]| {
        serde_json::from_slice::<Schema>(json)
            .map_err(|error| ArtifactError::new(format!("not an engine schema: {error}")))
    };
    Ok(super::schemas_are_compatible(
        &read(left_json)?,
        &read(right_json)?,
    ))
}

/// Read a `jazz.app.json` for a client: the schema versions and lenses it
/// opens along. Its references are checked as [`load`] checks them and
/// every lens is lowered; the deployment's review of each lens is the
/// writer's and every backend's, and the codegen metadata a code
/// generator's.
pub fn load_client(json: &[u8]) -> Result<crate::db::Migrations, ArtifactError> {
    let app_file = read_app(json)?;
    let history = check_history(&app_file)?;
    let (schemas, lenses) = lower_client_migrations(history.bundle())
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    Ok(crate::db::Migrations { schemas, lenses })
}

/// [`load_client`], with the current schema: what a client that has no
/// other copy of the schema opens with.
///
/// A client has no encryption hooks yet, so it refuses an app whose current
/// version declares encryption ([`load_client_encrypted`] reads one for a
/// client that installs them).
pub fn load_client_current(json: &[u8]) -> Result<(Schema, crate::db::Migrations), ArtifactError> {
    let app_file = read_app(json)?;
    let history = check_history(&app_file)?;
    if has_system_tables(history.current_schema()) {
        return Err(ArtifactError::new(
            crate::binding_host::ENCRYPTED_APP_REFUSAL,
        ));
    }
    let (schemas, lenses) = lower_client_migrations(history.bundle())
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    Ok((
        history.current_schema().clone(),
        crate::db::Migrations { schemas, lenses },
    ))
}

/// [`load_client_current`], with the current version's encryption
/// declarations, for a client of app `app_id` in environment `environment`
/// (both part of every cell's domain): what a client opening an encrypted
/// app builds its encryption hooks from. The declarations are the current
/// version's own, with no table when it keeps Jazz's system tables without
/// declaring encryption (it is still an encrypted app); `None` when it has
/// no system tables.
pub fn load_client_encrypted(
    json: &[u8],
    app_id: crate::tools::AppId,
    environment: &str,
) -> Result<
    (
        Schema,
        crate::db::Migrations,
        Option<crate::tools::public_schema::E2eeDeclarations>,
    ),
    ArtifactError,
> {
    let app_file = read_app(json)?;
    let history = check_history(&app_file)?;
    let (schemas, lenses) = lower_client_migrations(history.bundle())
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    // The version's own declarations: a version that keeps the system tables
    // without declaring encryption declares no encrypted column (those of an
    // earlier version are not its own: it may have dropped or decrypted
    // them), but it is still an encrypted app, with its system tables.
    let own = history.encryption.entries(&history.current.to_string());
    let declarations = (own.is_some() || has_system_tables(history.current_schema())).then(|| {
        crate::tools::public_schema::E2eeDeclarations {
            app_id,
            environment: environment.to_owned(),
            settings: history.encryption.settings.clone(),
            tables: own.cloned().unwrap_or_default(),
        }
    });
    Ok((
        history.current_schema().clone(),
        crate::db::Migrations { schemas, lenses },
        declarations,
    ))
}

/// The current version's encryption declarations from the compiled app
/// generated bindings embed (JSON of `{ schema, relations }`, the options
/// they pass to `bindApp`), for a client of app `app_id` in environment
/// `environment`: what a client that has the bindings but not
/// `jazz.app.json` builds its encryption hooks from. Checked as
/// [`load_backend`] checks them. `None` when the current version has no
/// system tables; one that keeps them without declaring encryption gives
/// declarations with no table.
pub fn bound_declarations(
    bound: &[u8],
    app_id: crate::tools::AppId,
    environment: &str,
) -> Result<Option<crate::tools::public_schema::E2eeDeclarations>, ArtifactError> {
    let refuse = |message: String| {
        ArtifactError::new(format!(
            "the bindings' compiled app: {message}; regenerate them with `npx jazz-tools generate`"
        ))
    };
    let bound: BoundApp =
        serde_json::from_slice(bound).map_err(|error| refuse(error.to_string()))?;
    let current = parse_schema_hash(&bound.schema.current).map_err(refuse)?;
    let schema = bound
        .schema
        .schemas
        .into_iter()
        .find(|entry| parse_schema_hash(&entry.hash).is_ok_and(|hash| hash == current))
        .map(|entry| structural(entry.schema))
        .ok_or_else(|| refuse(format!("no schema {current}, the current one")))?;
    let encryption = bound.schema.encryption.unwrap_or_default();
    // The version's own declarations: a version that keeps Jazz's system
    // tables without declaring encryption is still an encrypted app, with no
    // declared table (as `load_client_encrypted` returns it).
    let own = encryption.entries(&current.to_string());
    if own.is_none() && !has_system_tables(&schema) {
        return Ok(None);
    }
    for (table, entry) in own.into_iter().flatten() {
        encryption::check_entry(table, entry, &schema)
            .map_err(|(at, message)| refuse(format!("tables.{table}.encryption{at}: {message}")))?;
    }
    Ok(Some(crate::tools::public_schema::E2eeDeclarations {
        app_id,
        environment: environment.to_owned(),
        settings: encryption.settings.clone(),
        tables: own.cloned().unwrap_or_default(),
    }))
}

/// The relations `jazz.app.json` declares, resolved against its current
/// schema ([`relations::resolve_relations`]), and nothing else checked: what
/// a client that follows relations itself (the FFI) needs at open.
pub fn load_relations(json: &[u8]) -> Result<relations::Relations, ArtifactError> {
    let app_file = read_app(json)?;
    relations::resolve_relations(&app_file.codegen, &app_file.current)
        .map_err(|(path, message)| ArtifactError::at(APP_FILE, (path, message)))
}

fn read_app(json: &[u8]) -> Result<app_file::AppFile, ArtifactError> {
    let root = read_version(APP_FILE, json)?;
    refuse_todos(&root)?;
    app_file::read(&root).map_err(|refusal| ArtifactError::at(APP_FILE, refusal))
}

/// A string value starting with this is a decision `jazz-tools migrations
/// create` (or `upgrade`) left to the author.
pub const TODO_PREFIX: &str = "TODO:";

/// The JSON paths of every unresolved `"TODO: …"` in `root`, with its
/// text, in file order: in `migrations` (which `jazz-tools migrations create`
/// writes), computed fields' types and transforms' types (which `jazz-tools
/// upgrade` writes). A column default, an enum variant or a JSON Schema may
/// say `TODO:` freely, as may a `$comment`.
pub fn todos(root: &Json) -> Vec<(String, String)> {
    fn walk(value: &Json, path: String, out: &mut Vec<(String, String)>) {
        match value {
            Json::String(text) if text.starts_with(TODO_PREFIX) => {
                out.push((path, text.clone()));
            }
            Json::Array(items) => {
                for (i, item) in items.iter().enumerate() {
                    walk(item, format!("{path}[{i}]"), out);
                }
            }
            Json::Object(entries) => {
                for (key, item) in entries {
                    if key != "$comment" {
                        walk(item, format!("{path}.{key}"), out);
                    }
                }
            }
            _ => {}
        }
    }
    // A type is a placeholder itself, or a `json` type whose schema is one
    // (`{ "json": "TODO: …" }`, as upgrade writes it); a JSON Schema's own
    // text is the author's.
    fn type_todo(value: &Json, path: String, out: &mut Vec<(String, String)>) {
        match value {
            Json::String(text) if text.starts_with(TODO_PREFIX) => out.push((path, text.clone())),
            Json::Object(entries) => {
                for (key, item) in entries {
                    match (key.as_str(), item) {
                        ("json" | "type", Json::String(text)) if text.starts_with(TODO_PREFIX) => {
                            out.push((format!("{path}.{key}"), text.clone()))
                        }
                        ("array", element) => type_todo(element, format!("{path}.array"), out),
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    if let Some(migrations) = root.get("migrations") {
        walk(migrations, "migrations".into(), &mut out);
    }
    if let Some(Json::Object(tables)) = root.get("tables") {
        for (table, spec) in tables {
            if let Some(Json::Object(fields)) = spec.get("computed") {
                for (field, computed) in fields {
                    if let Some(ty) = computed.get("type") {
                        type_todo(
                            ty,
                            format!("tables.{table}.computed.{field}.type"),
                            &mut out,
                        );
                    }
                }
            }
        }
    }
    if let Some(Json::Object(transforms)) = root.get("transforms") {
        for (name, transform) in transforms {
            for side in ["stored", "view"] {
                if let Some(ty) = transform.get(side) {
                    type_todo(ty, format!("transforms.{name}.{side}"), &mut out);
                }
            }
        }
    }
    out
}

/// Refuse a file with unresolved TODOs, naming each.
fn refuse_todos(root: &Json) -> Result<(), ArtifactError> {
    let todos = todos(root);
    if todos.is_empty() {
        return Ok(());
    }
    let list: Vec<String> = todos
        .iter()
        .map(|(path, text)| format!("  {path}: {text}"))
        .collect();
    Err(ArtifactError::new(format!(
        "{} unresolved TODO{} left by `npx jazz-tools migrations create` or `upgrade`; replace each with \
         the value it asks for, then run `npx jazz-tools generate` again:\n{}",
        todos.len(),
        if todos.len() == 1 { "" } else { "s" },
        list.join("\n")
    )))
}

/// The file as JSON, once its version is one this version reads. The version
/// comes first, so a file from a newer Jazz is refused for what it is rather
/// than for the fields this version doesn't know.
///
/// `$schema` carries the version: `https://jazz.tools/schemas/<file>.v<N>.json`.
/// Legacy files (before alpha.66) carry `"format": 1` and/or the unversioned
/// `<file>.schema.json` URL instead; they read as version 1.
fn read_version(file: &'static str, json: &[u8]) -> Result<Json, ArtifactError> {
    let refuse = |message: String| ArtifactError::in_file(file, message);
    let root = json::parse(json).map_err(refuse)?;
    let Some(object) = root.as_object() else {
        return Err(refuse(format!(
            "must be a JSON object, not {}",
            root.kind()
        )));
    };
    // Ignored on load, but well-formed.
    generated_marker(&root).map_err(refuse)?;
    let (url, legacy_url) = if file == APP_FILE {
        (APP_SCHEMA_URL, LEGACY_APP_SCHEMA_URL)
    } else {
        (PERMISSIONS_SCHEMA_URL, LEGACY_PERMISSIONS_SCHEMA_URL)
    };
    let newer = |version: &str| {
        refuse(format!(
            "version {version} was written by a newer Jazz; this version of Jazz reads version \
             {VERSION}. Upgrade jazz-tools"
        ))
    };
    let get = |key: &str| object.iter().find(|(k, _)| k == key).map(|(_, v)| v);
    let format = match get("format") {
        None => None,
        Some(Json::Number(n)) if n == &VERSION.to_string() => Some(VERSION),
        Some(Json::Number(n)) if n.parse::<u64>().is_ok_and(|n| n > u64::from(VERSION)) => {
            return Err(newer(n));
        }
        Some(Json::Number(n)) if n.parse::<u64>().is_ok() => {
            return Err(refuse(format!(
                "version {n} is not supported; this version of Jazz reads version {VERSION}"
            )));
        }
        Some(other) => {
            return Err(refuse(format!(
                "the legacy `format` must be the integer {VERSION}, not {}; drop it and name \
                 \"$schema\": \"{url}\"",
                json::print(other).trim_end()
            )));
        }
    };
    let file_stem = file.trim_end_matches(".json");
    let versioned = |s: &str| -> Option<String> {
        s.strip_prefix(SCHEMA_URL_BASE)
            .and_then(|rest| rest.strip_prefix(file_stem))
            .and_then(|rest| rest.strip_prefix(".v"))
            .and_then(|rest| rest.strip_suffix(".json"))
            .filter(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()))
            .map(str::to_string)
    };
    match get("$schema") {
        // A legacy file: before alpha.66 `$schema` was only an editor hint, so any
        // value (a relative path, another URL, null) loaded next to `format`.
        // Only a versioned URL naming another version contradicts it.
        Some(Json::String(s)) if format.is_some() && versioned(s).is_none() => Ok(root),
        Some(other) if format.is_some() && !matches!(other, Json::String(_)) => Ok(root),
        None if format.is_some() => Ok(root),
        None => Err(refuse(format!(
            "no `$schema`; the file starts {{ \"$schema\": \"{url}\", … }}. Run `npx \
             jazz-tools generate` to write it"
        ))),
        Some(Json::String(s)) if s == legacy_url => Ok(root),
        Some(Json::String(s)) => match versioned(s) {
            Some(v) if v == VERSION.to_string() => Ok(root),
            Some(v)
                if format.is_none() && v.parse::<u64>().is_ok_and(|n| n > u64::from(VERSION)) =>
            {
                Err(newer(&v))
            }
            Some(v) if format.is_none() => Err(refuse(format!(
                "version {v} is not supported; this version of Jazz reads version {VERSION}"
            ))),
            Some(v) => Err(refuse(format!(
                "`$schema` names version {v} but the legacy `format` is {VERSION}; drop \
                     `format`"
            ))),
            None => Err(refuse(format!("`$schema` must be \"{url}\", not \"{s}\""))),
        },
        Some(other) => Err(refuse(format!(
            "`$schema` must be the string \"{url}\", not {}",
            json::print(other).trim_end()
        ))),
    }
}

/// What [`load`] and [`load_client`] check of the app file's history: its
/// schema versions and the migrations between them.
#[derive(Debug, Clone)]
struct History {
    current: SchemaHash,
    /// Oldest first, policy-free, the current one last.
    schemas: Vec<(SchemaHash, Schema)>,
    migrations: Vec<PublishMigrationRequest>,
    /// Versions no chain of migrations connects to the current one, and
    /// whose rows would need transforming: why, for each. Loaders warn
    /// (`jazz-tools validate` in .64 wrote such files while a migration was
    /// still to be written); `generate` refuses them.
    stranded: Vec<String>,
    /// The settings and each version's encryption entries.
    encryption: DeclaredEncryption,
}

impl History {
    /// The declarations, as codegen metadata carries them: `None` when
    /// nothing is declared.
    fn declared(&self) -> Option<DeclaredEncryption> {
        (!self.encryption.is_empty()).then(|| self.encryption.clone())
    }

    fn bundle(&self) -> ClientMigrations {
        ClientMigrations {
            schemas: self
                .schemas
                .iter()
                .map(|(hash, schema)| PublishedSchema {
                    hash: hash.to_string(),
                    schema: schema.clone(),
                })
                .collect(),
            migrations: self.migrations.to_vec(),
        }
    }

    fn current_schema(&self) -> &Schema {
        &self
            .schemas
            .iter()
            .find(|(hash, _)| *hash == self.current)
            .expect("checked by check_history")
            .1
    }

    fn client_migrations(&self) -> crate::db::Migrations {
        let (schemas, lenses) = lower_client_migrations(self.bundle()).expect("checked on load");
        crate::db::Migrations { schemas, lenses }
    }
}

/// Hash every version (the file names none), refuse repeats, and give each
/// migration its endpoints' hashes.
fn check_history(file: &app_file::AppFile) -> Result<History, ArtifactError> {
    // A version that declares encryption has Jazz's system tables, and so
    // does every version a migration leads to from one that has them, even
    // one that declares no encryption: once added they stay (an earlier
    // version's system rows would be stranded otherwise). Exactly the
    // server's rule, which follows the migrations, not the order the file
    // lists the versions in: a version no migration leads to from one that
    // has them keeps what it declares, so a plain sibling gets no tables
    // silently (a server holding the tables then refuses it, plainly). Every
    // loader computes the same schema and hash from the same file.
    const CURRENT: &str = "\u{0}current";
    let mut has: BTreeMap<&str, bool> = file
        .history
        .iter()
        .zip(&file.history_encryption)
        .map(|((name, _), entries)| (name.as_str(), !entries.is_empty()))
        .collect();
    has.insert(CURRENT, !file.current_encryption.is_empty());
    loop {
        let mut changed = false;
        for (from, to, _) in &file.migrations {
            let to = to.as_deref().unwrap_or(CURRENT);
            if has.get(from.as_str()).copied().unwrap_or(false)
                && !has.get(to).copied().unwrap_or(false)
            {
                has.insert(to, true);
                changed = true;
            }
        }
        if !changed {
            break;
        }
    }
    let with_fragment = |schema: &Schema, carries: bool| {
        if carries {
            system_tables::with_e2ee_fragment(schema)
        } else {
            schema.clone()
        }
    };
    let history_schemas: Vec<Schema> = file
        .history
        .iter()
        .map(|(name, schema)| with_fragment(schema, has[name.as_str()]))
        .collect();
    let current_schema = with_fragment(&file.current, has[CURRENT]);
    let current = SchemaHash::compute(&current_schema);
    let mut encryption = DeclaredEncryption {
        settings: file.settings.clone().unwrap_or_default(),
        versions: BTreeMap::new(),
    };
    if !file.current_encryption.is_empty() {
        encryption
            .versions
            .insert(current.to_string(), file.current_encryption.clone());
    }
    let mut schemas: Vec<(SchemaHash, Schema)> = Vec::new();
    let mut by_name = BTreeMap::new();
    for (((name, _), entries), schema) in file
        .history
        .iter()
        .zip(&file.history_encryption)
        .zip(&history_schemas)
    {
        let hash = SchemaHash::compute(schema);
        if !entries.is_empty() {
            encryption
                .versions
                .insert(hash.to_string(), entries.clone());
        }
        if hash == current {
            return Err(ArtifactError::new(format!(
                "history.{name} is the current schema; a version appears once"
            )));
        }
        if let Some((other, _)) = by_name.iter().find(|(_, h)| **h == hash) {
            return Err(ArtifactError::new(format!(
                "history.{name} repeats history.{other}"
            )));
        }
        by_name.insert(name.clone(), hash);
        schemas.push((hash, schema.clone()));
    }
    schemas.push((current, current_schema));
    let migrations = file
        .migrations
        .iter()
        .map(|(from, to, forward)| {
            let from_hash = by_name[from].to_string();
            let to_hash = to.as_ref().map_or(current, |to| by_name[to]).to_string();
            let mut forward = forward.clone();
            let carries = |hash: &str| {
                schemas
                    .iter()
                    .find(|(h, _)| h.to_string() == hash)
                    .is_some_and(|(_, schema)| has_system_tables(schema))
            };
            forward.extend(system_table_lenses(carries(&from_hash), carries(&to_hash)));
            PublishMigrationRequest {
                from_hash,
                to_hash,
                forward,
            }
        })
        .collect::<Vec<_>>();
    let schema_of = |hash: &str| {
        let hash = parse_schema_hash(hash).expect("written above");
        &schemas
            .iter()
            .find(|(h, _)| *h == hash)
            .expect("named above")
            .1
    };
    for (index, migration) in migrations.iter().enumerate() {
        if let Some(refusal) = retargeted_reference(
            schema_of(&migration.from_hash),
            schema_of(&migration.to_hash),
            &migration.forward,
        ) {
            return Err(ArtifactError::new(format!(
                "migrations[{index}]: {refusal}"
            )));
        }
        let version = |hash: &str| (schema_of(hash), encryption.entries(hash));
        encryption::check_transition(
            version(&migration.from_hash),
            version(&migration.to_hash),
            migration,
        )
        .map_err(|refusal| ArtifactError::new(format!("migrations[{index}]: {refusal}")))?;
    }
    // Every version a client may open must lead to the current one: lenses
    // run both ways, so any chain of migrations connects them. Before
    // alpha.66, `migrations create` wrote no migration when no row needed
    // transforming (an index, merge strategy or column order changed), so a
    // version such files hold may stand alone: it is joined to the next
    // version its rows pass to unchanged by the identity step it implies.
    let mut migrations = migrations;
    let reached_from = |migrations: &[PublishMigrationRequest]| {
        let mut reached = vec![current];
        loop {
            let before = reached.len();
            for migration in migrations {
                let (from, to) = (
                    parse_schema_hash(&migration.from_hash).expect("written above"),
                    parse_schema_hash(&migration.to_hash).expect("written above"),
                );
                if reached.contains(&from) != reached.contains(&to) {
                    reached.push(if reached.contains(&from) { to } else { from });
                }
            }
            if reached.len() == before {
                return reached;
            }
        }
    };
    let mut stranded = Vec::new();
    let mut stranded_hashes = Vec::new();
    loop {
        let reached = reached_from(&migrations);
        let Some(index) = schemas
            .iter()
            .position(|(hash, _)| !reached.contains(hash) && !stranded_hashes.contains(hash))
        else {
            break;
        };
        let (hash, schema) = &schemas[index];
        // The next version, in file order (history, then the current one),
        // that this one's rows pass to unchanged.
        let bridge = schemas[index + 1..]
            .iter()
            .find(|(later, other)| reached.contains(later) && rows_pass_unchanged(schema, other));
        let Some((later, other)) = bridge else {
            let name = by_name
                .iter()
                .find(|(_, h)| *h == hash)
                .map(|(n, _)| n.clone())
                .expect("a history version");
            stranded.push(format!(
                "{APP_FILE}: history.{name}: no migration leads from it to the current `tables`, \
                 directly or through other versions, so clients holding its rows couldn't open; \
                 add one (`npx jazz-tools migrations create` writes it) or remove the version"
            ));
            stranded_hashes.push(*hash);
            continue;
        };
        migrations.push(PublishMigrationRequest {
            from_hash: hash.to_string(),
            to_hash: later.to_string(),
            forward: identity_lenses(schema, other),
        });
    }
    Ok(History {
        current,
        schemas,
        migrations,
        stranded,
        encryption,
    })
}

/// Whether `schema` holds Jazz's system tables.
fn has_system_tables(schema: &Schema) -> bool {
    schema
        .keys()
        .any(|name| system_tables::is_system_table(name.as_str()))
}

/// The declarations the system tables of `version` follow: its own if it
/// declares encryption, else those of the nearest version before it (by the
/// migrations, breadth first) that does, since a version that keeps the
/// system tables without declaring encryption keeps its spaces too. `None`
/// when none does; equally near versions that declare differently leave no
/// nearest one and are refused, as the deploy's check does.
fn governing_entries<'a>(
    encryption: &'a DeclaredEncryption,
    migrations: &[PublishMigrationRequest],
    version: SchemaHash,
) -> Result<Option<&'a encryption::Entries>, ArtifactError> {
    let mut level = vec![version];
    let mut seen = vec![version];
    while !level.is_empty() {
        let declared: Vec<(SchemaHash, &encryption::Entries)> = level
            .iter()
            .filter_map(|hash| {
                encryption
                    .entries(&hash.to_string())
                    .map(|entries| (*hash, entries))
            })
            .collect();
        if let Some((first_hash, first)) = declared.first() {
            if let Some((other, _)) = declared.iter().find(|(_, entries)| entries != first) {
                return Err(ArtifactError::new(format!(
                    "the schema keeps Jazz's system tables without declaring encryption, and \
                     the versions it follows nearest declare differently ({first_hash} and \
                     {other}): declare encryption in it"
                )));
            }
            return Ok(Some(first));
        }
        level = level
            .iter()
            .flat_map(|hash| {
                migrations
                    .iter()
                    .filter(move |migration| migration.to_hash == hash.to_string())
                    .filter_map(|migration| parse_schema_hash(&migration.from_hash).ok())
            })
            .filter(|hash| {
                let fresh = !seen.contains(hash);
                if fresh {
                    seen.push(*hash);
                }
                fresh
            })
            .collect();
    }
    Ok(None)
}

/// The table lenses that add Jazz's system tables along a migration into a
/// version that has them from one that doesn't, or remove them the other
/// way.
fn system_table_lenses(from: bool, to: bool) -> Vec<super::published::PublishTableLens> {
    if from == to {
        return Vec::new();
    }
    system_tables::e2ee_system_tables_v1()
        .keys()
        .map(|table| super::published::PublishTableLens {
            table: table.as_str().to_owned(),
            added: to,
            removed: from,
            renamed_from: None,
            operations: Vec::new(),
        })
        .collect()
}

/// Whether every row of `from` is a row of `to` as it is: the same tables,
/// each with the same columns (by name) of the same type and optionality,
/// an enum perhaps gaining variants at the end. What may differ (indexes,
/// history retention, merge strategies, defaults, references, column order)
/// needs no step.
fn rows_pass_unchanged(from: &Schema, to: &Schema) -> bool {
    fn same_type(a: &ColumnType, b: &ColumnType) -> bool {
        match (a, b) {
            (ColumnType::Enum { variants: x }, ColumnType::Enum { variants: y }) => {
                y.starts_with(x)
            }
            (ColumnType::Array { element: x }, ColumnType::Array { element: y }) => same_type(x, y),
            _ => a == b,
        }
    }
    from.len() == to.len()
        && from.iter().all(|(name, table)| {
            let Some(other) = to.get(name) else {
                return false;
            };
            table.columns.columns.len() == other.columns.columns.len()
                && table.columns.columns.iter().all(|column| {
                    other.columns.columns.iter().any(|c| {
                        c.name == column.name
                            && c.nullable == column.nullable
                            && same_type(&column.column_type, &c.column_type)
                            && !retargets(column, c)
                    })
                })
        })
}

/// Whether a kept column's reference moves from one table to another: its
/// values would name rows of the old table. Gaining a reference is fine.
fn retargets(
    from: &crate::tools::public_schema::ColumnDescriptor,
    to: &crate::tools::public_schema::ColumnDescriptor,
) -> bool {
    matches!((&from.references, &to.references), (Some(a), Some(b)) if a != b)
}

/// The written migration's first kept column whose reference it moves to
/// another table, as a refusal.
fn retargeted_reference(
    from: &Schema,
    to: &Schema,
    forward: &[super::published::PublishTableLens],
) -> Option<String> {
    for (name, table) in to.iter() {
        let lens = forward.iter().find(|l| l.table == name.as_str());
        if lens.is_some_and(|l| l.added) {
            continue;
        }
        let old_name = lens
            .and_then(|l| l.renamed_from.clone())
            .unwrap_or_else(|| name.as_str().to_string());
        let Some(old) = from.get(&TableName::from(old_name.as_str())) else {
            continue;
        };
        for column in &table.columns.columns {
            let old_column = lens
                .and_then(|l| {
                    l.operations.iter().find_map(|op| match op {
                        super::published::PublishLensOp::Rename { column: was, value }
                            if value == column.name_str() =>
                        {
                            Some(was.clone())
                        }
                        _ => None,
                    })
                })
                .unwrap_or_else(|| column.name_str().to_string());
            // A reference to a table the migration renames follows it.
            let followed = |table: &TableName| -> TableName {
                forward
                    .iter()
                    .find(|l| l.renamed_from.as_deref() == Some(table.as_str()))
                    .map_or(*table, |l| TableName::from(l.table.as_str()))
            };
            if let Some(source) = old
                .columns
                .columns
                .iter()
                .find(|c| c.name_str() == old_column)
                && matches!(
                    (&source.references, &column.references),
                    (Some(a), Some(b)) if followed(a) != *b
                )
            {
                return Some(format!(
                    "column {name}.{} moves its reference from {} to {}, which is not supported \
                     yet: its values name rows of {}. Add a new column referencing {} and drop \
                     this one in a later release",
                    column.name_str(),
                    source.references.expect("retargets").as_str(),
                    column.references.expect("retargets").as_str(),
                    source.references.expect("retargets").as_str(),
                    column.references.expect("retargets").as_str(),
                ));
            }
        }
    }
    None
}

/// The identity step between two versions [`rows_pass_unchanged`] relates:
/// an empty lens for each table that differs.
fn identity_lenses(from: &Schema, to: &Schema) -> Vec<super::published::PublishTableLens> {
    to.iter()
        .filter(|(name, table)| {
            from.get(name).is_some_and(|old| {
                serde_json::to_value(old).ok() != serde_json::to_value(table).ok()
            })
        })
        .map(|(name, _)| super::published::PublishTableLens {
            table: name.as_str().to_string(),
            added: false,
            removed: false,
            renamed_from: None,
            operations: Vec::new(),
        })
        .collect()
}

/// The bundle id the server computes when this backend publishes: the
/// permissions are compiled against the current schema (refusing those
/// that don't compile), then hashed as the catalogue hashes them.
fn compute_bundle(
    current: &Schema,
    permissions: &Permissions,
    computed: &expr::ComputedNames,
) -> Result<BundleId, ArtifactError> {
    let mut target = current.clone();
    for (name, policies) in permissions {
        target
            .get_mut(name)
            .expect("checked against the current schema")
            .policies = policies.clone();
    }
    let refuse = |error: String| ArtifactError::in_file(PERMISSIONS_FILE, error);
    let compiled = JazzSchema::new(&target).map_err(|error| {
        // A refused policy is named as this file names it, by its table
        // and operation: the engine's path names its own slots.
        let rule = error.rule().and_then(|(table, slot)| {
            let policies = permissions.get(&TableName::from(table))?;
            Some(format!("tables.{table}.{}", rule_path(policies, slot)?))
        });
        let mut message = match rule {
            Some(rule) => format!("{rule}: {}", error.message()),
            None => format!("permissions: {error}"),
        };
        // The engine checks a relation's `outer.` columns, which the loader
        // can't place; say so when the one it misses is a computed field.
        if let Some((_, field)) = error
            .unknown_column()
            .filter(|(table, field)| computed.get(*table).is_some_and(|f| f.contains(*field)))
        {
            message.push_str(&format!(
                ": `{field}` is a computed field, which isn't stored, so a permission can't \
                 read it; write the policy over the columns it's computed from"
            ));
        }
        refuse(message)
    })?;
    bundle_id(
        &SchemaHash::compute(&structural(compiled.public_schema().clone())),
        &permissions_of(compiled.public_schema()),
    )
    .map_err(|error| refuse(format!("permissions: {error}")))
}

/// Where the permissions file holds the engine's policy `slot`
/// (`select.using`, `update.with_check`, …) of a table with `policies`:
/// `read`, `update.new`, `read.withCheck`, as [`permissions_file::read`]
/// reads them.
fn rule_path(policies: &TablePolicies, slot: &str) -> Option<String> {
    let (operation, side) = slot.split_once('.')?;
    let two_sided = |policy: &OperationPolicy, usual_using: bool| {
        if usual_using {
            policy.with_check.is_some()
        } else {
            policy.using.is_some()
        }
    };
    let side_key = if side == "using" {
        "using"
    } else {
        "withCheck"
    };
    Some(match operation {
        "select" | "delete" | "insert" => {
            let (name, policy, usual_using) = match operation {
                "select" => ("read", &policies.select, true),
                "delete" => ("delete", &policies.delete, true),
                _ => ("insert", &policies.insert, false),
            };
            if two_sided(policy, usual_using) {
                format!("{name}.{side_key}")
            } else {
                name.to_string()
            }
        }
        "update" if policies.update.using == policies.update.with_check => "update".to_string(),
        "update" => format!("update.{}", if side == "using" { "old" } else { "new" }),
        _ => return None,
    })
}

fn check_codegen(
    codegen: &Codegen,
    current: &Schema,
) -> Result<relations::Relations, ArtifactError> {
    let resolved = relations::resolve_relations(codegen, current)
        .map_err(|(path, message)| ArtifactError::new(format!("{path}: {message}")))?;
    let table = |path: &str, name: &str| {
        current.get(&TableName::from(name)).ok_or_else(|| {
            ArtifactError::new(format!(
                "{path}: the current schema has no table {name}{}",
                expr::did_you_mean(name, current.keys().map(|t| t.as_str()))
            ))
        })
    };
    let column_of = |schema: &TableSchema, column: &str| relations::column(schema, column).cloned();
    let has_column = |schema: &TableSchema, column: &str| column_of(schema, column).is_some();
    for (name, entry) in &codegen.tables {
        let schema = table(&format!("tables.{name}"), name)?;
        let columns = || schema.columns.columns.iter().map(|c| c.name_str());
        for (index, field) in entry.computed.iter().enumerate() {
            let path = format!("tables.{name}.computed.{}", field.name);
            if field.name == "id" || has_column(schema, &field.name) {
                return Err(ArtifactError::new(format!(
                    "{path}: table {name} already has a column {}; a computed field needs its own name",
                    field.name
                )));
            }
            if entry.relations.contains_key(&field.name) {
                return Err(ArtifactError::new(format!(
                    "{path}: table {name} already has a relation {}; a computed field needs its own name",
                    field.name
                )));
            }
            if entry.computed[..index].iter().any(|f| f.name == field.name) {
                return Err(ArtifactError::new(format!("{path} repeats")));
            }
            for (i, from) in field.from.iter().enumerate() {
                if from != "id" && !has_column(schema, from) {
                    let other_computed = entry.computed.iter().any(|f| &f.name == from);
                    return Err(ArtifactError::new(format!(
                        "{path}.from[{i}]: {}",
                        if other_computed {
                            format!(
                                "`{from}` is a computed field; a computed field is computed from stored columns only"
                            )
                        } else {
                            format!(
                                "table {name} has no column {from}{}",
                                expr::did_you_mean(from, columns())
                            )
                        }
                    )));
                }
                if field.from[..i].contains(from) {
                    return Err(ArtifactError::new(format!(
                        "{path}.from[{i}]: `{from}` is listed twice"
                    )));
                }
            }
        }
        for (column, transform) in &entry.transforms {
            let path = format!("tables.{name}.columns.{column}.transform");
            let descriptor = column_of(schema, column).ok_or_else(|| {
                ArtifactError::new(format!("{path}: table {name} has no column {column}"))
            })?;
            let Some(decl) = codegen.transforms.get(transform) else {
                return Err(ArtifactError::new(format!(
                    "{path}: no transform `{transform}` in the top-level `transforms`{}",
                    expr::did_you_mean(transform, codegen.transforms.keys().map(String::as_str))
                )));
            };
            if decl.stored != descriptor.column_type {
                return Err(ArtifactError::new(format!(
                    "{path}: transform `{transform}` stores {}, but column {column} is {}",
                    type_name(&decl.stored),
                    type_name(&descriptor.column_type)
                )));
            }
        }
    }
    Ok(resolved)
}

/// A schema in the readable form of a `history` version's `tables` (no
/// relations, computed fields or transforms), as the writer prints it.
pub fn version_tables(schema: &Schema) -> Result<Json, ArtifactError> {
    app_file::write_version_tables(schema)
        .map_err(|error| ArtifactError::unsupported(APP_FILE, error))
}

/// [`version_tables`] of a schema in the engine's JSON form, as
/// [`schema_hash`] reads it, printed.
pub fn version_tables_json(schema_json: &[u8]) -> Result<String, ArtifactError> {
    let schema: Schema = serde_json::from_slice(schema_json)
        .map_err(|error| ArtifactError::new(format!("not an engine schema: {error}")))?;
    Ok(json::print(&version_tables(&schema)?))
}

/// A column type in the file's form (`"string"`, `{ "enum": […] }`).
pub fn column_type_spec(ty: &ColumnType) -> Result<Json, ArtifactError> {
    app_file::type_spec(ty).map_err(|error| ArtifactError::unsupported(APP_FILE, error))
}

/// A value of `ty` in the file's form (a column `default`).
pub fn value_spec(
    value: &crate::tools::public_schema::Value,
    ty: &ColumnType,
) -> Result<Json, ArtifactError> {
    app_file::value_spec(value, ty).map_err(|error| ArtifactError::unsupported(APP_FILE, error))
}

/// A column type as the file writes it, for messages.
fn type_name(ty: &ColumnType) -> String {
    app_file::type_spec(ty).map_or_else(
        |_| format!("{ty:?}"),
        |json| json::print(&json).trim_end().to_string(),
    )
}

// ============================================================================
// Writing
// ============================================================================

/// What `jazz-tools validate` hands over after compiling the app's
/// TypeScript, in the engine's own JSON forms.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactInput {
    /// Hash of the schema the code is written against.
    pub current: String,
    /// Every version the app ships, oldest first. Policies embedded in a
    /// schema are dropped, as a deployment drops them.
    pub schemas: Vec<Schema>,
    #[serde(default)]
    pub migrations: Vec<PublishMigrationRequest>,
    #[serde(default)]
    pub permissions: Permissions,
    #[serde(default)]
    pub codegen: Codegen,
}

/// The two files [`write`] renders, as canonical text.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WrittenApp {
    /// `jazz.app.json`, which the app and its clients ship.
    pub app: String,
    /// `jazz.permissions.json`, which only backends read.
    pub permissions: String,
}

/// Build, check and render an app's `jazz.app.json` and
/// `jazz.permissions.json`: the files' only writer. The texts are
/// canonical, so committed files can be compared byte for byte with a
/// fresh build. What the files can't express yet is refused with an error
/// saying "not supported in … yet", naming the table, column or policy; and
/// what they express must read back to exactly the engine input (schema
/// hashes, lenses, permissions), or it is refused the same way.
pub fn write(input: ArtifactInput) -> Result<WrittenApp, ArtifactError> {
    let current_hash = parse_schema_hash(&input.current)
        .map_err(|error| ArtifactError::new(format!("current: {error}")))?;
    let schemas: Vec<(SchemaHash, Schema)> = input
        .schemas
        .into_iter()
        .map(|schema| {
            let schema = structural(schema);
            (SchemaHash::compute(&schema), schema)
        })
        .collect();
    let Some(current) = schemas
        .iter()
        .find(|(hash, _)| *hash == current_hash)
        .map(|(_, schema)| schema.clone())
    else {
        return Err(ArtifactError::new(format!(
            "current schema {current_hash} is not among the schemas given"
        )));
    };
    let names: Vec<(SchemaHash, String)> = schemas
        .iter()
        .filter(|(hash, _)| *hash != current_hash)
        .map(|(hash, _)| (*hash, app_file::version_name(hash)))
        .collect();
    let encryption = input.codegen.encryption.clone().unwrap_or_default();
    let entries_of = |hash: &SchemaHash| {
        encryption
            .versions
            .get(&hash.to_string())
            .cloned()
            .unwrap_or_default()
    };
    let history: Vec<(String, &Schema, app_file::Entries)> = schemas
        .iter()
        .filter(|(hash, _)| *hash != current_hash)
        .map(|(hash, schema)| (app_file::version_name(hash), schema, entries_of(hash)))
        .collect();
    let history: Vec<(String, &Schema, &app_file::Entries)> = history
        .iter()
        .map(|(name, schema, entries)| (name.clone(), *schema, entries))
        .collect();
    let name_of = |hash: &str| -> Result<Option<String>, ArtifactError> {
        let hash = parse_schema_hash(hash).map_err(ArtifactError::new)?;
        if hash == current_hash {
            return Ok(None);
        }
        names
            .iter()
            .find(|(h, _)| *h == hash)
            .map(|(_, name)| Some(name.clone()))
            .ok_or_else(|| {
                ArtifactError::new(format!(
                    "a migration names schema {hash}, which isn't given"
                ))
            })
    };
    let migrations = input
        .migrations
        .iter()
        .map(|m| Ok((name_of(&m.from_hash)?, name_of(&m.to_hash)?, m)))
        .collect::<Result<Vec<_>, ArtifactError>>()?;
    // An inconsistent migration is wrong input; everything else the writers
    // refuse is what the files can't express.
    if let Some(lens) = input
        .migrations
        .iter()
        .flat_map(|m| &m.forward)
        .find(|lens| lens.added && lens.removed)
    {
        return Err(ArtifactError::new(format!(
            "table {} cannot be both added and removed",
            lens.table
        )));
    }
    let app_json = app_file::write(
        (&current, &entries_of(&current_hash)),
        &history,
        &migrations,
        &input.codegen,
        Some(&encryption.settings),
    )
    .map_err(|error| ArtifactError::unsupported(APP_FILE, error))?;
    let relations = input.codegen.relations();
    let permissions: Permissions = input
        .permissions
        .into_iter()
        .filter(|(_, p)| *p != TablePolicies::default())
        .collect();
    let permissions_json = permissions_file::write(&permissions, &current, &relations)
        .map_err(|error| ArtifactError::unsupported(PERMISSIONS_FILE, error))?;
    let written = WrittenApp {
        app: json::print(&with_schema(app_json, APP_SCHEMA_URL)),
        permissions: json::print(&with_schema(permissions_json, PERMISSIONS_SCHEMA_URL)),
    };

    // Read the files back as a backend will.
    let loaded = load(written.app.as_bytes(), written.permissions.as_bytes())?;
    let round_trip = |what: &str| {
        ArtifactError::unsupported(
            APP_FILE,
            expr::unsupported(&format!("{what}, which the file can't express exactly,")),
        )
    };
    if loaded.current_hash() != current_hash {
        return Err(round_trip("the current schema"));
    }
    let given: Vec<SchemaHash> = schemas.iter().map(|(h, _)| *h).collect();
    if loaded
        .schemas()
        .iter()
        .map(|(h, _)| *h)
        .collect::<Vec<_>>()
        .len()
        != given.len()
        || !given
            .iter()
            .all(|h| loaded.schemas().iter().any(|(l, _)| l == h))
    {
        return Err(round_trip("an earlier schema version"));
    }
    for (table, policies) in &permissions {
        if loaded.permissions().get(table) != Some(policies) {
            return Err(ArtifactError::unsupported(
                PERMISSIONS_FILE,
                expr::unsupported(&format!(
                    "tables.{table}: a policy that doesn't read back the same"
                )),
            ));
        }
    }
    if loaded.permissions().len() != permissions.len() {
        return Err(round_trip("the permissions"));
    }
    let normal = |migrations: &[PublishMigrationRequest]| {
        let mut migrations = migrations.to_vec();
        for migration in &mut migrations {
            // The loaders add the system tables' lenses themselves.
            migration
                .forward
                .retain(|lens| !system_tables::is_system_table(&lens.table));
            migration.forward.sort_by(|a, b| a.table.cmp(&b.table));
        }
        serde_json::to_value(migrations).ok()
    };
    if normal(loaded.migrations()) != normal(&input.migrations) {
        return Err(round_trip("a migration"));
    }
    Ok(written)
}

/// [`write`], each file marked as generated `by` a tool (see
/// [`mark_generated`]): what `npx jazz-tools generate` writes from the TS.
pub fn write_generated(input: ArtifactInput, by: &str) -> Result<WrittenApp, ArtifactError> {
    let written = write(input)?;
    let mark = |file: &'static str, text: &str| {
        mark_generated(text.as_bytes(), by).map_err(|error| ArtifactError::in_file(file, error))
    };
    Ok(WrittenApp {
        app: mark(APP_FILE, &written.app)?,
        permissions: mark(PERMISSIONS_FILE, &written.permissions)?,
    })
}

/// An app's two readable files (`app`, `permissions`, as people, agents or
/// the TS tooling write them) checked as [`load`] checks them and
/// re-rendered by [`write`]: the canonical text, which reads back to
/// exactly the same engine input. With `generated_by`, each file carries a
/// `$generated` marker ([`mark_generated`]). Migration `name`s are kept;
/// `$comment`s, a version's own name (the writer names a version by its
/// short schema hash) and what loads only with a warning are not.
/// Idempotent: rewriting its output gives the same text.
pub fn rewrite(
    app: &[u8],
    permissions: &[u8],
    generated_by: Option<&str>,
) -> Result<WrittenApp, ArtifactError> {
    let loaded = load(app, permissions)?;
    let input = ArtifactInput {
        current: loaded.current_hash().to_string(),
        schemas: loaded.schemas().iter().map(|(_, s)| s.clone()).collect(),
        migrations: loaded.migrations().to_vec(),
        permissions: loaded.permissions().clone(),
        codegen: loaded.codegen().clone(),
    };
    let mut written = write(input)?;
    written.app = keep_migration_names(app, &written.app);
    if let Some(by) = generated_by {
        let mark = |file: &'static str, text: &str| {
            mark_generated(text.as_bytes(), by).map_err(|error| ArtifactError::in_file(file, error))
        };
        written = WrittenApp {
            app: mark(APP_FILE, &written.app)?,
            permissions: mark(PERMISSIONS_FILE, &written.permissions)?,
        };
    }
    Ok(written)
}

/// `written` (the writer's `jazz.app.json`) with the migration `name`s of
/// `source`, the file it was loaded from: the writer has no names, and the
/// file's migrations come first, in order, in what loading it gives.
fn keep_migration_names(source: &[u8], written: &str) -> String {
    let source = source.strip_prefix("\u{feff}".as_bytes()).unwrap_or(source);
    let (Ok(source), Ok(Json::Object(mut top))) =
        (json::parse(source), json::parse(written.as_bytes()))
    else {
        return written.to_string();
    };
    let Some(Json::Array(named)) = source.get("migrations") else {
        return written.to_string();
    };
    let Some((_, Json::Array(migrations))) = top.iter_mut().find(|(k, _)| k == "migrations") else {
        return written.to_string();
    };
    let mut changed = false;
    for (from, to) in named.iter().zip(migrations.iter_mut()) {
        if let (Some(name), Json::Object(entries)) = (from.get("name"), to) {
            entries.retain(|(k, _)| k != "name");
            entries.insert(0, ("name".to_string(), name.clone()));
            changed = true;
        }
    }
    if changed {
        json::print(&Json::Object(top))
    } else {
        written.to_string()
    }
}

/// `file` with `$schema` first.
fn with_schema(file: Json, url: &str) -> Json {
    let Json::Object(mut entries) = file else {
        return file;
    };
    entries.insert(0, ("$schema".into(), Json::string(url)));
    Json::Object(entries)
}

#[cfg(test)]
#[path = "artifact_tests.rs"]
mod tests;
