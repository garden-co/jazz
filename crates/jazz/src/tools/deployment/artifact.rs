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
pub mod expr;
pub mod json;
mod permissions_file;

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalogue::{BundleId, Permissions, bundle_id, permissions_of};
use crate::schema::JazzSchema;
use crate::tools::public_schema::{
    ColumnType, Schema, SchemaHash, TableName, TablePolicies, TableSchema,
};

use super::published::{
    ClientMigrations, PublishMigrationRequest, PublishedSchema, check_decoded_client_migrations,
    lower_client_migrations, lower_migration, parse_schema_hash,
};
use super::{DeploymentRequest, structural};
use json::Json;

/// The `format` of both files this version of Jazz reads and writes.
pub const FORMAT: u32 = 1;

/// The app's file: tables, relations, history and migrations.
pub const APP_FILE: &str = "jazz.app.json";
/// The backend's file: the current version's permissions.
pub const PERMISSIONS_FILE: &str = "jazz.permissions.json";

/// The JSON Schemas the files name in `$schema`.
pub const APP_SCHEMA_URL: &str = "https://jazz.tools/schemas/jazz.app.schema.json";
pub const PERMISSIONS_SCHEMA_URL: &str = "https://jazz.tools/schemas/jazz.permissions.schema.json";

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

const REGENERATE: &str = "regenerate it with `jazz-tools validate`";

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
    /// Slices a .63 `schema.ts` exported. Slicing is gone in alpha.65:
    /// accepted from the .63 writer's input and ignored, never written.
    #[serde(default, skip_serializing)]
    pub slices: BTreeMap<String, Vec<String>>,
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
    /// The .63 writer still emits it and loaders accept it with a warning,
    /// so `jazz-tools upgrade` can turn it into `computed` and `transforms`.
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

    pub fn permissions(&self) -> &Permissions {
        &self.permissions
    }

    pub fn codegen(&self) -> &Codegen {
        &self.codegen
    }

    /// What loaded but is ignored, as `jazz.app.json: <path>: <why>` lines.
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
    let permissions_json = read_format(PERMISSIONS_FILE, permissions)?;
    let permissions =
        permissions_file::read(&permissions_json, history.current_schema(), &relations)
            .map_err(|refusal| ArtifactError::at(PERMISSIONS_FILE, refusal))?;
    let bundle = compute_bundle(history.current_schema(), &permissions)?;
    Ok(LoadedApp {
        history,
        bundle,
        permissions,
        codegen: app_file.codegen,
        warnings: app_file
            .warnings
            .into_iter()
            .map(|w| format!("{APP_FILE}: {w}"))
            .collect(),
    })
}

/// `jazz.app.json` checked as [`load`] checks it, without permissions: what
/// code generators and `jazz validate` read.
#[derive(Debug, Clone)]
pub struct AppDefinition {
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

    /// What loaded but is ignored, as `jazz.app.json: <path>: <why>` lines.
    pub fn warnings(&self) -> &[String] {
        &self.warnings
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
    check_codegen(&app_file.codegen, history.current_schema())?;
    // Lowering for a client must succeed too.
    lower_client_migrations(history.bundle())
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    Ok(AppDefinition {
        history,
        codegen: app_file.codegen,
        warnings: app_file
            .warnings
            .into_iter()
            .map(|w| format!("{APP_FILE}: {w}"))
            .collect(),
    })
}

/// The `source` generated bindings record: `sha256:` and the hex SHA-256 of
/// the file's bytes, with CRLF line ends read as LF so a Windows checkout
/// of the same file has the same source.
pub fn source_hash(file: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    let mut rest = file;
    while let Some(at) = rest.windows(2).position(|w| w == b"\r\n") {
        hasher.update(&rest[..at]);
        hasher.update(b"\n");
        rest = &rest[at + 2..];
    }
    hasher.update(rest);
    format!("sha256:{}", hex::encode(hasher.finalize()))
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
pub fn load_client_current(json: &[u8]) -> Result<(Schema, crate::db::Migrations), ArtifactError> {
    let app_file = read_app(json)?;
    let history = check_history(&app_file)?;
    let (schemas, lenses) = lower_client_migrations(history.bundle())
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    Ok((
        history.current_schema().clone(),
        crate::db::Migrations { schemas, lenses },
    ))
}

/// What a binding's open configuration ships (a `jazz.app.json`, see
/// [`load_client`]), lowered for a store; none ships nothing.
pub fn client_migrations(app: Option<&[u8]>) -> Result<crate::db::Migrations, String> {
    match app {
        None => Ok(crate::db::Migrations::default()),
        Some(json) => load_client(json).map_err(|error| error.to_string()),
    }
}

fn read_app(json: &[u8]) -> Result<app_file::AppFile, ArtifactError> {
    let root = read_format(APP_FILE, json)?;
    refuse_todos(&root)?;
    app_file::read(&root).map_err(|refusal| ArtifactError::at(APP_FILE, refusal))
}

/// A string value starting with this is a decision `jazz-tools migrations
/// create` (or `upgrade`) left to the author.
pub const TODO_PREFIX: &str = "TODO:";

/// The JSON paths of every unresolved `"TODO: …"` in `root`, with its
/// text, in file order. `$comment`s may say TODO freely.
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
                        let path = if path.is_empty() {
                            key.clone()
                        } else {
                            format!("{path}.{key}")
                        };
                        walk(item, path, out);
                    }
                }
            }
            _ => {}
        }
    }
    let mut out = Vec::new();
    walk(root, String::new(), &mut out);
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
        "{} unresolved TODO{} left by `jazz-tools migrations create`; replace each with \
         the value it asks for, then run `jazz-tools validate` again:\n{}",
        todos.len(),
        if todos.len() == 1 { "" } else { "s" },
        list.join("\n")
    )))
}

/// The file as JSON, once its `format` is one this version reads. The
/// format comes first, so a file from a newer Jazz is refused for what it is
/// rather than for the fields this version doesn't know.
fn read_format(file: &'static str, json: &[u8]) -> Result<Json, ArtifactError> {
    let refuse = |message: String| ArtifactError::in_file(file, message);
    let root = json::parse(json).map_err(refuse)?;
    let Some(object) = root.as_object() else {
        return Err(refuse(format!(
            "must be a JSON object, not {}",
            root.kind()
        )));
    };
    match object.iter().find(|(k, _)| k == "format").map(|(_, v)| v) {
        None => Err(refuse(format!(
            "no `format`; the file starts {{ \"format\": {FORMAT}, … }}"
        ))),
        Some(Json::Number(n)) if n == &FORMAT.to_string() => Ok(root),
        Some(Json::Number(n)) if n.parse::<u64>().is_ok() => Err(refuse(format!(
            "format {n} is not supported by this version of Jazz, which reads format {FORMAT}"
        ))),
        Some(other) => Err(refuse(format!(
            "`format` must be the integer {FORMAT}, not {}",
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
}

impl History {
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
    let current = SchemaHash::compute(&file.current);
    let mut schemas: Vec<(SchemaHash, Schema)> = Vec::new();
    let mut by_name = BTreeMap::new();
    for (name, schema) in &file.history {
        let hash = SchemaHash::compute(schema);
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
    schemas.push((current, file.current.clone()));
    let migrations = file
        .migrations
        .iter()
        .map(|(from, to, forward)| PublishMigrationRequest {
            from_hash: by_name[from].to_string(),
            to_hash: to.as_ref().map_or(current, |to| by_name[to]).to_string(),
            forward: forward.clone(),
        })
        .collect();
    Ok(History {
        current,
        schemas,
        migrations,
    })
}

/// The bundle id the server computes when this backend publishes: the
/// permissions are compiled against the current schema (refusing those
/// that don't compile), then hashed as the catalogue hashes them.
fn compute_bundle(current: &Schema, permissions: &Permissions) -> Result<BundleId, ArtifactError> {
    let mut target = current.clone();
    for (name, policies) in permissions {
        target
            .get_mut(name)
            .expect("checked against the current schema")
            .policies = policies.clone();
    }
    let refuse = |error: String| ArtifactError::in_file(PERMISSIONS_FILE, error);
    let compiled =
        JazzSchema::new(&target).map_err(|error| refuse(format!("permissions: {error}")))?;
    bundle_id(
        &SchemaHash::compute(&structural(compiled.public_schema().clone())),
        &permissions_of(compiled.public_schema()),
    )
    .map_err(|error| refuse(format!("permissions: {error}")))
}

fn check_codegen(codegen: &Codegen, current: &Schema) -> Result<(), ArtifactError> {
    let table = |path: &str, name: &str| {
        current.get(&TableName::from(name)).ok_or_else(|| {
            ArtifactError::new(format!(
                "{path}: the current schema has no table {name}{}",
                expr::did_you_mean(name, current.keys().map(|t| t.as_str()))
            ))
        })
    };
    let column_of = |schema: &TableSchema, column: &str| {
        schema
            .columns
            .columns
            .iter()
            .find(|c| c.name_str() == column)
            .cloned()
    };
    let has_column = |schema: &TableSchema, column: &str| column_of(schema, column).is_some();
    for (name, entry) in &codegen.tables {
        let schema = table(&format!("tables.{name}"), name)?;
        for (relation, target) in &entry.relations {
            let path = format!("tables.{name}.relations.{relation}");
            match target {
                CodegenRelation::Forward {
                    table: other,
                    column,
                } => {
                    table(&path, other)?;
                    if !has_column(schema, column) {
                        return Err(ArtifactError::new(format!(
                            "{path}: table {name} has no column {column}"
                        )));
                    }
                }
                CodegenRelation::Reverse {
                    table: other,
                    relation: forward,
                } => {
                    table(&path, other)?;
                    let points_back = codegen
                        .tables
                        .get(other)
                        .and_then(|other| other.relations.get(forward))
                        .is_some_and(|forward| {
                            matches!(forward, CodegenRelation::Forward { table, .. } if table == name)
                        });
                    if !points_back {
                        return Err(ArtifactError::new(format!(
                            "{path}: {other} has no forward relation {forward} to {name}"
                        )));
                    }
                }
            }
        }
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
    Ok(())
}

/// A schema in the readable form of a `history` version's `tables` (no
/// relations, computed fields or transforms), as the writer prints it.
pub fn version_tables(schema: &Schema) -> Result<Json, ArtifactError> {
    app_file::write_version_tables(schema)
        .map_err(|error| ArtifactError::unsupported(APP_FILE, error))
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
    let history: Vec<(String, &Schema)> = schemas
        .iter()
        .filter(|(hash, _)| *hash != current_hash)
        .map(|(hash, schema)| (app_file::version_name(hash), schema))
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
    let app_json = app_file::write(&current, &history, &migrations, &input.codegen)
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
            migration.forward.sort_by(|a, b| a.table.cmp(&b.table));
        }
        serde_json::to_value(migrations).ok()
    };
    if normal(loaded.migrations()) != normal(&input.migrations) {
        return Err(round_trip("a migration"));
    }
    Ok(written)
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
