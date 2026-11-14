//! `jazz.app.json`: an app's schema versions, migrations, permissions and
//! codegen metadata in one language-neutral file.
//!
//! `jazz-tools validate` compiles `schema.ts`, `migrations/` and
//! `permissions.ts` and hands the result to [`write`], which is the only
//! writer of the file. Every runtime reads it with [`load`], the only reader.
//! A backend publishes [`LoadedApp::deployment_request`] at startup and pins
//! its sessions to [`LoadedApp::bundle`]; clients open along
//! [`LoadedApp::client_migrations`] and never look at the permissions.
//!
//! The file's JSON is its own: every type below mirrors an engine type
//! field for field, so the engine's wire and catalogue encodings stay as
//! they are while the file follows one convention throughout. Fields are
//! snake_case, sum types carry an internal `"type"` tag with PascalCase
//! names, and [`Value`] keeps its adjacent `{"type", "value"}` form. Unknown
//! fields are refused everywhere.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::catalogue::{BundleId, Permissions, bundle_id, permissions_of};
use crate::query::{
    OrderDirection, RecursionBound, RelationCmpOp, RelationColumnRef, RelationExpr,
    RelationJoinCondition, RelationJoinKind, RelationKeyRef, RelationOrderBy, RelationPredicate,
    RelationProjectColumn, RelationProjectExpr, RelationRowIdRef, RelationUnionArm,
    RelationValueRef,
};
use crate::schema::JazzSchema;
use crate::tools::public_schema::{
    CmpOp, ColumnDescriptor, ColumnMergeStrategy, ColumnType, HistoryRetention, Operation,
    PolicyExpr, PolicyValue, Schema, SchemaHash, TableName, TablePolicies, TableSchema, Value,
};
use crate::tools::public_schema::{EnumCaseDescriptor, OperationPolicy, RowDescriptor};

use super::published::{
    ClientMigrations, PublishLensOp, PublishMigrationRequest, PublishTableLens, PublishedSchema,
    check_decoded_client_migrations, lower_client_migrations, lower_migration, parse_schema_hash,
};
use super::{DeploymentRequest, structural};

/// The `format` this version of Jazz reads and writes.
pub const FORMAT: u32 = 1;

/// Why a `jazz.app.json` was refused. Every message names the field.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("jazz.app.json: {0}")]
pub struct ArtifactError(String);

impl ArtifactError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    /// The refusal, without the file prefix.
    pub fn message(&self) -> &str {
        &self.0
    }
}

const REGENERATE: &str = "regenerate it with `jazz-tools validate`";

// ============================================================================
// The file
// ============================================================================

/// `jazz.app.json` as it is on disk.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AppArtifact {
    /// Major version of this format; readers refuse others.
    pub format: u32,
    /// Hash of the schema version the app's code is written against.
    pub current: String,
    /// Id of the permissions bundle `current` and `permissions` make.
    pub bundle: String,
    /// Every schema version the app still ships, oldest first.
    pub schemas: Vec<ArtifactSchema>,
    #[serde(default)]
    pub migrations: Vec<ArtifactMigration>,
    /// Permissions for `current`, by table.
    #[serde(default)]
    pub permissions: BTreeMap<String, ArtifactTablePolicies>,
    /// Authoring metadata for typed code in each language. It never
    /// affects a schema hash or a bundle id.
    #[serde(default)]
    pub codegen: Codegen,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactSchema {
    pub hash: String,
    pub tables: BTreeMap<String, ArtifactTable>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTable {
    pub columns: Vec<ArtifactColumn>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed_columns: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub composite_indexes: Vec<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub branch_by: Vec<String>,
    #[serde(default, skip_serializing_if = "ArtifactHistory::is_full")]
    pub history: ArtifactHistory,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactHistory {
    #[default]
    Full,
    Latest,
}

impl ArtifactHistory {
    fn is_full(&self) -> bool {
        *self == Self::Full
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactColumn {
    pub name: String,
    pub column_type: ArtifactColumnType,
    pub nullable: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_strategy: Option<ColumnMergeStrategy>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactColumnType {
    Integer,
    BigInt,
    Double,
    Boolean,
    Text,
    Timestamp,
    Uuid,
    TransactionId,
    Bytea,
    Enum {
        variants: Vec<String>,
    },
    EnumPayload {
        cases: Vec<ArtifactEnumCase>,
    },
    Json {
        /// JSON Schema (draft-07) the values must satisfy.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        schema: Option<serde_json::Value>,
    },
    Array {
        element: Box<ArtifactColumnType>,
    },
    Row {
        columns: Vec<ArtifactColumn>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactEnumCase {
    pub name: String,
    pub fields: Vec<ArtifactColumn>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactMigration {
    pub from: String,
    pub to: String,
    pub forward: Vec<ArtifactTableLens>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTableLens {
    pub table: String,
    #[serde(default, skip_serializing_if = "is_false")]
    pub added: bool,
    #[serde(default, skip_serializing_if = "is_false")]
    pub removed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub renamed_from: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub operations: Vec<ArtifactLensOp>,
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactLensOp {
    /// A column the target has and the source lacks; source rows read
    /// `default` for it.
    Introduce {
        column: String,
        column_type: ArtifactColumnType,
        default: Value,
    },
    /// A column the source has and the target lacks; going back, target
    /// rows read `default` for it.
    Drop {
        column: String,
        column_type: ArtifactColumnType,
        default: Value,
    },
    Rename {
        from: String,
        to: String,
    },
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactTablePolicies {
    #[serde(default, skip_serializing_if = "ArtifactOperationPolicy::is_empty")]
    pub select: ArtifactOperationPolicy,
    #[serde(default, skip_serializing_if = "ArtifactOperationPolicy::is_empty")]
    pub insert: ArtifactOperationPolicy,
    #[serde(default, skip_serializing_if = "ArtifactOperationPolicy::is_empty")]
    pub update: ArtifactOperationPolicy,
    #[serde(default, skip_serializing_if = "ArtifactOperationPolicy::is_empty")]
    pub delete: ArtifactOperationPolicy,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactOperationPolicy {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub using: Option<ArtifactPolicyExpr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub with_check: Option<ArtifactPolicyExpr>,
}

impl ArtifactOperationPolicy {
    fn is_empty(&self) -> bool {
        self.using.is_none() && self.with_check.is_none()
    }
}

/// [`PolicyExpr`], with relation expressions in this file's form.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactPolicyExpr {
    Cmp {
        column: String,
        op: CmpOp,
        value: ArtifactPolicyValue,
    },
    SessionCmp {
        path: Vec<String>,
        op: CmpOp,
        value: Value,
    },
    IsNull {
        column: String,
    },
    SessionIsNull {
        path: Vec<String>,
    },
    IsNotNull {
        column: String,
    },
    SessionIsNotNull {
        path: Vec<String>,
    },
    Contains {
        column: String,
        value: ArtifactPolicyValue,
    },
    SessionContains {
        path: Vec<String>,
        value: Value,
    },
    In {
        column: String,
        session_path: Vec<String>,
    },
    InList {
        column: String,
        values: Vec<ArtifactPolicyValue>,
    },
    SessionInList {
        path: Vec<String>,
        values: Vec<Value>,
    },
    Exists {
        table: String,
        condition: Box<ArtifactPolicyExpr>,
    },
    ExistsRel {
        rel: ArtifactRelationExpr,
    },
    Inherits {
        operation: Operation,
        via_column: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
    },
    InheritsReferencing {
        operation: Operation,
        source_table: String,
        via_column: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
    },
    And {
        exprs: Vec<ArtifactPolicyExpr>,
    },
    Or {
        exprs: Vec<ArtifactPolicyExpr>,
    },
    Not {
        expr: Box<ArtifactPolicyExpr>,
    },
    True,
    False,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactPolicyValue {
    Literal { value: Value },
    SessionRef { path: Vec<String> },
}

/// [`RelationExpr`], internally tagged. (The engine's own form is
/// externally tagged because it is also postcard-encoded on the query wire,
/// which internal tags don't support.)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactRelationExpr {
    TableScan {
        table: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alias: Option<String>,
    },
    Filter {
        input: Box<ArtifactRelationExpr>,
        predicate: ArtifactPredicate,
    },
    Union {
        inputs: Vec<ArtifactUnionArm>,
    },
    Join {
        left: Box<ArtifactRelationExpr>,
        right: Box<ArtifactRelationExpr>,
        on: Vec<ArtifactJoinCondition>,
        join_kind: ArtifactJoinKind,
    },
    Project {
        input: Box<ArtifactRelationExpr>,
        columns: Vec<ArtifactProjectColumn>,
    },
    Gather {
        seed: Box<ArtifactRelationExpr>,
        step: Box<ArtifactRelationExpr>,
        frontier_key: ArtifactKeyRef,
        /// Maximum recursion depth; absent means until a fixpoint.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        max_depth: Option<usize>,
        dedupe_key: Vec<ArtifactKeyRef>,
    },
    Distinct {
        input: Box<ArtifactRelationExpr>,
        key: Vec<ArtifactKeyRef>,
    },
    OrderBy {
        input: Box<ArtifactRelationExpr>,
        terms: Vec<ArtifactOrderBy>,
    },
    Offset {
        input: Box<ArtifactRelationExpr>,
        offset: usize,
    },
    Limit {
        input: Box<ArtifactRelationExpr>,
        limit: usize,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactUnionArm {
    pub label: String,
    pub input: ArtifactRelationExpr,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactJoinCondition {
    pub left: ArtifactColumnRef,
    pub right: ArtifactColumnRef,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactJoinKind {
    Inner,
    Left,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactProjectColumn {
    pub alias: String,
    pub expr: ArtifactKeyRef,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactOrderBy {
    pub column: ArtifactColumnRef,
    pub direction: ArtifactDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactColumnRef {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub scope: Option<String>,
    pub column: String,
}

/// A column or a row id, as join keys, dedupe keys and projections name
/// them.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactKeyRef {
    Column { column: ArtifactColumnRef },
    RowId { row: ArtifactRowId },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ArtifactRowId {
    Current,
    Outer,
    Frontier,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactPredicate {
    Cmp {
        left: ArtifactColumnRef,
        op: RelationCmpOp,
        right: ArtifactValueRef,
    },
    IsNull {
        column: ArtifactColumnRef,
    },
    IsNotNull {
        column: ArtifactColumnRef,
    },
    In {
        left: ArtifactColumnRef,
        values: Vec<ArtifactValueRef>,
    },
    Contains {
        left: ArtifactColumnRef,
        right: ArtifactValueRef,
    },
    EnumMatch {
        column: ArtifactColumnRef,
        case: String,
        payload: Box<ArtifactPredicate>,
    },
    And {
        exprs: Vec<ArtifactPredicate>,
    },
    Or {
        exprs: Vec<ArtifactPredicate>,
    },
    Not {
        expr: Box<ArtifactPredicate>,
    },
    True,
    False,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ArtifactValueRef {
    /// A literal in the relation IR's JSON form, kept as the engine reads it.
    Literal {
        value: serde_json::Value,
    },
    Param {
        name: String,
    },
    SessionRef {
        path: Vec<String>,
    },
    OuterColumn {
        column: ArtifactColumnRef,
    },
    FrontierColumn {
        column: ArtifactColumnRef,
    },
    RowId {
        row: ArtifactRowId,
    },
}

/// What typed code in each language needs beyond the engine's schema.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Codegen {
    /// For tables of `current`.
    #[serde(default)]
    pub tables: BTreeMap<String, CodegenTable>,
    /// Slices `schema.ts` exports, by export name.
    #[serde(default)]
    pub slices: BTreeMap<String, Vec<String>>,
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CodegenTable {
    #[serde(default)]
    pub relations: BTreeMap<String, CodegenRelation>,
    /// Columns whose values only TypeScript computes. Other languages see
    /// the stored column of a `Transform` and nothing of a `Computed`, and
    /// warn about both.
    #[serde(default)]
    pub ts_only: Vec<TsOnlyColumn>,
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

// ============================================================================
// Reading
// ============================================================================

/// A `jazz.app.json` that passed every check.
#[derive(Debug, Clone)]
pub struct LoadedApp {
    current: SchemaHash,
    bundle: BundleId,
    /// Oldest first, policy-free.
    schemas: Vec<(SchemaHash, Schema)>,
    migrations: Vec<PublishMigrationRequest>,
    permissions: Permissions,
    codegen: Codegen,
}

impl LoadedApp {
    pub fn current_hash(&self) -> SchemaHash {
        self.current
    }

    /// The current schema, without permissions.
    pub fn current_schema(&self) -> &Schema {
        &self
            .schemas
            .iter()
            .find(|(hash, _)| *hash == self.current)
            .expect("checked on load")
            .1
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

    /// The schema versions and lenses a client opens along.
    pub fn client_migrations(&self) -> crate::db::Migrations {
        let (schemas, lenses) =
            lower_client_migrations(self.client_bundle()).expect("checked on load");
        crate::db::Migrations { schemas, lenses }
    }

    /// What a backend publishes at startup.
    pub fn deployment_request(&self, allow_dropping_history: bool) -> DeploymentRequest {
        let schemas = self.schemas.clone();
        let migrations = self
            .migrations
            .iter()
            .map(|migration| {
                let schema = |hash: &str| {
                    let hash = parse_schema_hash(hash).expect("checked on load");
                    &self.schemas.iter().find(|(h, _)| *h == hash).unwrap().1
                };
                lower_migration(
                    migration.clone(),
                    schema(&migration.from_hash),
                    schema(&migration.to_hash),
                )
                .expect("checked on load")
            })
            .collect();
        DeploymentRequest {
            target_schema_hash: self.current,
            schemas,
            migrations,
            permissions: self
                .permissions
                .iter()
                .map(|(t, p)| (*t, p.clone()))
                .collect(),
            allow_dropping_history,
        }
    }

    fn client_bundle(&self) -> ClientMigrations {
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
}

/// Read and check a `jazz.app.json`.
pub fn load(json: &[u8]) -> Result<LoadedApp, ArtifactError> {
    // The format first, so a file from a newer Jazz is refused for what it
    // is rather than for the fields this version doesn't know.
    let raw: serde_json::Value = serde_json::from_slice(json)
        .map_err(|error| ArtifactError::new(format!("not JSON: {error}")))?;
    match raw.get("format").and_then(serde_json::Value::as_u64) {
        Some(format) if format == u64::from(FORMAT) => {}
        Some(format) => {
            return Err(ArtifactError::new(format!(
                "format {format} is not supported by this version of Jazz, which reads format {FORMAT}"
            )));
        }
        None => return Err(ArtifactError::new("no `format`")),
    }
    let artifact: AppArtifact = serde_json::from_value(raw)
        .map_err(|error| ArtifactError::new(format!("{error}; {REGENERATE}")))?;
    check(artifact, BundleCheck::Verify)
}

/// Whether [`check`] compares the file's `bundle` with the computed id, or
/// leaves it to the writer to fill in.
#[derive(PartialEq)]
enum BundleCheck {
    Verify,
    Compute,
}

fn check(artifact: AppArtifact, bundle_check: BundleCheck) -> Result<LoadedApp, ArtifactError> {
    let current = hash_field("current", &artifact.current)?;

    let mut schemas = Vec::new();
    for (index, entry) in artifact.schemas.iter().enumerate() {
        let claimed = hash_field(&format!("schemas[{index}].hash"), &entry.hash)?;
        let schema = schema_from_artifact(&entry.tables);
        let actual = SchemaHash::compute(&schema);
        if actual != claimed {
            return Err(ArtifactError::new(format!(
                "schemas[{index}].hash is {claimed} but the schema hashes to {actual}; {REGENERATE}"
            )));
        }
        if schemas.iter().any(|(hash, _)| *hash == actual) {
            return Err(ArtifactError::new(format!(
                "schemas[{index}] repeats schema {actual}"
            )));
        }
        schemas.push((actual, schema));
    }
    let Some(current_schema) = schemas
        .iter()
        .find(|(hash, _)| *hash == current)
        .map(|(_, schema)| schema.clone())
    else {
        return Err(ArtifactError::new(format!(
            "current schema {current} is not among `schemas`"
        )));
    };

    let mut migrations = Vec::new();
    for (index, migration) in artifact.migrations.iter().enumerate() {
        for (field, hash) in [("from", &migration.from), ("to", &migration.to)] {
            let path = format!("migrations[{index}].{field}");
            let hash = hash_field(&path, hash)?;
            if !schemas.iter().any(|(known, _)| *known == hash) {
                return Err(ArtifactError::new(format!(
                    "{path} names schema {hash}, which is not among `schemas`"
                )));
            }
        }
        migrations.push(migration_from_artifact(migration));
    }

    let mut permissions = Permissions::new();
    for (table, policies) in &artifact.permissions {
        let name = TableName::from(table.as_str());
        if !current_schema.contains_key(&name) {
            return Err(ArtifactError::new(format!(
                "permissions.{table}: the current schema has no table {table}"
            )));
        }
        permissions.insert(name, table_policies_from_artifact(policies));
    }

    check_codegen(&artifact.codegen, &current_schema)?;

    let loaded = LoadedApp {
        current,
        bundle: BundleId([0; 32]),
        schemas,
        migrations,
        permissions,
        codegen: artifact.codegen,
    };

    // Lenses, as a deployment and `jazz-tools validate` check them. Where a
    // migration sits in the whole history is the store's to judge: its
    // catalogue may hold versions this file no longer ships.
    check_decoded_client_migrations(loaded.client_bundle(), current)
        .map_err(|error| ArtifactError::new(format!("migrations: {error}")))?;

    let bundle = compute_bundle(&current_schema, &loaded.permissions)?;
    if bundle_check == BundleCheck::Compute {
        return Ok(LoadedApp { bundle, ..loaded });
    }
    let claimed = BundleId::from_hex(&artifact.bundle)
        .ok_or_else(|| ArtifactError::new("`bundle` is not a 64-character hex id"))?;
    if claimed != bundle {
        return Err(ArtifactError::new(format!(
            "`bundle` is {claimed} but the current schema and permissions make {bundle}; {REGENERATE}"
        )));
    }
    Ok(LoadedApp { bundle, ..loaded })
}

fn hash_field(path: &str, text: &str) -> Result<SchemaHash, ArtifactError> {
    parse_schema_hash(text).map_err(|error| ArtifactError::new(format!("{path}: {error}")))
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
    let compiled = JazzSchema::new(&target)
        .map_err(|error| ArtifactError::new(format!("permissions: {error}")))?;
    bundle_id(
        &SchemaHash::compute(&structural(compiled.public_schema().clone())),
        &permissions_of(compiled.public_schema()),
    )
    .map_err(|error| ArtifactError::new(format!("permissions: {error}")))
}

fn check_codegen(codegen: &Codegen, current: &Schema) -> Result<(), ArtifactError> {
    let table = |path: &str, name: &str| {
        current.get(&TableName::from(name)).ok_or_else(|| {
            ArtifactError::new(format!("{path}: the current schema has no table {name}"))
        })
    };
    let has_column = |schema: &TableSchema, column: &str| {
        schema
            .columns
            .columns
            .iter()
            .any(|c| c.name_str() == column)
    };
    for (name, entry) in &codegen.tables {
        let schema = table(&format!("codegen.tables.{name}"), name)?;
        for (relation, target) in &entry.relations {
            let path = format!("codegen.tables.{name}.relations.{relation}");
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
        for (index, column) in entry.ts_only.iter().enumerate() {
            if let TsOnlyColumn::Transform { column } = column
                && !has_column(schema, column)
            {
                return Err(ArtifactError::new(format!(
                    "codegen.tables.{name}.ts_only[{index}]: table {name} has no column {column}"
                )));
            }
        }
    }
    for (slice, tables) in &codegen.slices {
        if tables.is_empty() {
            return Err(ArtifactError::new(format!(
                "codegen.slices.{slice} has no tables"
            )));
        }
        for name in tables {
            table(&format!("codegen.slices.{slice}"), name)?;
        }
    }
    Ok(())
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

/// Build, check and render a `jazz.app.json`: the file's only writer.
/// The text is canonical, so a committed file can be compared byte for byte
/// with a fresh build.
pub fn write(input: ArtifactInput) -> Result<String, ArtifactError> {
    let schemas = input
        .schemas
        .into_iter()
        .enumerate()
        .map(|(index, schema)| {
            let schema = structural(schema);
            Ok(ArtifactSchema {
                hash: SchemaHash::compute(&schema).to_string(),
                tables: schema_to_artifact(&schema)
                    .map_err(|error| ArtifactError::new(format!("schemas[{index}]: {error}")))?,
            })
        })
        .collect::<Result<Vec<_>, ArtifactError>>()?;
    let permissions = input
        .permissions
        .iter()
        .filter(|(_, policies)| **policies != TablePolicies::default())
        .map(|(table, policies)| {
            Ok((
                table.as_str().to_string(),
                table_policies_to_artifact(policies)
                    .map_err(|error| ArtifactError::new(format!("permissions.{table}: {error}")))?,
            ))
        })
        .collect::<Result<BTreeMap<_, _>, ArtifactError>>()?;
    let mut artifact = AppArtifact {
        format: FORMAT,
        current: input.current,
        bundle: String::new(),
        schemas,
        migrations: input
            .migrations
            .iter()
            .map(migration_to_artifact)
            .collect::<Result<_, _>>()?,
        permissions,
        codegen: input.codegen,
    };
    // Check the file as a reader will, computing its bundle id on the way.
    artifact.bundle = check(artifact.clone(), BundleCheck::Compute)?
        .bundle
        .to_hex();
    Ok(render(&artifact))
}

/// The canonical text of `artifact`: object keys sorted, arrays in their
/// order, two-space indent, trailing newline.
pub fn render(artifact: &AppArtifact) -> String {
    fn sorted(value: serde_json::Value) -> serde_json::Value {
        match value {
            serde_json::Value::Object(map) => {
                let entries: BTreeMap<String, serde_json::Value> =
                    map.into_iter().map(|(k, v)| (k, sorted(v))).collect();
                serde_json::Value::Object(entries.into_iter().collect())
            }
            serde_json::Value::Array(items) => {
                serde_json::Value::Array(items.into_iter().map(sorted).collect())
            }
            other => other,
        }
    }
    let value = sorted(serde_json::to_value(artifact).expect("artifact serializes"));
    let mut text = serde_json::to_string_pretty(&value).expect("JSON value serializes");
    text.push('\n');
    text
}

// ============================================================================
// Engine forms <-> file forms
// ============================================================================

fn schema_to_artifact(schema: &Schema) -> Result<BTreeMap<String, ArtifactTable>, String> {
    schema
        .iter()
        .map(|(name, table)| {
            let columns = table
                .columns
                .columns
                .iter()
                .map(column_to_artifact)
                .collect::<Result<_, _>>()
                .map_err(|error| format!("table {name}: {error}"))?;
            Ok((
                name.as_str().to_string(),
                ArtifactTable {
                    columns,
                    indexed_columns: table
                        .indexed_columns
                        .as_ref()
                        .map(|columns| columns.iter().map(|c| c.as_str().to_string()).collect()),
                    composite_indexes: table
                        .composite_indexes
                        .iter()
                        .map(|index| index.iter().map(|c| c.as_str().to_string()).collect())
                        .collect(),
                    branch_by: table
                        .branch_by
                        .iter()
                        .map(|c| c.as_str().to_string())
                        .collect(),
                    history: match table.history {
                        HistoryRetention::Full => ArtifactHistory::Full,
                        HistoryRetention::Latest => ArtifactHistory::Latest,
                    },
                },
            ))
        })
        .collect()
}

fn schema_from_artifact(tables: &BTreeMap<String, ArtifactTable>) -> Schema {
    let mut schema = Schema::new();
    for (name, table) in tables {
        let columns = table.columns.iter().map(column_from_artifact).collect();
        let table_schema = TableSchema {
            columns: RowDescriptor { columns },
            indexed_columns: table
                .indexed_columns
                .as_ref()
                .map(|columns| columns.iter().map(|c| c.as_str().into()).collect()),
            composite_indexes: table
                .composite_indexes
                .iter()
                .map(|index| index.iter().map(|c| c.as_str().into()).collect())
                .collect(),
            policies: TablePolicies::default(),
            branch_by: table.branch_by.iter().map(|c| c.as_str().into()).collect(),
            history: match table.history {
                ArtifactHistory::Full => HistoryRetention::Full,
                ArtifactHistory::Latest => HistoryRetention::Latest,
            },
        };
        schema.insert(TableName::from(name.as_str()), table_schema);
    }
    schema
}

fn column_to_artifact(column: &ColumnDescriptor) -> Result<ArtifactColumn, String> {
    Ok(ArtifactColumn {
        name: column.name_str().to_string(),
        column_type: column_type_to_artifact(&column.column_type)
            .map_err(|error| format!("column {}: {error}", column.name_str()))?,
        nullable: column.nullable,
        references: column.references.as_ref().map(|t| t.as_str().to_string()),
        default: column.default.clone(),
        merge_strategy: column.merge_strategy,
    })
}

fn column_from_artifact(column: &ArtifactColumn) -> ColumnDescriptor {
    let mut descriptor = ColumnDescriptor::new(
        column.name.as_str(),
        column_type_from_artifact(&column.column_type),
    );
    descriptor.nullable = column.nullable;
    descriptor.references = column.references.as_deref().map(TableName::from);
    descriptor.default = column.default.clone();
    descriptor.merge_strategy = column.merge_strategy;
    descriptor
}

fn column_type_to_artifact(column_type: &ColumnType) -> Result<ArtifactColumnType, String> {
    Ok(match column_type {
        ColumnType::Integer => ArtifactColumnType::Integer,
        ColumnType::BigInt => ArtifactColumnType::BigInt,
        ColumnType::Double => ArtifactColumnType::Double,
        ColumnType::Boolean => ArtifactColumnType::Boolean,
        ColumnType::Text => ArtifactColumnType::Text,
        ColumnType::Timestamp => ArtifactColumnType::Timestamp,
        ColumnType::Uuid => ArtifactColumnType::Uuid,
        ColumnType::TransactionId => ArtifactColumnType::TransactionId,
        ColumnType::Bytea => ArtifactColumnType::Bytea,
        ColumnType::Enum { variants } => ArtifactColumnType::Enum {
            variants: variants.clone(),
        },
        ColumnType::EnumPayload { cases } => ArtifactColumnType::EnumPayload {
            cases: cases
                .iter()
                .map(|case| {
                    Ok(ArtifactEnumCase {
                        name: case.name.clone(),
                        fields: case
                            .fields
                            .iter()
                            .map(column_to_artifact)
                            .collect::<Result<_, String>>()?,
                    })
                })
                .collect::<Result<_, String>>()?,
        },
        ColumnType::Json { schema } => ArtifactColumnType::Json {
            schema: schema.clone(),
        },
        ColumnType::Array { element } => ArtifactColumnType::Array {
            element: Box::new(column_type_to_artifact(element)?),
        },
        ColumnType::Row { columns } => ArtifactColumnType::Row {
            columns: columns
                .columns
                .iter()
                .map(column_to_artifact)
                .collect::<Result<_, _>>()?,
        },
        ColumnType::ScalarEnum { .. } | ColumnType::CatalogueEnumPayload { .. } => {
            return Err(
                "catalogue-native enum types are not supported in jazz.app.json yet".into(),
            );
        }
    })
}

fn column_type_from_artifact(column_type: &ArtifactColumnType) -> ColumnType {
    match column_type {
        ArtifactColumnType::Integer => ColumnType::Integer,
        ArtifactColumnType::BigInt => ColumnType::BigInt,
        ArtifactColumnType::Double => ColumnType::Double,
        ArtifactColumnType::Boolean => ColumnType::Boolean,
        ArtifactColumnType::Text => ColumnType::Text,
        ArtifactColumnType::Timestamp => ColumnType::Timestamp,
        ArtifactColumnType::Uuid => ColumnType::Uuid,
        ArtifactColumnType::TransactionId => ColumnType::TransactionId,
        ArtifactColumnType::Bytea => ColumnType::Bytea,
        ArtifactColumnType::Enum { variants } => ColumnType::Enum {
            variants: variants.clone(),
        },
        ArtifactColumnType::EnumPayload { cases } => ColumnType::EnumPayload {
            cases: cases
                .iter()
                .map(|case| EnumCaseDescriptor {
                    name: case.name.clone(),
                    fields: case.fields.iter().map(column_from_artifact).collect(),
                })
                .collect(),
        },
        ArtifactColumnType::Json { schema } => ColumnType::Json {
            schema: schema.clone(),
        },
        ArtifactColumnType::Array { element } => ColumnType::Array {
            element: Box::new(column_type_from_artifact(element)),
        },
        ArtifactColumnType::Row { columns } => ColumnType::Row {
            columns: Box::new(RowDescriptor {
                columns: columns.iter().map(column_from_artifact).collect(),
            }),
        },
    }
}

fn migration_to_artifact(
    migration: &PublishMigrationRequest,
) -> Result<ArtifactMigration, ArtifactError> {
    let path = format!("migration {} -> {}", migration.from_hash, migration.to_hash);
    Ok(ArtifactMigration {
        from: migration.from_hash.clone(),
        to: migration.to_hash.clone(),
        forward: migration
            .forward
            .iter()
            .map(|lens| {
                Ok(ArtifactTableLens {
                    table: lens.table.clone(),
                    added: lens.added,
                    removed: lens.removed,
                    renamed_from: lens.renamed_from.clone(),
                    operations: lens
                        .operations
                        .iter()
                        .map(|op| {
                            Ok(match op {
                                PublishLensOp::Introduce {
                                    column,
                                    column_type,
                                    value,
                                } => ArtifactLensOp::Introduce {
                                    column: column.clone(),
                                    column_type: column_type_to_artifact(column_type)?,
                                    default: value.clone(),
                                },
                                PublishLensOp::Drop {
                                    column,
                                    column_type,
                                    value,
                                } => ArtifactLensOp::Drop {
                                    column: column.clone(),
                                    column_type: column_type_to_artifact(column_type)?,
                                    default: value.clone(),
                                },
                                PublishLensOp::Rename { column, value } => ArtifactLensOp::Rename {
                                    from: column.clone(),
                                    to: value.clone(),
                                },
                            })
                        })
                        .collect::<Result<_, String>>()
                        .map_err(|error| ArtifactError::new(format!("{path}: {error}")))?,
                })
            })
            .collect::<Result<_, ArtifactError>>()?,
    })
}

fn migration_from_artifact(migration: &ArtifactMigration) -> PublishMigrationRequest {
    PublishMigrationRequest {
        from_hash: migration.from.clone(),
        to_hash: migration.to.clone(),
        forward: migration
            .forward
            .iter()
            .map(|lens| PublishTableLens {
                table: lens.table.clone(),
                added: lens.added,
                removed: lens.removed,
                renamed_from: lens.renamed_from.clone(),
                operations: lens
                    .operations
                    .iter()
                    .map(|op| match op {
                        ArtifactLensOp::Introduce {
                            column,
                            column_type,
                            default,
                        } => PublishLensOp::Introduce {
                            column: column.clone(),
                            column_type: column_type_from_artifact(column_type),
                            value: default.clone(),
                        },
                        ArtifactLensOp::Drop {
                            column,
                            column_type,
                            default,
                        } => PublishLensOp::Drop {
                            column: column.clone(),
                            column_type: column_type_from_artifact(column_type),
                            value: default.clone(),
                        },
                        ArtifactLensOp::Rename { from, to } => PublishLensOp::Rename {
                            column: from.clone(),
                            value: to.clone(),
                        },
                    })
                    .collect(),
            })
            .collect(),
    }
}

fn table_policies_to_artifact(policies: &TablePolicies) -> Result<ArtifactTablePolicies, String> {
    let operation = |policy: &OperationPolicy| -> Result<ArtifactOperationPolicy, String> {
        Ok(ArtifactOperationPolicy {
            using: policy.using.as_ref().map(policy_to_artifact).transpose()?,
            with_check: policy
                .with_check
                .as_ref()
                .map(policy_to_artifact)
                .transpose()?,
        })
    };
    Ok(ArtifactTablePolicies {
        select: operation(&policies.select)?,
        insert: operation(&policies.insert)?,
        update: operation(&policies.update)?,
        delete: operation(&policies.delete)?,
    })
}

fn table_policies_from_artifact(policies: &ArtifactTablePolicies) -> TablePolicies {
    let operation = |policy: &ArtifactOperationPolicy| OperationPolicy {
        using: policy.using.as_ref().map(policy_from_artifact),
        with_check: policy.with_check.as_ref().map(policy_from_artifact),
    };
    TablePolicies {
        select: operation(&policies.select),
        insert: operation(&policies.insert),
        update: operation(&policies.update),
        delete: operation(&policies.delete),
    }
}

fn policy_value_to_artifact(value: &PolicyValue) -> ArtifactPolicyValue {
    match value {
        PolicyValue::Literal(value) => ArtifactPolicyValue::Literal {
            value: value.clone(),
        },
        PolicyValue::SessionRef(path) => ArtifactPolicyValue::SessionRef { path: path.clone() },
    }
}

fn policy_value_from_artifact(value: &ArtifactPolicyValue) -> PolicyValue {
    match value {
        ArtifactPolicyValue::Literal { value } => PolicyValue::Literal(value.clone()),
        ArtifactPolicyValue::SessionRef { path } => PolicyValue::SessionRef(path.clone()),
    }
}

fn policy_to_artifact(expr: &PolicyExpr) -> Result<ArtifactPolicyExpr, String> {
    let all = |exprs: &[PolicyExpr]| -> Result<Vec<_>, String> {
        exprs.iter().map(policy_to_artifact).collect()
    };
    Ok(match expr {
        PolicyExpr::Cmp { column, op, value } => ArtifactPolicyExpr::Cmp {
            column: column.clone(),
            op: op.clone(),
            value: policy_value_to_artifact(value),
        },
        PolicyExpr::SessionCmp { path, op, value } => ArtifactPolicyExpr::SessionCmp {
            path: path.clone(),
            op: op.clone(),
            value: value.clone(),
        },
        PolicyExpr::IsNull { column } => ArtifactPolicyExpr::IsNull {
            column: column.clone(),
        },
        PolicyExpr::SessionIsNull { path } => {
            ArtifactPolicyExpr::SessionIsNull { path: path.clone() }
        }
        PolicyExpr::IsNotNull { column } => ArtifactPolicyExpr::IsNotNull {
            column: column.clone(),
        },
        PolicyExpr::SessionIsNotNull { path } => {
            ArtifactPolicyExpr::SessionIsNotNull { path: path.clone() }
        }
        PolicyExpr::Contains { column, value } => ArtifactPolicyExpr::Contains {
            column: column.clone(),
            value: policy_value_to_artifact(value),
        },
        PolicyExpr::SessionContains { path, value } => ArtifactPolicyExpr::SessionContains {
            path: path.clone(),
            value: value.clone(),
        },
        PolicyExpr::In {
            column,
            session_path,
        } => ArtifactPolicyExpr::In {
            column: column.clone(),
            session_path: session_path.clone(),
        },
        PolicyExpr::InList { column, values } => ArtifactPolicyExpr::InList {
            column: column.clone(),
            values: values.iter().map(policy_value_to_artifact).collect(),
        },
        PolicyExpr::SessionInList { path, values } => ArtifactPolicyExpr::SessionInList {
            path: path.clone(),
            values: values.clone(),
        },
        PolicyExpr::Exists { table, condition } => ArtifactPolicyExpr::Exists {
            table: table.clone(),
            condition: Box::new(policy_to_artifact(condition)?),
        },
        PolicyExpr::ExistsRel { rel } => ArtifactPolicyExpr::ExistsRel {
            rel: relation_to_artifact(rel),
        },
        PolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => ArtifactPolicyExpr::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        PolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => ArtifactPolicyExpr::InheritsReferencing {
            operation: *operation,
            source_table: source_table.clone(),
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        PolicyExpr::And(exprs) => ArtifactPolicyExpr::And { exprs: all(exprs)? },
        PolicyExpr::Or(exprs) => ArtifactPolicyExpr::Or { exprs: all(exprs)? },
        PolicyExpr::Not(expr) => ArtifactPolicyExpr::Not {
            expr: Box::new(policy_to_artifact(expr)?),
        },
        PolicyExpr::True => ArtifactPolicyExpr::True,
        PolicyExpr::False => ArtifactPolicyExpr::False,
    })
}

fn policy_from_artifact(expr: &ArtifactPolicyExpr) -> PolicyExpr {
    let all = |exprs: &[ArtifactPolicyExpr]| exprs.iter().map(policy_from_artifact).collect();
    match expr {
        ArtifactPolicyExpr::Cmp { column, op, value } => PolicyExpr::Cmp {
            column: column.clone(),
            op: op.clone(),
            value: policy_value_from_artifact(value),
        },
        ArtifactPolicyExpr::SessionCmp { path, op, value } => PolicyExpr::SessionCmp {
            path: path.clone(),
            op: op.clone(),
            value: value.clone(),
        },
        ArtifactPolicyExpr::IsNull { column } => PolicyExpr::IsNull {
            column: column.clone(),
        },
        ArtifactPolicyExpr::SessionIsNull { path } => {
            PolicyExpr::SessionIsNull { path: path.clone() }
        }
        ArtifactPolicyExpr::IsNotNull { column } => PolicyExpr::IsNotNull {
            column: column.clone(),
        },
        ArtifactPolicyExpr::SessionIsNotNull { path } => {
            PolicyExpr::SessionIsNotNull { path: path.clone() }
        }
        ArtifactPolicyExpr::Contains { column, value } => PolicyExpr::Contains {
            column: column.clone(),
            value: policy_value_from_artifact(value),
        },
        ArtifactPolicyExpr::SessionContains { path, value } => PolicyExpr::SessionContains {
            path: path.clone(),
            value: value.clone(),
        },
        ArtifactPolicyExpr::In {
            column,
            session_path,
        } => PolicyExpr::In {
            column: column.clone(),
            session_path: session_path.clone(),
        },
        ArtifactPolicyExpr::InList { column, values } => PolicyExpr::InList {
            column: column.clone(),
            values: values.iter().map(policy_value_from_artifact).collect(),
        },
        ArtifactPolicyExpr::SessionInList { path, values } => PolicyExpr::SessionInList {
            path: path.clone(),
            values: values.clone(),
        },
        ArtifactPolicyExpr::Exists { table, condition } => PolicyExpr::Exists {
            table: table.clone(),
            condition: Box::new(policy_from_artifact(condition)),
        },
        ArtifactPolicyExpr::ExistsRel { rel } => PolicyExpr::ExistsRel {
            rel: relation_from_artifact(rel),
        },
        ArtifactPolicyExpr::Inherits {
            operation,
            via_column,
            max_depth,
        } => PolicyExpr::Inherits {
            operation: *operation,
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        ArtifactPolicyExpr::InheritsReferencing {
            operation,
            source_table,
            via_column,
            max_depth,
        } => PolicyExpr::InheritsReferencing {
            operation: *operation,
            source_table: source_table.clone(),
            via_column: via_column.clone(),
            max_depth: *max_depth,
        },
        ArtifactPolicyExpr::And { exprs } => PolicyExpr::And(all(exprs)),
        ArtifactPolicyExpr::Or { exprs } => PolicyExpr::Or(all(exprs)),
        ArtifactPolicyExpr::Not { expr } => PolicyExpr::Not(Box::new(policy_from_artifact(expr))),
        ArtifactPolicyExpr::True => PolicyExpr::True,
        ArtifactPolicyExpr::False => PolicyExpr::False,
    }
}

fn column_ref_to_artifact(column: &RelationColumnRef) -> ArtifactColumnRef {
    ArtifactColumnRef {
        scope: column.scope.clone(),
        column: column.column.clone(),
    }
}

fn column_ref_from_artifact(column: &ArtifactColumnRef) -> RelationColumnRef {
    RelationColumnRef {
        scope: column.scope.clone(),
        column: column.column.clone(),
    }
}

fn row_id_to_artifact(row: RelationRowIdRef) -> ArtifactRowId {
    match row {
        RelationRowIdRef::Current => ArtifactRowId::Current,
        RelationRowIdRef::Outer => ArtifactRowId::Outer,
        RelationRowIdRef::Frontier => ArtifactRowId::Frontier,
    }
}

fn row_id_from_artifact(row: ArtifactRowId) -> RelationRowIdRef {
    match row {
        ArtifactRowId::Current => RelationRowIdRef::Current,
        ArtifactRowId::Outer => RelationRowIdRef::Outer,
        ArtifactRowId::Frontier => RelationRowIdRef::Frontier,
    }
}

fn key_to_artifact(key: &RelationKeyRef) -> ArtifactKeyRef {
    match key {
        RelationKeyRef::Column(column) => ArtifactKeyRef::Column {
            column: column_ref_to_artifact(column),
        },
        RelationKeyRef::RowId(row) => ArtifactKeyRef::RowId {
            row: row_id_to_artifact(*row),
        },
    }
}

fn key_from_artifact(key: &ArtifactKeyRef) -> RelationKeyRef {
    match key {
        ArtifactKeyRef::Column { column } => {
            RelationKeyRef::Column(column_ref_from_artifact(column))
        }
        ArtifactKeyRef::RowId { row } => RelationKeyRef::RowId(row_id_from_artifact(*row)),
    }
}

fn relation_to_artifact(expr: &RelationExpr) -> ArtifactRelationExpr {
    let boxed = |expr: &RelationExpr| Box::new(relation_to_artifact(expr));
    match expr {
        RelationExpr::TableScan { table, alias } => ArtifactRelationExpr::TableScan {
            table: table.clone(),
            alias: alias.clone(),
        },
        RelationExpr::Filter { input, predicate } => ArtifactRelationExpr::Filter {
            input: boxed(input),
            predicate: predicate_to_artifact(predicate),
        },
        RelationExpr::Union { inputs } => ArtifactRelationExpr::Union {
            inputs: inputs
                .iter()
                .map(|arm| ArtifactUnionArm {
                    label: arm.label.clone(),
                    input: relation_to_artifact(&arm.input),
                })
                .collect(),
        },
        RelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => ArtifactRelationExpr::Join {
            left: boxed(left),
            right: boxed(right),
            on: on
                .iter()
                .map(|condition| ArtifactJoinCondition {
                    left: column_ref_to_artifact(&condition.left),
                    right: column_ref_to_artifact(&condition.right),
                })
                .collect(),
            join_kind: match join_kind {
                RelationJoinKind::Inner => ArtifactJoinKind::Inner,
                RelationJoinKind::Left => ArtifactJoinKind::Left,
            },
        },
        RelationExpr::Project { input, columns } => ArtifactRelationExpr::Project {
            input: boxed(input),
            columns: columns
                .iter()
                .map(|column| ArtifactProjectColumn {
                    alias: column.alias.clone(),
                    expr: match &column.expr {
                        RelationProjectExpr::Column(column) => ArtifactKeyRef::Column {
                            column: column_ref_to_artifact(column),
                        },
                        RelationProjectExpr::RowId(row) => ArtifactKeyRef::RowId {
                            row: row_id_to_artifact(*row),
                        },
                    },
                })
                .collect(),
        },
        RelationExpr::Gather {
            seed,
            step,
            frontier_key,
            bound,
            dedupe_key,
        } => ArtifactRelationExpr::Gather {
            seed: boxed(seed),
            step: boxed(step),
            frontier_key: key_to_artifact(frontier_key),
            max_depth: match bound {
                RecursionBound::Fixpoint => None,
                RecursionBound::MaxDepth(depth) => Some(*depth),
            },
            dedupe_key: dedupe_key.iter().map(key_to_artifact).collect(),
        },
        RelationExpr::Distinct { input, key } => ArtifactRelationExpr::Distinct {
            input: boxed(input),
            key: key.iter().map(key_to_artifact).collect(),
        },
        RelationExpr::OrderBy { input, terms } => ArtifactRelationExpr::OrderBy {
            input: boxed(input),
            terms: terms
                .iter()
                .map(|term| ArtifactOrderBy {
                    column: column_ref_to_artifact(&term.column),
                    direction: match term.direction {
                        OrderDirection::Asc => ArtifactDirection::Asc,
                        OrderDirection::Desc => ArtifactDirection::Desc,
                    },
                })
                .collect(),
        },
        RelationExpr::Offset { input, offset } => ArtifactRelationExpr::Offset {
            input: boxed(input),
            offset: *offset,
        },
        RelationExpr::Limit { input, limit } => ArtifactRelationExpr::Limit {
            input: boxed(input),
            limit: *limit,
        },
    }
}

fn relation_from_artifact(expr: &ArtifactRelationExpr) -> RelationExpr {
    let boxed = |expr: &ArtifactRelationExpr| Box::new(relation_from_artifact(expr));
    match expr {
        ArtifactRelationExpr::TableScan { table, alias } => RelationExpr::TableScan {
            table: table.clone(),
            alias: alias.clone(),
        },
        ArtifactRelationExpr::Filter { input, predicate } => RelationExpr::Filter {
            input: boxed(input),
            predicate: predicate_from_artifact(predicate),
        },
        ArtifactRelationExpr::Union { inputs } => RelationExpr::Union {
            inputs: inputs
                .iter()
                .map(|arm| RelationUnionArm {
                    label: arm.label.clone(),
                    input: relation_from_artifact(&arm.input),
                })
                .collect(),
        },
        ArtifactRelationExpr::Join {
            left,
            right,
            on,
            join_kind,
        } => RelationExpr::Join {
            left: boxed(left),
            right: boxed(right),
            on: on
                .iter()
                .map(|condition| RelationJoinCondition {
                    left: column_ref_from_artifact(&condition.left),
                    right: column_ref_from_artifact(&condition.right),
                })
                .collect(),
            join_kind: match join_kind {
                ArtifactJoinKind::Inner => RelationJoinKind::Inner,
                ArtifactJoinKind::Left => RelationJoinKind::Left,
            },
        },
        ArtifactRelationExpr::Project { input, columns } => RelationExpr::Project {
            input: boxed(input),
            columns: columns
                .iter()
                .map(|column| RelationProjectColumn {
                    alias: column.alias.clone(),
                    expr: match &column.expr {
                        ArtifactKeyRef::Column { column } => {
                            RelationProjectExpr::Column(column_ref_from_artifact(column))
                        }
                        ArtifactKeyRef::RowId { row } => {
                            RelationProjectExpr::RowId(row_id_from_artifact(*row))
                        }
                    },
                })
                .collect(),
        },
        ArtifactRelationExpr::Gather {
            seed,
            step,
            frontier_key,
            max_depth,
            dedupe_key,
        } => RelationExpr::Gather {
            seed: boxed(seed),
            step: boxed(step),
            frontier_key: key_from_artifact(frontier_key),
            bound: match max_depth {
                None => RecursionBound::Fixpoint,
                Some(depth) => RecursionBound::MaxDepth(*depth),
            },
            dedupe_key: dedupe_key.iter().map(key_from_artifact).collect(),
        },
        ArtifactRelationExpr::Distinct { input, key } => RelationExpr::Distinct {
            input: boxed(input),
            key: key.iter().map(key_from_artifact).collect(),
        },
        ArtifactRelationExpr::OrderBy { input, terms } => RelationExpr::OrderBy {
            input: boxed(input),
            terms: terms
                .iter()
                .map(|term| RelationOrderBy {
                    column: column_ref_from_artifact(&term.column),
                    direction: match term.direction {
                        ArtifactDirection::Asc => OrderDirection::Asc,
                        ArtifactDirection::Desc => OrderDirection::Desc,
                    },
                })
                .collect(),
        },
        ArtifactRelationExpr::Offset { input, offset } => RelationExpr::Offset {
            input: boxed(input),
            offset: *offset,
        },
        ArtifactRelationExpr::Limit { input, limit } => RelationExpr::Limit {
            input: boxed(input),
            limit: *limit,
        },
    }
}

fn value_ref_to_artifact(value: &RelationValueRef) -> ArtifactValueRef {
    match value {
        RelationValueRef::Literal(value) => ArtifactValueRef::Literal {
            value: value.clone(),
        },
        RelationValueRef::Param(name) => ArtifactValueRef::Param { name: name.clone() },
        RelationValueRef::SessionRef(path) => ArtifactValueRef::SessionRef { path: path.clone() },
        RelationValueRef::OuterColumn(column) => ArtifactValueRef::OuterColumn {
            column: column_ref_to_artifact(column),
        },
        RelationValueRef::FrontierColumn(column) => ArtifactValueRef::FrontierColumn {
            column: column_ref_to_artifact(column),
        },
        RelationValueRef::RowId(row) => ArtifactValueRef::RowId {
            row: row_id_to_artifact(*row),
        },
    }
}

fn value_ref_from_artifact(value: &ArtifactValueRef) -> RelationValueRef {
    match value {
        ArtifactValueRef::Literal { value } => RelationValueRef::Literal(value.clone()),
        ArtifactValueRef::Param { name } => RelationValueRef::Param(name.clone()),
        ArtifactValueRef::SessionRef { path } => RelationValueRef::SessionRef(path.clone()),
        ArtifactValueRef::OuterColumn { column } => {
            RelationValueRef::OuterColumn(column_ref_from_artifact(column))
        }
        ArtifactValueRef::FrontierColumn { column } => {
            RelationValueRef::FrontierColumn(column_ref_from_artifact(column))
        }
        ArtifactValueRef::RowId { row } => RelationValueRef::RowId(row_id_from_artifact(*row)),
    }
}

fn predicate_to_artifact(predicate: &RelationPredicate) -> ArtifactPredicate {
    let all = |exprs: &[RelationPredicate]| exprs.iter().map(predicate_to_artifact).collect();
    match predicate {
        RelationPredicate::Cmp { left, op, right } => ArtifactPredicate::Cmp {
            left: column_ref_to_artifact(left),
            op: *op,
            right: value_ref_to_artifact(right),
        },
        RelationPredicate::IsNull { column } => ArtifactPredicate::IsNull {
            column: column_ref_to_artifact(column),
        },
        RelationPredicate::IsNotNull { column } => ArtifactPredicate::IsNotNull {
            column: column_ref_to_artifact(column),
        },
        RelationPredicate::In { left, values } => ArtifactPredicate::In {
            left: column_ref_to_artifact(left),
            values: values.iter().map(value_ref_to_artifact).collect(),
        },
        RelationPredicate::Contains { left, right } => ArtifactPredicate::Contains {
            left: column_ref_to_artifact(left),
            right: value_ref_to_artifact(right),
        },
        RelationPredicate::EnumMatch {
            column,
            case,
            payload,
        } => ArtifactPredicate::EnumMatch {
            column: column_ref_to_artifact(column),
            case: case.clone(),
            payload: Box::new(predicate_to_artifact(payload)),
        },
        RelationPredicate::And(exprs) => ArtifactPredicate::And { exprs: all(exprs) },
        RelationPredicate::Or(exprs) => ArtifactPredicate::Or { exprs: all(exprs) },
        RelationPredicate::Not(expr) => ArtifactPredicate::Not {
            expr: Box::new(predicate_to_artifact(expr)),
        },
        RelationPredicate::True => ArtifactPredicate::True,
        RelationPredicate::False => ArtifactPredicate::False,
    }
}

fn predicate_from_artifact(predicate: &ArtifactPredicate) -> RelationPredicate {
    let all = |exprs: &[ArtifactPredicate]| exprs.iter().map(predicate_from_artifact).collect();
    match predicate {
        ArtifactPredicate::Cmp { left, op, right } => RelationPredicate::Cmp {
            left: column_ref_from_artifact(left),
            op: *op,
            right: value_ref_from_artifact(right),
        },
        ArtifactPredicate::IsNull { column } => RelationPredicate::IsNull {
            column: column_ref_from_artifact(column),
        },
        ArtifactPredicate::IsNotNull { column } => RelationPredicate::IsNotNull {
            column: column_ref_from_artifact(column),
        },
        ArtifactPredicate::In { left, values } => RelationPredicate::In {
            left: column_ref_from_artifact(left),
            values: values.iter().map(value_ref_from_artifact).collect(),
        },
        ArtifactPredicate::Contains { left, right } => RelationPredicate::Contains {
            left: column_ref_from_artifact(left),
            right: value_ref_from_artifact(right),
        },
        ArtifactPredicate::EnumMatch {
            column,
            case,
            payload,
        } => RelationPredicate::EnumMatch {
            column: column_ref_from_artifact(column),
            case: case.clone(),
            payload: Box::new(predicate_from_artifact(payload)),
        },
        ArtifactPredicate::And { exprs } => RelationPredicate::And(all(exprs)),
        ArtifactPredicate::Or { exprs } => RelationPredicate::Or(all(exprs)),
        ArtifactPredicate::Not { expr } => {
            RelationPredicate::Not(Box::new(predicate_from_artifact(expr)))
        }
        ArtifactPredicate::True => RelationPredicate::True,
        ArtifactPredicate::False => RelationPredicate::False,
    }
}

#[cfg(test)]
#[path = "artifact_tests.rs"]
mod tests;
