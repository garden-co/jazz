//! Jazz schema metadata and lowering to groove storage schemas. This module
//! owns table/column declarations, merge-strategy declarations, policy metadata,
//! storage-table naming, and migration-lens schema surfaces from
//! `jazz/SPEC/10_lenses_migrations.md`;
//! policy evaluation lives in [`crate::node::policy`], query shapes in
//! [`crate::query`], and runtime catalogue ingestion in [`crate::node::ingest`].
//! In the layer map it is the schema bridge from Jazz concepts to groove tables.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use groove::records::{
    RecordDescriptor, ScalarEnumSchema, SystemVariantRegistry, Value, ValueType,
};
use groove::schema::{
    ColumnType as GrooveColumnType, DatabaseSchema as GrooveDatabaseSchema,
    DirectRecordStoreSchema, IndexSchema as GrooveIndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, TableSchema as GrooveTableSchema,
};
use groove::storage::StorageLayout;

use crate::ids::{BranchDimensionId, SchemaVersionId};
use crate::protocol::{BranchDimensionValue, BranchKey, BranchSelector};
use crate::query::Query;
#[cfg(test)]
use crate::query::{claim, col, eq};
use crate::tools::public_schema::Schema as PublicSchema;

pub use crate::tools::public_schema_convert::SchemaConversionError;

/// Namespace used for schema-version UUIDv5 ids.
pub const SCHEMA_VERSION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("61b9ef21-3195-50e8-87fc-2aa83a6f74e3");

/// Direct groove record store used for persisted fast known-state facts.
pub const KNOWN_STATE_FACTS_STORE: &str = "jazz_known_state_facts";
/// Direct groove record store used for persisted settled result memberships.
pub const SETTLED_RESULT_MEMBERS_STORE: &str = "jazz_settled_result_members";
/// Direct groove record store used for persisted settled program facts.
pub const SETTLED_PROGRAM_FACTS_STORE: &str = "jazz_settled_program_facts";
/// Direct groove record store used to distinguish clean shutdown from crash
/// recovery windows for bounded startup repair.
pub const CLEAN_CLOSE_MARKERS_STORE: &str = "jazz_clean_close_markers";
/// Direct groove record store used to bound crash recovery work when a process
/// dies after a durable consistency boundary but before clean close runs.
pub const STORAGE_CONSISTENCY_MARKERS_STORE: &str = "jazz_storage_consistency_markers";
/// Node-local derived content-head table used to avoid row-history scans on
/// ordinary accepted writes. It is storage metadata, never wire or app data.
pub const MERGE_HEADS_TABLE: &str = "jazz_merge_heads";

/// Source-backed Jazz schema accepted by public database APIs.
///
/// The developer-authored source is retained for durable catalogue
/// publication while [`RuntimeSchema`] is the derived, in-memory engine form.
#[derive(Clone, Debug)]
pub struct JazzSchema {
    public_schema: PublicSchema,
    runtime: RuntimeSchema,
}

impl PartialEq for JazzSchema {
    fn eq(&self, other: &Self) -> bool {
        self.runtime == other.runtime
    }
}

impl JazzSchema {
    /// Construct an empty source-backed schema.
    pub fn empty() -> Self {
        Self::new(&PublicSchema::new()).expect("empty public schema always compiles")
    }

    /// Compile a developer-authored public schema and retain its durable source.
    pub fn new(schema: &PublicSchema) -> Result<Self, SchemaConversionError> {
        crate::tools::public_schema_convert::convert_public_schema(schema)
    }

    /// Return the developer-authored public schema retained for persistence.
    pub fn public_schema(&self) -> &PublicSchema {
        &self.public_schema
    }

    /// Return the compiled application tables used by the runtime.
    pub fn tables(&self) -> &[TableSchema] {
        &self.runtime.tables
    }

    pub(crate) fn from_runtime(public_schema: PublicSchema, runtime: RuntimeSchema) -> Self {
        Self {
            public_schema,
            runtime,
        }
    }

    pub(crate) fn runtime(&self) -> &RuntimeSchema {
        &self.runtime
    }

    #[cfg(any(test, feature = "testing"))]
    pub(crate) fn runtime_mut_for_testing(&mut self) -> &mut RuntimeSchema {
        &mut self.runtime
    }

    #[cfg(test)]
    pub(crate) fn into_runtime(self) -> RuntimeSchema {
        self.runtime
    }

    /// Construct a compiled schema for internal engine tests whose tables
    /// already carry branch-dimension bindings.
    #[cfg(test)]
    pub(crate) fn new_with_branch_dimensions(
        branch_dimensions: impl IntoIterator<Item = BranchDimensionSchema>,
        tables: impl IntoIterator<Item = TableSchema>,
    ) -> Self {
        Self::from_runtime(
            PublicSchema::new(),
            RuntimeSchema::new_with_branch_dimensions(branch_dimensions, tables),
        )
    }
}

impl Deref for JazzSchema {
    type Target = RuntimeSchema;

    fn deref(&self) -> &Self::Target {
        &self.runtime
    }
}

/// Compiled logical schema used internally by the Jazz engine.
#[doc(hidden)]
#[derive(Clone, Debug)]
pub struct RuntimeSchema {
    /// Globally named, lineage-stable branch dimensions in canonical order.
    pub branch_dimensions: Vec<BranchDimensionSchema>,
    /// Application tables in the schema.
    pub tables: Vec<TableSchema>,
}

impl PartialEq for RuntimeSchema {
    fn eq(&self, other: &Self) -> bool {
        self.branch_dimensions == other.branch_dimensions && self.tables == other.tables
    }
}

impl RuntimeSchema {
    /// Construct a compiled schema from already-lowered application tables.
    #[cfg(test)]
    pub(crate) fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self::new_with_branch_dimensions([], tables)
    }

    /// Construct a schema with its globally named dimensions and tables atomically.
    ///
    /// This is the primary constructor for schemas whose tables already bind
    /// branch dimensions: validation observes the complete declaration rather
    /// than an invalid intermediate builder state.
    #[cfg(test)]
    pub(crate) fn new_with_branch_dimensions(
        branch_dimensions: impl IntoIterator<Item = BranchDimensionSchema>,
        tables: impl IntoIterator<Item = TableSchema>,
    ) -> Self {
        Self {
            branch_dimensions: branch_dimensions.into_iter().collect(),
            tables: tables.into_iter().collect(),
        }
        .validated()
    }

    /// Declare one globally named branch dimension.
    pub fn with_branch_dimension(mut self, dimension: BranchDimensionSchema) -> Self {
        self.branch_dimensions.push(dimension);
        self.validated()
    }

    /// Project a named selector onto one table and validate its exact branch key.
    pub fn project_branch_selector(
        &self,
        table: &TableSchema,
        selector: &BranchSelector,
    ) -> Result<(BranchKey, BTreeMap<String, Value>), String> {
        if selector.dimensions.len() != table.branch_by.len() {
            return Err(format!(
                "branch selector for {} must provide exactly {} dimensions",
                table.name,
                table.branch_by.len()
            ));
        }
        let mut dimensions = Vec::with_capacity(table.branch_by.len());
        let mut cells = BTreeMap::new();
        for binding in &table.branch_by {
            let dimension = self
                .branch_dimensions
                .iter()
                .find(|candidate| candidate.id == binding.dimension)
                .ok_or_else(|| format!("unknown branch dimension on {}", table.name))?;
            let encoded = selector
                .dimensions
                .get(&dimension.name)
                .ok_or_else(|| format!("missing branch dimension {}", dimension.name))?;
            let value = encoded
                .decode()
                .map_err(|_| format!("invalid branch dimension {} encoding", dimension.name))?;
            if BranchDimensionValue::from(value.clone()) != *encoded {
                return Err(format!(
                    "non-canonical branch dimension {} encoding",
                    dimension.name
                ));
            }
            RecordDescriptor::new([("value", dimension.column_type.clone())])
                .create(std::slice::from_ref(&value))
                .map_err(|_| format!("invalid branch dimension {} value", dimension.name))?;
            dimensions.push((dimension.id, encoded.clone()));
            cells.insert(binding.column.clone(), value);
        }
        dimensions.sort_by_key(|(id, _)| *id);
        Ok((BranchKey { dimensions }, cells))
    }

    /// Validate a schema-wide view selector, then project it onto one table's subset.
    pub fn project_branch_view_selector(
        &self,
        table: &TableSchema,
        selector: &BranchSelector,
    ) -> Result<(BranchKey, BTreeMap<String, Value>), String> {
        if selector.dimensions.len() != self.branch_dimensions.len()
            || self
                .branch_dimensions
                .iter()
                .any(|dimension| !selector.dimensions.contains_key(&dimension.name))
        {
            return Err(
                "branch view selector must provide every schema dimension exactly once".to_owned(),
            );
        }
        let projected = BranchSelector {
            dimensions: table
                .branch_by
                .iter()
                .map(|binding| {
                    let dimension = self
                        .branch_dimensions
                        .iter()
                        .find(|dimension| dimension.id == binding.dimension)
                        .expect("validated table binding");
                    (
                        dimension.name.clone(),
                        selector.dimensions[&dimension.name].clone(),
                    )
                })
                .collect(),
        };
        self.project_branch_selector(table, &projected)
    }

    /// Compare a stored key with a selector in this schema, interpreting
    /// dimensions absent from older monotone schemas at their migration defaults.
    pub(crate) fn branch_key_matches(
        &self,
        table: &TableSchema,
        stored: &BranchKey,
        selected: &BranchKey,
    ) -> bool {
        table.branch_by.iter().all(|binding| {
            let dimension = self
                .branch_dimensions
                .iter()
                .find(|dimension| dimension.id == binding.dimension)
                .expect("validated branch dimension binding");
            let selected = selected
                .dimensions
                .iter()
                .find(|(id, _)| *id == binding.dimension)
                .map(|(_, value)| value);
            let stored = stored
                .dimensions
                .iter()
                .find(|(id, _)| *id == binding.dimension)
                .map(|(_, value)| value)
                .cloned()
                .unwrap_or_else(|| BranchDimensionValue::from(dimension.migration_default.clone()));
            selected == Some(&stored)
        }) && stored.dimensions.iter().all(|(id, _)| {
            table
                .branch_by
                .iter()
                .any(|binding| binding.dimension == *id)
        })
    }

    /// Expand an older table-local key with immutable defaults from the
    /// current monotone dimension declaration.
    pub(crate) fn normalize_branch_key(
        &self,
        table: &TableSchema,
        stored: &BranchKey,
    ) -> Result<BranchKey, String> {
        if stored.dimensions.iter().any(|(id, _)| {
            !table
                .branch_by
                .iter()
                .any(|binding| binding.dimension == *id)
        }) {
            return Err(format!(
                "stored branch key has an unknown dimension on {}",
                table.name
            ));
        }
        let mut dimensions = table
            .branch_by
            .iter()
            .map(|binding| {
                let dimension = self
                    .branch_dimensions
                    .iter()
                    .find(|dimension| dimension.id == binding.dimension)
                    .ok_or_else(|| format!("unknown branch dimension on {}", table.name))?;
                let value = stored
                    .dimensions
                    .iter()
                    .find(|(id, _)| *id == binding.dimension)
                    .map(|(_, value)| value.clone())
                    .unwrap_or_else(|| {
                        BranchDimensionValue::from(dimension.migration_default.clone())
                    });
                Ok((binding.dimension, value))
            })
            .collect::<Result<Vec<_>, String>>()?;
        dimensions.sort_by_key(|(dimension, _)| *dimension);
        Ok(BranchKey { dimensions })
    }

    /// Reconstruct the table-local named selector for an exact stored key.
    pub(crate) fn branch_selector_for_key(
        &self,
        table: &TableSchema,
        key: &BranchKey,
    ) -> Result<BranchSelector, String> {
        let mut dimensions = BTreeMap::new();
        for binding in &table.branch_by {
            let dimension = self
                .branch_dimensions
                .iter()
                .find(|dimension| dimension.id == binding.dimension)
                .ok_or_else(|| format!("unknown branch dimension on {}", table.name))?;
            let value = key
                .dimensions
                .iter()
                .find(|(id, _)| *id == binding.dimension)
                .map(|(_, value)| value.clone())
                .unwrap_or_else(|| BranchDimensionValue::from(dimension.migration_default.clone()));
            dimensions.insert(dimension.name.clone(), value);
        }
        Ok(BranchSelector { dimensions })
    }

    fn validated(self) -> Self {
        let mut dimension_ids = BTreeSet::new();
        let mut dimension_names = BTreeSet::new();
        for dimension in &self.branch_dimensions {
            assert!(
                dimension_ids.insert(dimension.id),
                "duplicate branch dimension id"
            );
            assert!(
                dimension_names.insert(dimension.name.clone()),
                "duplicate branch dimension name"
            );
            assert!(
                !matches!(dimension.column_type, GrooveColumnType::Nullable(_)),
                "branch dimensions must be non-nullable"
            );
            assert!(
                matches!(
                    dimension.column_type,
                    GrooveColumnType::Uuid
                        | GrooveColumnType::U8
                        | GrooveColumnType::U16
                        | GrooveColumnType::U32
                        | GrooveColumnType::U64
                        | GrooveColumnType::I32
                        | GrooveColumnType::I64
                        | GrooveColumnType::EnumTag(_)
                ),
                "branch dimensions require UUID, stable enum, or fixed-width integer values"
            );
            assert!(
                RecordDescriptor::new([("value", dimension.column_type.clone())])
                    .create(std::slice::from_ref(&dimension.migration_default))
                    .is_ok(),
                "branch dimension migration default must match its declared type"
            );
        }
        for table in &self.tables {
            let mut bound_dimensions = BTreeSet::new();
            for binding in &table.branch_by {
                let dimension = self
                    .branch_dimensions
                    .iter()
                    .find(|dimension| dimension.id == binding.dimension)
                    .unwrap_or_else(|| panic!("unknown branch dimension on {}", table.name));
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == binding.column)
                    .unwrap_or_else(|| {
                        panic!("unknown branch column {}.{}", table.name, binding.column)
                    });
                assert!(
                    bound_dimensions.insert(binding.dimension),
                    "duplicate table branch dimension"
                );
                assert_eq!(
                    column.column_type, dimension.column_type,
                    "branch dimension column type mismatch"
                );
            }
            for (column_name, strategy) in &table.merge_strategies {
                let column = table
                    .columns
                    .iter()
                    .find(|candidate| candidate.name == *column_name)
                    .unwrap_or_else(|| {
                        panic!(
                            "merge strategy declared for unknown column {}.{}",
                            table.name, column_name
                        )
                    });
                match strategy {
                    MergeStrategy::Lww => {}
                    MergeStrategy::Counter => {
                        assert!(
                            is_counter_column_type(&column.column_type),
                            "counter merge strategy requires a non-nullable integer column: {}.{}",
                            table.name,
                            column.name
                        );
                    }
                    MergeStrategy::GSet => {
                        assert!(
                            is_gset_column_type(&column.column_type),
                            "g-set merge strategy requires a non-nullable array column: {}.{}",
                            table.name,
                            column.name
                        );
                    }
                }
            }
            if let Some(policy) = &table.read_policy {
                assert_eq!(policy.table, table.name, "read policy table must match");
                policy.validate_runtime(&self).unwrap_or_else(|error| {
                    panic!("valid read policy shape for {}: {error:?}", table.name)
                });
            }
            for (label, policy) in table.write_policies.iter() {
                assert_eq!(
                    policy.table, table.name,
                    "{label} write policy table must match"
                );
                policy
                    .validate_runtime(&self)
                    .unwrap_or_else(|_| panic!("valid {label} write policy shape"));
            }
        }
        self
    }

    /// Lower the Jazz schema into groove storage tables.
    pub fn lower_to_groove(&self) -> GrooveDatabaseSchema {
        self.with_jazz_direct_record_stores(GrooveDatabaseSchema::new(self.storage_tables()))
    }

    /// Lower only the fixed metadata tables needed for the first open stage.
    pub fn lower_catalogue_meta_to_groove(&self) -> GrooveDatabaseSchema {
        self.with_jazz_direct_record_stores(GrooveDatabaseSchema::new(
            self.catalogue_meta_storage_tables(),
        ))
    }

    /// Return the required RocksDB column-family names.
    pub fn column_families(&self) -> Vec<String> {
        let lowered = self.lower_to_groove();
        StorageLayout::jazz_class_v1().physical_column_families(
            lowered
                .column_families()
                .into_iter()
                .chain(std::iter::once("indices"))
                // A schema-independent runtime may open before its first typed
                // application view is registered. Keep the shared physical
                // row classes available even for the empty bootstrap schema.
                .chain(std::iter::once("jazz_physical_history"))
                .chain(std::iter::once("jazz_physical_register"))
                .chain(std::iter::once("jazz_physical_global_current"))
                .chain(std::iter::once("jazz_physical_ahead_current")),
        )
    }

    /// Return physical storage column-family names for Jazz's class-CF layout.
    pub fn physical_column_families(&self) -> Vec<String> {
        self.column_families()
    }

    /// Return all storage tables used by Jazz.
    pub fn storage_tables(&self) -> Vec<GrooveTableSchema> {
        let mut tables = vec![
            nodes_table(),
            schema_versions_table(),
            catalogue_table(),
            catalogue_pointer_table(),
            transactions_table(),
            rejected_transactions_table(),
            pending_edges_table(),
            merge_heads_table(),
        ];
        tables.push(global_changes_table());
        tables.push(shared_deletion_history_table());
        tables
    }

    /// Return the version-independent metadata tables used by staged catalogue open.
    pub fn catalogue_meta_storage_tables(&self) -> Vec<GrooveTableSchema> {
        vec![
            nodes_table(),
            schema_versions_table(),
            catalogue_table(),
            catalogue_pointer_table(),
        ]
    }

    /// Return the canonical byte encoding used to address this schema version.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        canonical_schema_bytes(self)
    }

    /// Return the content-addressed id for this schema.
    pub fn version_id(&self) -> SchemaVersionId {
        SchemaVersionId(uuid::Uuid::new_v5(
            &SCHEMA_VERSION_NAMESPACE,
            &self.canonical_bytes(),
        ))
    }

    fn with_jazz_direct_record_stores(&self, schema: GrooveDatabaseSchema) -> GrooveDatabaseSchema {
        schema
            .with_direct_record_store(DirectRecordStoreSchema::new(
                KNOWN_STATE_FACTS_STORE,
                RecordDescriptor::new([
                    ("shape_id", ValueType::Uuid),
                    ("binding_id", ValueType::Uuid),
                    ("read_view_id", ValueType::Uuid),
                ]),
                RecordDescriptor::new([
                    ("settled_through", ValueType::U64),
                    ("authorization_progress", ValueType::U64),
                ]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                SETTLED_RESULT_MEMBERS_STORE,
                RecordDescriptor::new([
                    ("shape_id", ValueType::Uuid),
                    ("binding_id", ValueType::Uuid),
                    ("read_view_id", ValueType::Uuid),
                    ("member", ValueType::Bytes),
                ]),
                RecordDescriptor::new([("present", ValueType::U64)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                SETTLED_PROGRAM_FACTS_STORE,
                RecordDescriptor::new([
                    ("shape_id", ValueType::Uuid),
                    ("binding_id", ValueType::Uuid),
                    ("read_view_id", ValueType::Uuid),
                    ("fact", ValueType::Bytes),
                ]),
                RecordDescriptor::new([("present", ValueType::U64)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                CLEAN_CLOSE_MARKERS_STORE,
                RecordDescriptor::new([("marker", ValueType::String)]),
                RecordDescriptor::new([("version", ValueType::U64), ("node", ValueType::Uuid)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                STORAGE_CONSISTENCY_MARKERS_STORE,
                RecordDescriptor::new([("marker", ValueType::String)]),
                RecordDescriptor::new([
                    ("version", ValueType::U64),
                    ("node", ValueType::Uuid),
                    ("tx_time", ValueType::U64),
                ]),
            ))
    }
}

/// One globally named branch coordinate whose identity survives column renames.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct BranchDimensionSchema {
    /// Stable semantic identity.
    pub id: BranchDimensionId,
    /// Stable selector name used by branch views.
    pub name: String,
    /// Non-null, canonically key-encodable value type.
    pub column_type: GrooveColumnType,
    /// Immutable default used when this dimension is added monotonically.
    pub migration_default: Value,
}

impl BranchDimensionSchema {
    /// Construct a branch dimension declaration.
    pub fn new(
        id: BranchDimensionId,
        name: impl Into<String>,
        column_type: GrooveColumnType,
        migration_default: Value,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            column_type,
            migration_default,
        }
    }
}

/// Binding from an ordinary application column to a stable branch dimension.
#[derive(Clone, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub struct BranchDimensionBinding {
    /// Application column, freely renameable through schema lineage.
    pub column: String,
    /// Stable dimension identity.
    pub dimension: BranchDimensionId,
}

/// Per-column strategy used when upstream nodes merge concurrent content heads.
///
/// Counter deltas are represented on the wire as an absolute user cell plus the
/// version's parents. The observed base is reconstructed from the parent set;
/// merge computes `merged(parent union) + sum(version_value - merged(parents))`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum MergeStrategy {
    /// Highest HLC/TxId value wins for this column.
    #[default]
    Lww,
    /// Concurrent integer deltas from observed bases are summed.
    Counter,
    /// Array elements grow monotonically by union over the version DAG.
    GSet,
}

fn is_counter_column_type(column_type: &GrooveColumnType) -> bool {
    matches!(
        column_type,
        GrooveColumnType::U8
            | GrooveColumnType::U16
            | GrooveColumnType::U32
            | GrooveColumnType::U64
            | GrooveColumnType::I32
            | GrooveColumnType::I64
    )
}

fn is_gset_column_type(column_type: &GrooveColumnType) -> bool {
    matches!(column_type, GrooveColumnType::Array(_))
}

/// Semantics declared for a built-in column transform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnTransformSemantics {
    /// The transform has a total inverse over its declared value domain.
    pub(crate) bijective: bool,
    /// Equal canonical source values remain equal after transform and inverse.
    pub(crate) canonical_equality_preserving: bool,
}

/// Return the semantics for a registered built-in column transform.
pub(crate) fn registered_column_transform(key: &str) -> Option<ColumnTransformSemantics> {
    match key {
        "identity" | "jazz.identity" => Some(ColumnTransformSemantics {
            bijective: true,
            canonical_equality_preserving: true,
        }),
        _ => None,
    }
}

/// Application column declaration.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct ColumnSchema {
    /// Logical column name.
    pub name: String,
    /// Groove storage type used for this column's cell value.
    pub column_type: GrooveColumnType,
    /// Literal value used when an insert omits this column.
    #[serde(default)]
    pub default: Option<Value>,
}

impl ColumnSchema {
    /// Construct an ordinary column from a groove storage type.
    pub fn new(name: impl Into<String>, column_type: GrooveColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            default: None,
        }
    }

    /// Attach a literal insert default to this column.
    pub fn with_default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }
}

impl From<groove::schema::ColumnSchema> for ColumnSchema {
    fn from(column: groove::schema::ColumnSchema) -> Self {
        Self {
            name: column.name,
            column_type: column.column_type,
            default: None,
        }
    }
}

/// Operation-specific write policy clauses for an application table.
#[derive(Clone, Debug, Default, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct WritePolicies {
    /// Policy evaluated against the inserted row.
    #[serde(default)]
    pub insert_check: Option<Query>,
    /// Policy evaluated against the row before an update.
    #[serde(default)]
    pub update_using: Option<Query>,
    /// Policy evaluated against the row after an update.
    #[serde(default)]
    pub update_check: Option<Query>,
    /// Policy evaluated against the row being deleted.
    #[serde(default)]
    pub delete_using: Option<Query>,
}

impl WritePolicies {
    /// Build operation-specific clauses from the legacy single write policy.
    pub fn legacy(policy: Option<Query>) -> Self {
        Self {
            insert_check: policy.clone(),
            update_using: policy.clone(),
            update_check: policy.clone(),
            delete_using: policy,
        }
    }

    /// Iterate over every present operation-specific clause.
    pub fn iter(&self) -> impl Iterator<Item = (&'static str, &Query)> {
        [
            ("insert_check", self.insert_check.as_ref()),
            ("update_using", self.update_using.as_ref()),
            ("update_check", self.update_check.as_ref()),
            ("delete_using", self.delete_using.as_ref()),
        ]
        .into_iter()
        .filter_map(|(label, policy)| policy.map(|policy| (label, policy)))
    }

    /// Return one representative policy for coarse subscription scoping.
    pub fn any(&self) -> Option<Query> {
        self.insert_check
            .clone()
            .or_else(|| self.update_check.clone())
            .or_else(|| self.update_using.clone())
            .or_else(|| self.delete_using.clone())
    }
}

/// Application table whose rows are stored as immutable history versions.
#[doc(hidden)]
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableSchema {
    /// Logical table name.
    pub name: String,
    /// User columns.
    pub columns: Vec<ColumnSchema>,
    /// Ordinary columns that form this table's exact branch key.
    #[serde(default)]
    pub branch_by: Vec<BranchDimensionBinding>,
    /// Jazz-level reference metadata by source column name.
    pub references: BTreeMap<String, String>,
    /// Read policy used when serving views.
    pub read_policy: Option<Query>,
    /// Write policies used by fate authority.
    #[serde(default)]
    pub write_policies: WritePolicies,
    /// User columns materialized and indexed on the global-current content table.
    #[serde(default)]
    pub indexed_columns: BTreeSet<String>,
    /// Per-column merge strategy. Columns omitted here use [`MergeStrategy::Lww`].
    #[serde(default)]
    pub merge_strategies: BTreeMap<String, MergeStrategy>,
}

impl TableSchema {
    /// Construct a public/read-anyone table.
    pub(crate) fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<ColumnSchema>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            branch_by: Vec::new(),
            references: BTreeMap::new(),
            read_policy: None,
            write_policies: WritePolicies::default(),
            indexed_columns: BTreeSet::new(),
            merge_strategies: BTreeMap::new(),
        }
    }

    /// Bind an ordinary application column to a stable branch dimension.
    pub fn with_branch_dimension(
        mut self,
        column: impl Into<String>,
        dimension: BranchDimensionId,
    ) -> Self {
        self.branch_by.push(BranchDimensionBinding {
            column: column.into(),
            dimension,
        });
        self
    }

    /// Mark a user column as referencing another Jazz table.
    #[cfg(test)]
    pub(crate) fn with_reference(
        mut self,
        column: impl Into<String>,
        target_table: impl Into<String>,
    ) -> Self {
        self.references.insert(column.into(), target_table.into());
        self
    }

    /// Set a user column's merge strategy.
    #[cfg(test)]
    pub(crate) fn with_column_merge_strategy(
        mut self,
        column: impl Into<String>,
        strategy: MergeStrategy,
    ) -> Self {
        self.merge_strategies.insert(column.into(), strategy);
        self
    }

    /// Return the merge strategy for a user column.
    pub fn merge_strategy(&self, column: &str) -> MergeStrategy {
        self.merge_strategies
            .get(column)
            .copied()
            .unwrap_or_default()
    }

    /// Set the table read policy.
    #[cfg(test)]
    pub(crate) fn with_read_policy(mut self, read_policy: impl Into<Option<Query>>) -> Self {
        self.read_policy = read_policy.into();
        self
    }

    /// Set the table write policy.
    #[cfg(test)]
    pub(crate) fn with_write_policy(mut self, write_policy: impl Into<Option<Query>>) -> Self {
        self.write_policies = WritePolicies::legacy(write_policy.into());
        self
    }

    /// Set operation-specific write policies.
    #[cfg(test)]
    pub(crate) fn with_write_policies(mut self, write_policies: WritePolicies) -> Self {
        self.write_policies = write_policies;
        self
    }

    /// Return the storage table for rejected versions of this application table.
    pub fn rejected_versions_storage_table(&self) -> GrooveTableSchema {
        let mut columns = vec![
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("row_uuid", GrooveColumnType::Uuid),
            column("layer", GrooveColumnType::Bytes),
            column("parents", tx_id_column().array_of()),
            column("_deletion", deletion_column().nullable()),
        ];
        columns.extend(self.columns.iter().map(|user_column| {
            column(
                format!("user_{}", user_column.name),
                user_column.column_type.clone().nullable(),
            )
        }));
        GrooveTableSchema::new(format!("jazz_{}_rejected_versions", self.name), columns)
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::bytes("layer"),
            ]))
    }

    /// Return the storage history table for this application table.
    pub fn history_storage_table(&self) -> GrooveTableSchema {
        self.history_storage_table_named(format!("jazz_{}_history", self.name))
    }

    fn history_storage_table_named(&self, name: String) -> GrooveTableSchema {
        let mut columns = vec![
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", GrooveColumnType::Uuid),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", GrooveColumnType::Uuid),
            column("updated_at", GrooveColumnType::U64),
        ];
        columns.extend(self.columns.iter().map(|user_column| {
            column(
                format!("user_{}", user_column.name),
                user_column.column_type.clone().nullable(),
            )
        }));
        // Absent on legacy records. When present, this is a serialized set of
        // user columns explicitly authored by this version.
        columns.push(column(
            "authored_columns",
            GrooveColumnType::Bytes.nullable(),
        ));

        GrooveTableSchema::new(name, columns)
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(GrooveIndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
            ))
    }

    /// Return the storage table for deletion-register versions.
    pub fn register_storage_table(&self) -> GrooveTableSchema {
        self.register_storage_table_named(format!("jazz_{}_register", self.name))
    }

    fn register_storage_table_named(&self, name: String) -> GrooveTableSchema {
        GrooveTableSchema::new(
            name,
            [
                column("branch_key", GrooveColumnType::Bytes),
                column("row_uuid", GrooveColumnType::Uuid),
                column("tx_time", GrooveColumnType::U64),
                column("tx_node_id", GrooveColumnType::U64),
                column("schema_version", GrooveColumnType::U64),
                column("parents", tx_id_column().array_of()),
                column("created_by", GrooveColumnType::Uuid),
                column("created_at", GrooveColumnType::U64),
                column("updated_by", GrooveColumnType::Uuid),
                column("updated_at", GrooveColumnType::U64),
                column("_deletion", deletion_column()),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
            PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
        ]))
        .with_index(GrooveIndexSchema::new(
            "by_tx",
            ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
        ))
    }

    /// Return per-layer global-current tables for content and register winners.
    pub fn global_current_storage_tables(&self) -> Vec<GrooveTableSchema> {
        let indexed_columns = self.global_current_indexed_columns();
        let mut content_columns = vec![
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", GrooveColumnType::Uuid),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", GrooveColumnType::Uuid),
            column("updated_at", GrooveColumnType::U64),
            column("global_time", GrooveColumnType::U64.nullable()),
        ];
        // Carry every user column (not only indexed ones) so the global-current
        // table is a self-sufficient current-row index: whole-table current
        // reads and subscriptions resolve cells here in O(current rows) without
        // joining the full history table. Secondary indexes are still built only
        // on the indexed subset below.
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                format!("user_{}", user_column.name),
                user_column.column_type.clone().nullable(),
            )
        }));
        content_columns.push(column(
            "authored_columns",
            GrooveColumnType::Bytes.nullable(),
        ));
        let mut content_table = GrooveTableSchema::new(
            format!("jazz_{}_global_current", self.name),
            content_columns,
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
        ]));
        for indexed in &indexed_columns {
            content_table = content_table.with_index(GrooveIndexSchema::new(
                global_current_index_name(indexed),
                ["branch_key".to_owned(), format!("user_{indexed}")],
            ));
        }
        vec![
            content_table,
            GrooveTableSchema::new(
                format!("jazz_{}_register_global_current", self.name),
                [
                    column("branch_key", GrooveColumnType::Bytes),
                    column("row_uuid", GrooveColumnType::Uuid),
                    column("tx_time", GrooveColumnType::U64),
                    column("tx_node_id", GrooveColumnType::U64),
                    column("schema_version", GrooveColumnType::U64),
                    column("parents", tx_id_column().array_of()),
                    column("created_by", GrooveColumnType::Uuid),
                    column("created_at", GrooveColumnType::U64),
                    column("updated_by", GrooveColumnType::Uuid),
                    column("updated_at", GrooveColumnType::U64),
                    column("global_time", GrooveColumnType::U64.nullable()),
                    column("_deletion", deletion_column()),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
            ])),
        ]
    }

    /// Return per-layer ahead-of-global candidate tables.
    pub fn ahead_current_storage_tables(&self) -> Vec<GrooveTableSchema> {
        let mut content_columns = vec![
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", GrooveColumnType::Uuid),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", GrooveColumnType::Uuid),
            column("updated_at", GrooveColumnType::U64),
            column("global_time", GrooveColumnType::U64.nullable()),
        ];
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                format!("user_{}", user_column.name),
                user_column.column_type.clone().nullable(),
            )
        }));
        content_columns.push(column(
            "authored_columns",
            GrooveColumnType::Bytes.nullable(),
        ));
        vec![
            GrooveTableSchema::new(format!("jazz_{}_ahead_current", self.name), content_columns)
                .with_primary_key(PrimaryKey::composite([
                    PrimaryKeyColumn::bytes("branch_key"),
                    PrimaryKeyColumn::uuid("row_uuid"),
                    PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                    PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
                ]))
                .with_index(GrooveIndexSchema::new(
                    "by_tx",
                    ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
                )),
            GrooveTableSchema::new(
                format!("jazz_{}_register_ahead_current", self.name),
                [
                    column("branch_key", GrooveColumnType::Bytes),
                    column("row_uuid", GrooveColumnType::Uuid),
                    column("tx_time", GrooveColumnType::U64),
                    column("tx_node_id", GrooveColumnType::U64),
                    column("schema_version", GrooveColumnType::U64),
                    column("parents", tx_id_column().array_of()),
                    column("created_by", GrooveColumnType::Uuid),
                    column("created_at", GrooveColumnType::U64),
                    column("updated_by", GrooveColumnType::Uuid),
                    column("updated_at", GrooveColumnType::U64),
                    column("global_time", GrooveColumnType::U64.nullable()),
                    column("_deletion", deletion_column()),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(GrooveIndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "branch_key", "row_uuid"],
            )),
        ]
    }

    /// Columns available for constrained global-current reads.
    pub fn global_current_indexed_columns(&self) -> BTreeSet<String> {
        self.references
            .keys()
            .cloned()
            .chain(self.indexed_columns.iter().cloned())
            .collect()
    }

    /// Return the wire descriptor for replicated immutable row payloads.
    ///
    /// Wire records contain row payload data and immutable row provenance:
    /// `row_uuid`, `parents`, provenance, `_deletion`, and nullable user cells.
    /// Receiver-local currentness and authority-state columns are deliberately
    /// excluded. Schema changes change this descriptor; v0 requires identical
    /// descriptors at sender and receiver.
    pub fn wire_record_descriptor(&self) -> RecordDescriptor {
        RecordDescriptor::new(
            [
                ("row_uuid".to_owned(), ValueType::Uuid),
                (
                    "parents".to_owned(),
                    ValueType::Array(Box::new(tx_id_column().clone())),
                ),
                ("created_by".to_owned(), ValueType::Uuid),
                ("created_at".to_owned(), ValueType::U64),
                ("updated_by".to_owned(), ValueType::Uuid),
                ("updated_at".to_owned(), ValueType::U64),
                (
                    "_deletion".to_owned(),
                    ValueType::Nullable(Box::new(deletion_column().clone())),
                ),
            ]
            .into_iter()
            .chain(self.columns.iter().map(|column| {
                (
                    format!("user_{}", column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone())),
                )
            })),
        )
    }
}

fn schema_versions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_schema_versions",
        [
            // node-local-derived: allocated by schema-version alias interning.
            column("id", GrooveColumnType::U64),
            column("uuid", GrooveColumnType::Uuid),
            // node-local: mapping from a schema version's logical table & column names to stable
            // physical storage identities. Stored as serialized JSON bytes containing `SchemaPhysicalMapping`:
            // {
            //   tables: {
            //     "todos": {
            //       table_id: 7,
            //       columns: {
            //         "title": 12,
            //         "body": 19
            //       }
            //     }
            //   }
            // }
            column("physical_mapping", GrooveColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn catalogue_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_catalogue",
        [
            column("kind", GrooveColumnType::Bytes),
            column("id", GrooveColumnType::Uuid),
            column("payload", GrooveColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("kind"),
        PrimaryKeyColumn::uuid("id"),
    ]))
}

fn catalogue_pointer_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_catalogue_pointer",
        [
            column("revision", GrooveColumnType::U64),
            column("schema", GrooveColumnType::Uuid),
        ],
    )
    .with_primary_key(PrimaryKey::new("revision", IntegerKeyType::U64).user_supplied())
}

fn global_changes_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_global_changes",
        [
            column("physical_table_id", GrooveColumnType::U64),
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("layer", GrooveColumnType::Bytes),
            column("global_time", GrooveColumnType::U64),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("_deletion", deletion_column().nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::bytes("layer"),
        PrimaryKeyColumn::integer("global_time", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new(
        "by_global_time",
        [
            "global_time",
            "physical_table_id",
            "branch_key",
            "row_uuid",
            "layer",
        ],
    ))
    .with_index(GrooveIndexSchema::new(
        "by_table_global_time",
        [
            "physical_table_id",
            "branch_key",
            "global_time",
            "row_uuid",
            "layer",
        ],
    ))
}

/// Immutable sparse deletion/restore history for every physical table lineage.
///
/// This is intentionally a fixed system table rather than a schema-variant
/// table: deletion payload has no user cells. Branch-key routing is added by
/// the physical incarnation layer rather than transaction metadata.
pub(crate) fn shared_deletion_history_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_deletion_history",
        [
            column("branch_key", GrooveColumnType::Bytes),
            column("physical_table_id", GrooveColumnType::U64),
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", GrooveColumnType::Uuid),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", GrooveColumnType::Uuid),
            column("updated_at", GrooveColumnType::U64),
            column("_deletion", deletion_column()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new(
        "by_tx",
        [
            "tx_time",
            "tx_node_id",
            "branch_key",
            "physical_table_id",
            "row_uuid",
        ],
    ))
}

fn merge_heads_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        MERGE_HEADS_TABLE,
        [
            column("physical_table_id", GrooveColumnType::U64),
            column("branch_key", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("heads", GrooveColumnType::Bytes),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("physical_table_id", IntegerKeyType::U64),
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::uuid("row_uuid"),
    ]))
}

fn pending_edges_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_pending_edges",
        [
            column("child_time", GrooveColumnType::U64),
            column("child_node_id", GrooveColumnType::U64),
            column("parent_time", GrooveColumnType::U64),
            column("parent_node_id", GrooveColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("child_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("child_node_id", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("parent_time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("parent_node_id", IntegerKeyType::U64),
    ]))
}

/// Policy-shape constructors.
#[cfg(test)]
pub(crate) struct Policy;

#[cfg(test)]
impl Policy {
    /// Owner-only policy equivalent to `column == claim("sub")`.
    pub(crate) fn owner_only(table: impl Into<String>, column: impl Into<String>) -> Option<Query> {
        Some(Query::from(table).filter(eq(col(column), claim("sub"))))
    }
}

fn storage_enum(name: &str, variants: &[&str]) -> GrooveColumnType {
    GrooveColumnType::EnumTag(
        ScalarEnumSchema::new(name, variants.iter().copied()).expect("valid enum schema"),
    )
}

fn tx_kind_column() -> GrooveColumnType {
    storage_enum("jazz_tx_kind", &["mergeable", "exclusive"])
}

fn fate_column() -> GrooveColumnType {
    storage_enum("jazz_fate", &["pending", "accepted", "rejected"])
}

fn deletion_column() -> GrooveColumnType {
    GrooveColumnType::EnumTag(
        ScalarEnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum")
            .with_system_registry(SystemVariantRegistry::deletion_state()),
    )
}

fn rejection_reason_column() -> GrooveColumnType {
    storage_enum(
        "jazz_rejection_reason",
        &[
            "client_clock_too_far_ahead",
            "authorization_denied",
            "exclusive_conflict",
            "causality_violation",
            "cascade",
            "malformed_commit",
        ],
    )
}

fn durability_column() -> GrooveColumnType {
    storage_enum("jazz_durability", &["none", "local", "edge", "global"])
}

fn tx_id_column() -> GrooveColumnType {
    GrooveColumnType::Tuple(vec![GrooveColumnType::U64, GrooveColumnType::Uuid])
}

fn column(name: impl Into<String>, column_type: GrooveColumnType) -> groove::schema::ColumnSchema {
    groove::schema::ColumnSchema::new(name, column_type)
}

pub(crate) fn global_current_index_name(column: &str) -> String {
    format!("by_user_{column}")
}

fn nodes_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_nodes",
        [
            // node-local-derived: allocated by node alias interning.
            column("id", GrooveColumnType::U64),
            column("uuid", GrooveColumnType::Uuid),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
}

fn transactions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_transactions",
        [
            column("time", GrooveColumnType::U64),
            column("node_id", GrooveColumnType::U64),
            column("kind", tx_kind_column()),
            column("n_total_writes", GrooveColumnType::U32),
            column("made_by", GrooveColumnType::Uuid),
            column("base_snapshot", GrooveColumnType::Bytes.nullable()),
            column("row_read_set", GrooveColumnType::Bytes.nullable()),
            column("absent_read_set", GrooveColumnType::Bytes.nullable()),
            column("predicate_read_set", GrooveColumnType::Bytes.nullable()),
            column("user_metadata", GrooveColumnType::String.nullable()),
            column("contribution_merge", GrooveColumnType::Bytes.nullable()),
            column("permission_subject", GrooveColumnType::Uuid.nullable()),
            column("merge_strategy", GrooveColumnType::String.nullable()),
            // upstream-decided: written only by fate/state application.
            column("fate", fate_column()),
            // upstream-decided: written only by fate/state application.
            column("global_time", GrooveColumnType::U64.nullable()),
            // upstream-decided: written only by rejection/state application.
            column("rejection_reason", rejection_reason_column().nullable()),
            // upstream-decided: written only by rejection/state application.
            column("cascade_root", tx_id_column().nullable()),
            // upstream-decided: written only by rejection/state application.
            column("reason_detail", GrooveColumnType::String.nullable()),
            // node-local-derived: updated when the node learns stronger durability.
            column("durability", durability_column()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new("by_global_time", ["global_time"]))
}

fn canonical_schema_bytes(schema: &RuntimeSchema) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, "jazz-schema-v1-branch-views");
    put_u64(&mut bytes, schema.branch_dimensions.len() as u64);
    for dimension in &schema.branch_dimensions {
        bytes.extend_from_slice(dimension.id.0.as_bytes());
        put_str(&mut bytes, &dimension.name);
        put_column_type(&mut bytes, &dimension.column_type);
        let default = postcard::to_allocvec(&dimension.migration_default)
            .expect("branch dimension defaults are encodable");
        put_u64(&mut bytes, default.len() as u64);
        bytes.extend(default);
    }
    let mut tables = schema.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    put_u64(&mut bytes, tables.len() as u64);
    for table in tables {
        put_str(&mut bytes, &table.name);
        put_u64(&mut bytes, table.columns.len() as u64);
        for column in &table.columns {
            put_str(&mut bytes, &column.name);
            put_column_type(&mut bytes, &column.column_type);
            put_merge_strategy(&mut bytes, table.merge_strategy(&column.name));
        }
        put_u64(&mut bytes, table.references.len() as u64);
        for (column, target) in &table.references {
            put_str(&mut bytes, column);
            put_str(&mut bytes, target);
        }
        let mut branch_by = table.branch_by.iter().collect::<Vec<_>>();
        branch_by.sort_by_key(|binding| binding.dimension);
        put_u64(&mut bytes, branch_by.len() as u64);
        for binding in branch_by {
            bytes.extend_from_slice(binding.dimension.0.as_bytes());
            put_str(&mut bytes, &binding.column);
        }
    }
    bytes
}

fn put_merge_strategy(bytes: &mut Vec<u8>, strategy: MergeStrategy) {
    bytes.push(match strategy {
        MergeStrategy::Lww => 0,
        MergeStrategy::Counter => 1,
        MergeStrategy::GSet => 2,
    });
}

fn put_column_type(bytes: &mut Vec<u8>, column_type: &GrooveColumnType) {
    match column_type {
        GrooveColumnType::U8 => bytes.push(0),
        GrooveColumnType::U16 => bytes.push(1),
        GrooveColumnType::U32 => bytes.push(2),
        GrooveColumnType::U64 => bytes.push(3),
        GrooveColumnType::I32 => bytes.push(4),
        GrooveColumnType::I64 => bytes.push(5),
        GrooveColumnType::F64 => bytes.push(6),
        GrooveColumnType::Bool => bytes.push(7),
        GrooveColumnType::String => bytes.push(8),
        GrooveColumnType::Bytes => bytes.push(9),
        GrooveColumnType::Uuid => bytes.push(10),
        GrooveColumnType::EnumTag(schema) => {
            bytes.push(11);
            put_str(bytes, &schema.name);
            put_u64(bytes, schema.variants.len() as u64);
            for variant in &schema.variants {
                put_str(bytes, variant);
            }
        }
        GrooveColumnType::Tuple(members) => {
            bytes.push(12);
            put_u64(bytes, members.len() as u64);
            for member in members {
                put_column_type(bytes, member);
            }
        }
        GrooveColumnType::Array(member) => {
            bytes.push(13);
            put_column_type(bytes, member);
        }
        GrooveColumnType::Nullable(member) => {
            bytes.push(14);
            put_column_type(bytes, member);
        }
        GrooveColumnType::Record(descriptor) => {
            bytes.push(15);
            put_u64(bytes, descriptor.fields().len() as u64);
            for field in descriptor.fields() {
                match &field.name {
                    Some(name) => {
                        bytes.push(1);
                        put_str(bytes, name);
                    }
                    None => bytes.push(0),
                }
                put_column_type(bytes, &field.value_type);
            }
        }
        GrooveColumnType::Enum(schema) => {
            bytes.push(16);
            put_str(bytes, &schema.name);
            put_u64(bytes, schema.cases.len() as u64);
            for case in &schema.cases {
                put_str(bytes, &case.name);
                put_u64(bytes, case.payload.fields().len() as u64);
                for field in case.payload.fields() {
                    match &field.name {
                        Some(name) => {
                            bytes.push(1);
                            put_str(bytes, name);
                        }
                        None => bytes.push(0),
                    }
                    put_column_type(bytes, &field.value_type);
                }
            }
        }
    }
}

fn put_str(bytes: &mut Vec<u8>, value: &str) {
    put_bytes(bytes, value.as_bytes());
}

fn put_bytes(bytes: &mut Vec<u8>, value: &[u8]) {
    put_u64(bytes, value.len() as u64);
    bytes.extend_from_slice(value);
}

fn put_u64(bytes: &mut Vec<u8>, value: u64) {
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn rejected_transactions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_rejected_transactions",
        [
            column("time", GrooveColumnType::U64),
            column("node_id", GrooveColumnType::U64),
            column("kind", tx_kind_column()),
            column("made_by", GrooveColumnType::Uuid),
            column("rejection_reason", rejection_reason_column()),
            column("cascade_root", tx_id_column().nullable()),
            column("reason_detail", GrooveColumnType::String.nullable()),
            column("user_metadata", GrooveColumnType::String.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
    ]))
}

#[cfg(test)]
mod tests {
    use super::*;
    use groove::schema::ColumnType;

    #[test]
    #[should_panic(expected = "branch dimensions require UUID")]
    fn variable_width_branch_dimensions_are_rejected() {
        let dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x30; 16]));
        let _ = JazzSchema::new_with_branch_dimensions(
            [BranchDimensionSchema::new(
                dimension,
                "branch",
                ColumnType::String,
                Value::String("main".to_owned()),
            )],
            [
                TableSchema::new("todos", [ColumnSchema::new("branch", ColumnType::String)])
                    .with_branch_dimension("branch", dimension),
            ],
        );
    }

    #[test]
    fn added_branch_dimension_reads_older_keys_at_its_default() {
        let dimension = BranchDimensionId(uuid::Uuid::from_bytes([0x31; 16]));
        let schema = JazzSchema::new_with_branch_dimensions(
            [BranchDimensionSchema::new(
                dimension,
                "workspace",
                ColumnType::Uuid,
                Value::Uuid(uuid::Uuid::nil()),
            )],
            [TableSchema::new(
                "todos",
                [ColumnSchema::new("workspace_id", ColumnType::Uuid)],
            )
            .with_branch_dimension("workspace_id", dimension)],
        );
        let table = &schema.tables[0];
        let default = schema
            .project_branch_selector(
                table,
                &BranchSelector::new([("workspace", Value::Uuid(uuid::Uuid::nil()))]),
            )
            .unwrap()
            .0;
        let other = schema
            .project_branch_selector(
                table,
                &BranchSelector::new([(
                    "workspace",
                    Value::Uuid(uuid::Uuid::from_bytes([0x32; 16])),
                )]),
            )
            .unwrap()
            .0;

        assert!(schema.branch_key_matches(table, &BranchKey::default(), &default));
        assert!(!schema.branch_key_matches(table, &BranchKey::default(), &other));
    }

    #[test]
    fn logical_history_descriptor_has_composite_primary_key() {
        let schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);

        let groove = schema.lower_to_groove();
        assert!(groove.table("jazz_nodes").is_some());
        assert!(groove.table("jazz_schema_versions").is_some());
        assert!(groove.table("jazz_transactions").is_some());
        assert!(groove.table("jazz_todos_history").is_none());
        let table = schema.tables[0].history_storage_table();
        let primary_key = table.primary_key.as_ref().unwrap();

        assert_eq!(primary_key.columns.len(), 4);
        assert_eq!(primary_key.columns[0].column, "branch_key");
        assert_eq!(primary_key.columns[1].column, "row_uuid");
        assert_eq!(primary_key.columns[2].column, "tx_time");
        assert_eq!(primary_key.columns[3].column, "tx_node_id");
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.name == "user_title")
        );
        assert!(
            table
                .columns
                .iter()
                .any(|column| column.name == "schema_version")
        );
    }

    #[test]
    fn schema_version_id_is_stable_and_content_addressed() {
        let schema_a = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let schema_b = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let schema_c = RuntimeSchema::new([TableSchema::new(
            "todos",
            [
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("done", ColumnType::Bool),
            ],
        )]);

        assert_eq!(schema_a.version_id(), schema_b.version_id());
        assert_eq!(schema_a.canonical_bytes(), schema_b.canonical_bytes());
        assert_ne!(schema_a.version_id(), schema_c.version_id());
    }

    #[test]
    fn counter_merge_strategy_changes_schema_identity() {
        let lww = RuntimeSchema::new([TableSchema::new(
            "counters",
            [ColumnSchema::new("count", ColumnType::U64)],
        )]);
        let counter = RuntimeSchema::new([TableSchema::new(
            "counters",
            [ColumnSchema::new("count", ColumnType::U64)],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)]);

        assert_ne!(lww.version_id(), counter.version_id());
    }

    #[test]
    #[should_panic(expected = "counter merge strategy requires a non-nullable integer column")]
    fn counter_merge_strategy_rejects_string_columns() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_column_merge_strategy("title", MergeStrategy::Counter)]);
    }

    #[test]
    #[should_panic(expected = "merge strategy declared for unknown column todos.missing")]
    fn merge_strategy_rejects_unknown_user_column() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_column_merge_strategy("missing", MergeStrategy::Lww)]);
    }

    #[test]
    #[should_panic(expected = "counter merge strategy requires a non-nullable integer column")]
    fn counter_merge_strategy_rejects_nullable_integer_columns() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("count", ColumnType::U64.nullable())],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)]);
    }

    #[test]
    #[should_panic(expected = "read policy table must match")]
    fn read_policy_must_name_attached_table() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_read_policy(Policy::owner_only("other", "owner"))]);
    }

    #[test]
    #[should_panic(expected = "write policy table must match")]
    fn write_policy_must_name_attached_table() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_write_policy(Policy::owner_only("other", "owner"))]);
    }

    #[test]
    #[should_panic(expected = "valid read policy shape")]
    fn read_policy_validates_against_complete_schema() {
        RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_read_policy(Policy::owner_only("todos", "missing"))]);
    }

    #[test]
    fn global_changes_table_key_and_index_match_sync_contract() {
        let table = global_changes_table();
        let primary_key = table.primary_key.as_ref().unwrap();
        assert_eq!(table.name, "jazz_global_changes");
        assert_eq!(
            primary_key
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec![
                "physical_table_id",
                "branch_key",
                "row_uuid",
                "layer",
                "global_time"
            ]
        );

        let index = table
            .indices
            .iter()
            .find(|index| index.name == "by_global_time")
            .unwrap();
        assert_eq!(
            index.columns,
            vec![
                "global_time",
                "physical_table_id",
                "branch_key",
                "row_uuid",
                "layer"
            ]
        );
        let table_index = table
            .indices
            .iter()
            .find(|index| index.name == "by_table_global_time")
            .unwrap();
        assert_eq!(
            table_index.columns,
            vec![
                "physical_table_id",
                "branch_key",
                "global_time",
                "row_uuid",
                "layer"
            ]
        );
    }

    // This is intentionally an internal schema test: physical-key boundedness
    // is not observable through the public API until deletion ingestion routes
    // through the shared table. It guards the storage contract that makes the
    // later black-box collision and branch-isolation tests meaningful.
    #[test]
    fn shared_deletion_history_is_prefix_bounded_by_lineage_table_and_row() {
        let table = shared_deletion_history_table();
        assert_eq!(table.name, "jazz_deletion_history");
        assert_eq!(
            table
                .primary_key
                .as_ref()
                .expect("shared deletion history has a primary key")
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec![
                "branch_key",
                "physical_table_id",
                "row_uuid",
                "tx_time",
                "tx_node_id",
            ]
        );
        assert_eq!(
            table
                .indices
                .iter()
                .find(|index| index.name == "by_tx")
                .expect("shared deletion history has tx lookup")
                .columns,
            vec![
                "tx_time",
                "tx_node_id",
                "branch_key",
                "physical_table_id",
                "row_uuid",
            ]
        );
    }

    #[test]
    fn storage_lowering_declares_system_columns_by_shape() {
        let schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let tables = schema.storage_tables();
        let transactions = tables
            .iter()
            .find(|table| table.name == "jazz_transactions")
            .unwrap();
        assert!(
            tables
                .iter()
                .all(|table| table.name != "jazz_todos_history"
                    && table.name != "jazz_todos_register")
        );
        assert!(
            tables
                .iter()
                .any(|table| table.name == "jazz_deletion_history")
        );
        let history = schema.tables[0].history_storage_table();
        let register = schema.tables[0].register_storage_table();
        assert!(tables.iter().all(|table| {
            table.name != "jazz_todos_global_current"
                && table.name != "jazz_todos_register_global_current"
                && table.name != "jazz_todos_ahead_current"
                && table.name != "jazz_todos_register_ahead_current"
        }));
        let current_tables = schema.tables[0].global_current_storage_tables();
        let global_current = &current_tables[0];
        let register_global_current = &current_tables[1];

        assert!(
            transactions
                .columns
                .iter()
                .any(|column| column.name == "fate")
        );
        assert!(
            transactions
                .columns
                .iter()
                .any(|column| column.name == "durability")
        );
        assert!(
            history
                .columns
                .iter()
                .any(|column| column.name == "parents")
        );
        assert!(
            register
                .columns
                .iter()
                .any(|column| column.name == "_deletion")
        );
        assert_eq!(
            global_current
                .primary_key
                .as_ref()
                .unwrap()
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec!["branch_key", "row_uuid"]
        );
        assert_eq!(
            register_global_current
                .primary_key
                .as_ref()
                .unwrap()
                .columns
                .iter()
                .map(|column| column.column.as_str())
                .collect::<Vec<_>>(),
            vec!["branch_key", "row_uuid"]
        );
    }

    #[test]
    fn system_deletion_registry_survives_storage_rebinding_but_user_enums_do_not() {
        let state = ScalarEnumSchema::new("state", ["open", "done"]).unwrap();
        let left = TableSchema::new(
            "left",
            [ColumnSchema::new(
                "state",
                ColumnType::EnumTag(state.clone()),
            )],
        );
        let right = TableSchema::new(
            "right",
            [ColumnSchema::new("state", ColumnType::EnumTag(state))],
        );
        let registry = |table: GrooveTableSchema, name: &str| match &table
            .columns
            .iter()
            .find(|column| column.name == name)
            .unwrap()
            .column_type
        {
            GrooveColumnType::EnumTag(schema) => schema.registry_id(),
            GrooveColumnType::Nullable(inner) => match inner.as_ref() {
                GrooveColumnType::EnumTag(schema) => schema.registry_id(),
                other => panic!("expected enum field, got {other:?}"),
            },
            other => panic!("expected enum field, got {other:?}"),
        };

        assert_eq!(
            registry(left.register_storage_table(), "_deletion"),
            registry(right.register_storage_table(), "_deletion")
        );
        assert_ne!(
            registry(left.history_storage_table(), "user_state"),
            registry(right.history_storage_table(), "user_state"),
            "structurally identical user enums must retain independent registries"
        );
    }
}
