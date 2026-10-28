//! Jazz schema metadata: table/column declarations, merge strategies, policy
//! metadata, storage-table naming and migration-lens schema surfaces
//! (`jazz/SPEC/10_lenses_migrations.md`). Query shapes are in
//! [`crate::query`]; `jazz-db` turns the schema into SQLite tables.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::Deref;

use jazz_types::column_schema::{
    ColumnType as StorageColumnType, IndexSchema as StorageIndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, TableSchema as StorageTableSchema,
};
use jazz_types::records::{
    EnumCase, EnumSchema, RecordDescriptor, ScalarEnumSchema, SystemVariantRegistry, Value,
    ValueType,
};

use crate::ids::SchemaVersionId;
use crate::model::branch::{BranchColumnValue, BranchKey, BranchSelector};
use crate::model::public_schema::Schema as PublicSchema;
use crate::query::Query;
#[cfg(test)]
use crate::query::{claim, col, eq};

pub use crate::model::public_schema_convert::SchemaConversionError;

/// Query-only descriptors for virtual history relations.
pub mod history;
/// Query-only public computed columns.
pub mod magic;

/// Namespace used for schema-version UUIDv5 ids.
pub const SCHEMA_VERSION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("61b9ef21-3195-50e8-87fc-2aa83a6f74e3");

/// Collision-checked directory from a fixed policy-binding digest to its full
/// authority identity. Result-store keys remain bounded without reducing the
/// runtime policy boundary to a hash-only identity.
pub const AUTHORITY_POLICY_BINDINGS_STORE: &str = "jazz_authority_policy_bindings";
/// Per-subscription watermark ("Q at W") v1: the settled seq and supporting
/// revision of a row-local view, so a reopened receiver can ask Core for only
/// the rows that moved. The held set is rebuilt from synced local rows.
pub const SUBSCRIPTION_WATERMARKS_STORE: &str = "jazz_subscription_watermarks_v1";
/// Versioned local current-row availability and ordering receipts.
pub const LOCAL_ROW_AVAILABILITY_STORE: &str = "jazz_local_row_availability_v1";
/// The dictionary that every stored author column is interned into:
/// transaction records' `made_by` and the row images'
/// `created_by`/`updated_by` store a node-local code, while
/// every read, query, policy and the wire see the full [`RowAuthor`].
///
/// [`RowAuthor`]: crate::ids::RowAuthor
pub const AUTHORS_DICTIONARY: &str = "authors";

/// The stored type of an author column: a [`RowAuthor`] interned into
/// [`AUTHORS_DICTIONARY`].
///
/// [`RowAuthor`]: crate::ids::RowAuthor
pub fn interned_author_type() -> ValueType {
    ValueType::interned(AUTHORS_DICTIONARY, crate::ids::RowAuthor::value_type())
}

/// The fate authority's superseded-writes markers (SPEC 4 §4.6): per row
/// (keyed by physical table id, which a table rename keeps) and writer node,
/// the newest transaction time of that node's writes to the row that Core
/// passed over when it decided a later write of the node chained on a
/// predecessor it holds no decision for (unknown, or stored Pending). A write
/// of that node at or before the time arriving afterwards is late
/// (`INV-HIST-23`).
pub const SUPERSEDED_WRITES_STORE: &str = "jazz_superseded_writes_v1";
/// Source-backed Jazz schema accepted by public database APIs.
///
/// The developer-authored source is retained for durable catalogue
/// publication while [`RuntimeSchema`] is the derived, in-memory engine form.
#[derive(Clone, Debug)]
pub struct JazzSchema {
    public_schema: PublicSchema,
    runtime: RuntimeSchema,
    /// Conservative, schema-owned dispatch metadata; never a policy decision.
    has_authorized_created_insert_sources: bool,
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
        crate::model::public_schema_convert::convert_public_schema(schema)
    }

    /// Structural catalogue payload, independent of the active permissions.
    pub fn without_permissions(&self) -> Self {
        let mut schema = self.clone();
        for table in schema.public_schema.values_mut() {
            table.policies = Default::default();
        }
        for table in &mut schema.runtime.tables {
            table.read_policy = None;
            table.write_policies = Default::default();
        }
        schema.has_authorized_created_insert_sources = false;
        schema
    }

    /// Original lineage records may retain defaults, indexes and grants that a
    /// later deployment refreshed without changing the runtime version.
    #[doc(hidden)]
    pub fn has_same_lineage_structure(&self, other: &Self) -> bool {
        let normalize = |schema: &Self| {
            let mut runtime = schema.runtime.clone();
            for table in &mut runtime.tables {
                table.read_policy = None;
                table.write_policies = Default::default();
                table.indexed_columns.clear();
                for column in &mut table.columns {
                    if !table.branch_by.contains(&column.name) {
                        column.default = None;
                    }
                }
            }
            runtime
        };
        normalize(self) == normalize(other)
    }

    /// Refresh non-identity metadata while retaining the selected grants.
    pub fn with_permissions_from(&self, selected: &Self) -> Self {
        assert_eq!(self.version_id(), selected.version_id());
        let mut schema = self.clone();
        for (name, table) in &mut schema.public_schema {
            table.policies = selected
                .public_schema
                .get(name)
                .expect("same schema tables")
                .policies
                .clone();
        }
        for table in &mut schema.runtime.tables {
            let selected = selected
                .runtime
                .tables
                .iter()
                .find(|other| other.name == table.name)
                .expect("same schema tables");
            table.read_policy = selected.read_policy.clone();
            table.write_policies = selected.write_policies.clone();
        }
        schema.has_authorized_created_insert_sources =
            selected.has_authorized_created_insert_sources;
        schema
    }

    /// Return the developer-authored public schema retained for persistence.
    pub fn public_schema(&self) -> &PublicSchema {
        &self.public_schema
    }

    /// Return the compiled application tables used by the runtime.
    pub fn tables(&self) -> &[TableSchema] {
        &self.runtime.tables
    }

    #[doc(hidden)]
    pub fn from_runtime(public_schema: PublicSchema, runtime: RuntimeSchema) -> Self {
        let has_authorized_created_insert_sources = runtime.tables.iter().any(|table| {
            table
                .write_policies
                .insert_check
                .as_ref()
                .is_some_and(Query::uses_authorized_created_sources)
        });
        Self {
            public_schema,
            runtime,
            has_authorized_created_insert_sources,
        }
    }

    #[doc(hidden)]
    pub fn may_have_authorized_created_insert_sources(&self) -> bool {
        self.has_authorized_created_insert_sources
    }

    #[doc(hidden)]
    pub fn runtime(&self) -> &RuntimeSchema {
        &self.runtime
    }

    #[cfg(any(test, feature = "testing"))]
    #[doc(hidden)]
    pub fn runtime_mut_for_testing(&mut self) -> &mut RuntimeSchema {
        // Tests may replace a policy through this mutable escape hatch. A
        // conservative positive only requests exact operation selection.
        self.has_authorized_created_insert_sources = true;
        &mut self.runtime
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub fn into_runtime(self) -> RuntimeSchema {
        self.runtime
    }

    /// Construct a compiled schema for internal engine tests whose tables
    /// already declare their branch columns.
    #[cfg(any(test, feature = "testing"))]
    pub fn new_with_branch_columns(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self::from_runtime(PublicSchema::new(), RuntimeSchema::new(tables))
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
    /// Application tables in the schema.
    pub tables: Vec<TableSchema>,
}

impl PartialEq for RuntimeSchema {
    fn eq(&self, other: &Self) -> bool {
        self.tables == other.tables
    }
}

impl RuntimeSchema {
    /// Construct a compiled schema from already-lowered application tables.
    #[cfg(any(test, feature = "testing"))]
    pub fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
        }
        .validated()
    }

    /// Project a named selector onto one table and validate its exact branch key.
    pub fn project_branch_selector(
        &self,
        table: &TableSchema,
        selector: &BranchSelector,
    ) -> Result<(BranchKey, BTreeMap<String, Value>), String> {
        if selector.values.len() != table.branch_by.len() {
            return Err(format!(
                "branch selector for {} must provide exactly {} values",
                table.name,
                table.branch_by.len()
            ));
        }
        let mut values = Vec::with_capacity(table.branch_by.len());
        let mut cells = BTreeMap::new();
        for column_name in &table.branch_by {
            let column = table
                .columns
                .iter()
                .find(|column| &column.name == column_name)
                .ok_or_else(|| {
                    format!("unknown branch column {column_name:?} on {}", table.name)
                })?;
            let encoded = selector
                .values
                .get(column_name)
                .ok_or_else(|| format!("missing branch column {column_name}"))?;
            let value = encoded
                .decode()
                .map_err(|_| format!("invalid branch column {column_name} encoding"))?;
            RecordDescriptor::new([("value", column.column_type.clone())])
                .create(std::slice::from_ref(&value))
                .map_err(|_| format!("invalid branch column {column_name} value"))?;
            let encoded = BranchColumnValue::encode_typed(&value, &column.column_type)
                .map_err(|_| format!("invalid branch column {column_name} value"))?;
            values.push((column_name.clone(), encoded));
            cells.insert(column_name.clone(), value);
        }
        values.sort_by(|left, right| left.0.cmp(&right.0));
        Ok((BranchKey { values }, cells))
    }

    /// Validate a schema-wide view selector, then project it onto one table's subset.
    pub fn project_branch_view_selector(
        &self,
        table: &TableSchema,
        selector: &BranchSelector,
    ) -> Result<(BranchKey, BTreeMap<String, Value>), String> {
        let schema_branch_columns = self
            .tables
            .iter()
            .flat_map(|table| table.branch_by.iter().cloned())
            .collect::<BTreeSet<_>>();
        if selector.values.keys().cloned().collect::<BTreeSet<_>>() != schema_branch_columns {
            return Err(
                "branch view selector must provide every schema branch column exactly once"
                    .to_owned(),
            );
        }
        let projected = BranchSelector {
            values: table
                .branch_by
                .iter()
                .map(|column| (column.clone(), selector.values[column].clone()))
                .collect(),
        };
        self.project_branch_selector(table, &projected)
    }

    /// Validate an authored table-local branch key without applying migration
    /// defaults. Wire versions must carry the exact key declared by their own
    /// schema: accepting a short or non-canonical spelling here would let a
    /// peer change the physical branch coordinate independently of row cells.
    pub fn validate_authored_branch_key(
        &self,
        table: &TableSchema,
        key: &BranchKey,
    ) -> Result<BTreeMap<String, Value>, String> {
        Self::validate_branch_key_for_table(table, key)
    }

    /// Decode one persisted branch coordinate using the table that owns it.
    ///
    /// The binary envelope is only self-describing enough to reject malformed
    /// bytes. The table's branch declaration is the authority for component
    /// names, scalar types, and stable-enum domains, so recovery must perform
    /// this second validation before a physical coordinate can influence a
    /// query or cache.
    pub fn decode_persisted_branch_key(
        table: &TableSchema,
        bytes: &[u8],
    ) -> Result<BranchKey, String> {
        let key = BranchKey::from_canonical_bytes(bytes)
            .map_err(|_| format!("invalid persisted branch key for {}", table.name))?;
        Self::validate_branch_key_for_table(table, &key)?;
        Ok(key)
    }

    /// Validate an exact branch coordinate against its owning table
    /// declaration. Shared by admission and persisted-state recovery so their
    /// type/domain rules cannot drift.
    pub fn validate_branch_key_for_table(
        table: &TableSchema,
        key: &BranchKey,
    ) -> Result<BTreeMap<String, Value>, String> {
        let mut bindings = table.branch_by.iter().collect::<Vec<_>>();
        bindings.sort();
        if key.values.len() != bindings.len() {
            return Err(format!(
                "branch key for {} must contain exactly {} values",
                table.name,
                bindings.len()
            ));
        }

        let mut cells = BTreeMap::new();
        for (column_name, (actual_name, encoded)) in bindings.into_iter().zip(&key.values) {
            if actual_name != column_name {
                return Err(format!(
                    "branch key for {} is not in canonical column order",
                    table.name
                ));
            }
            let column = table
                .columns
                .iter()
                .find(|column| column.name == *column_name)
                .ok_or_else(|| format!("unknown branch column on {}", table.name))?;
            let value = encoded
                .decode_as(&column.column_type)
                .map_err(|_| format!("invalid branch column {column_name} encoding"))?;
            if BranchColumnValue::encode_typed(&value, &column.column_type)
                .map_err(|_| format!("invalid branch column {column_name} value"))?
                != *encoded
            {
                return Err(format!(
                    "non-canonical branch column {column_name} encoding"
                ));
            }
            RecordDescriptor::new([("value", column.column_type.clone())])
                .create(std::slice::from_ref(&value))
                .map_err(|_| format!("invalid branch column {column_name} value"))?;
            cells.insert(column_name.clone(), value);
        }
        Ok(cells)
    }

    #[cfg(any(test, feature = "testing"))]
    fn validated(self) -> Self {
        let mut branch_column_types = BTreeMap::new();
        for table in &self.tables {
            assert!(
                table.columns.iter().all(|column| column.name != "id"),
                "column name \"id\" is reserved for the automatically generated UUID row ID"
            );
            let mut bound_columns = BTreeSet::new();
            for column_name in &table.branch_by {
                let column = table
                    .columns
                    .iter()
                    .find(|column| column.name == *column_name)
                    .unwrap_or_else(|| {
                        panic!("unknown branch column {}.{column_name}", table.name)
                    });
                assert!(
                    bound_columns.insert(column_name),
                    "duplicate table branch column"
                );
                assert!(
                    !matches!(column.column_type, StorageColumnType::Nullable(_)),
                    "branch columns must be non-nullable"
                );
                assert!(
                    matches!(
                        column.column_type,
                        StorageColumnType::String
                            | StorageColumnType::Uuid
                            | StorageColumnType::U8
                            | StorageColumnType::U16
                            | StorageColumnType::U32
                            | StorageColumnType::U64
                            | StorageColumnType::I32
                            | StorageColumnType::I64
                            | StorageColumnType::EnumTag(_)
                    ),
                    "branch columns require string, UUID, stable enum, or fixed-width integer values"
                );
                if let Some(existing) =
                    branch_column_types.insert(column_name.clone(), column.column_type.clone())
                {
                    assert_eq!(
                        existing, column.column_type,
                        "same-named branch columns must have the same type"
                    );
                }
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
                    MergeStrategy::Text | MergeStrategy::Markdown | MergeStrategy::Html => {
                        assert!(
                            matches!(column.column_type, StorageColumnType::String),
                            "text merge strategies require a non-nullable string column: {}.{}",
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
    /// Concurrent edits to a string are merged three-way against the base
    /// each write was made over (plain text).
    Text,
    /// As [`Self::Text`], with markdown syntax kept whole.
    Markdown,
    /// As [`Self::Text`], with HTML tags and entities kept whole.
    Html,
}

impl MergeStrategy {
    /// The text format a text-merging column declares, if any.
    pub fn text_format(self) -> Option<TextFormat> {
        match self {
            Self::Text => Some(TextFormat::Plain),
            Self::Markdown => Some(TextFormat::Markdown),
            Self::Html => Some(TextFormat::Html),
            Self::Lww | Self::Counter | Self::GSet => None,
        }
    }
}

/// How a text-merging column's values are cut into the units a merge never
/// splits.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TextFormat {
    /// Words, whitespace and other characters.
    Plain,
    /// Plain units, plus delimiter runs, list markers and link targets.
    Markdown,
    /// Plain units, plus tags and entities.
    Html,
}

#[cfg(any(test, feature = "testing"))]
fn is_counter_column_type(column_type: &StorageColumnType) -> bool {
    matches!(
        column_type,
        StorageColumnType::U8
            | StorageColumnType::U16
            | StorageColumnType::U32
            | StorageColumnType::U64
            | StorageColumnType::I32
            | StorageColumnType::I64
    )
}

#[cfg(any(test, feature = "testing"))]
fn is_gset_column_type(column_type: &StorageColumnType) -> bool {
    matches!(column_type, StorageColumnType::Array(_))
}

/// Semantics declared for a built-in column transform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ColumnTransformSemantics {
    /// The transform has a total inverse over its declared value domain.
    pub bijective: bool,
    /// Equal canonical source values remain equal after transform and inverse.
    pub canonical_equality_preserving: bool,
}

/// Return the semantics for a registered built-in column transform.
pub fn registered_column_transform(key: &str) -> Option<ColumnTransformSemantics> {
    match key {
        "identity" | "jazz.identity" => Some(ColumnTransformSemantics {
            bijective: true,
            canonical_equality_preserving: true,
        }),
        _ => None,
    }
}

/// Internal schema-derived physical interpretation for a scalar column.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LargeValueSemanticKind {
    /// The column never uses the hidden large-scalar representation.
    NotLarge,
    /// Byte scalar semantics.
    Bytes,
    /// UTF-8 text scalar semantics.
    String,
    /// Canonical JSON-in-string scalar semantics.
    Json,
}

/// Application column declaration.
#[derive(Clone, Debug, PartialEq, serde::Serialize)]
pub struct ColumnSchema {
    /// Logical column name.
    pub name: String,
    /// Storage type used for this column's cell value.
    pub column_type: StorageColumnType,
    /// Immutable, schema-derived semantic kind used only by the hidden
    /// physical large-scalar envelope. JSON remains logically string-shaped in
    /// storage, but cannot be decoded or staged as text at this boundary.
    pub large_value_kind: LargeValueSemanticKind,
    /// Literal value used when an insert omits this column.
    #[serde(default)]
    pub default: Option<Value>,
}

#[derive(serde::Deserialize)]
struct SerializedColumnSchema {
    name: String,
    column_type: StorageColumnType,
    #[serde(default)]
    large_value_kind: Option<LargeValueSemanticKind>,
    #[serde(default)]
    default: Option<Value>,
}

impl<'de> serde::Deserialize<'de> for ColumnSchema {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let serialized = SerializedColumnSchema::deserialize(deserializer)?;
        if contains_internal_storage_type(&serialized.column_type) {
            return Err(serde::de::Error::custom(
                "internal storage types cannot appear in a Jazz schema",
            ));
        }
        let derived = large_value_kind_for_type(&serialized.column_type);
        let large_value_kind = serialized.large_value_kind.unwrap_or(derived);
        let kind_is_valid = large_value_kind == derived
            || (large_value_kind == LargeValueSemanticKind::Json
                && derived == LargeValueSemanticKind::String);
        if !kind_is_valid {
            return Err(serde::de::Error::custom(
                "large-value semantic kind does not match the declared column type",
            ));
        }
        Ok(Self {
            name: serialized.name,
            column_type: serialized.column_type,
            large_value_kind,
            default: serialized.default,
        })
    }
}

impl ColumnSchema {
    /// Logical column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Public logical storage type. Physical large-scalar backing types remain sealed.
    pub fn column_type(&self) -> &StorageColumnType {
        &self.column_type
    }

    /// Construct an ordinary column from a storage type.
    pub fn new(name: impl Into<String>, column_type: StorageColumnType) -> Self {
        assert!(
            !contains_internal_storage_type(&column_type),
            "raw and stored-scalar value types are physical-only and cannot be declared in a Jazz schema"
        );
        Self {
            name: name.into(),
            large_value_kind: large_value_kind_for_type(&column_type),
            column_type,
            default: None,
        }
    }

    /// Attach a literal insert default to this column.
    pub fn with_default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    /// Whether this is a nullable JSON column. Its published storage type is
    /// the bare `StoredScalar(Json)`: one nullable slot records presence, and
    /// there is no separate slot for the column's own nullability.
    pub fn is_nullable_json(&self) -> bool {
        self.large_value_kind == LargeValueSemanticKind::Json
            && matches!(self.column_type, StorageColumnType::Nullable(_))
    }

    /// Lower a newly authored logical cell to its storage value without
    /// changing the published JSON descriptor. A present nullable JSON value
    /// is stored bare; SQL NULL is left to the caller's validation (#2733).
    pub fn storage_value(&self, value: Value) -> Value {
        match value {
            Value::Nullable(Some(value)) if self.is_nullable_json() => *value,
            value => value,
        }
    }
}

fn contains_internal_storage_type(column_type: &StorageColumnType) -> bool {
    if column_type.is_internal_storage_type() {
        return true;
    }
    match column_type {
        StorageColumnType::Nullable(inner) | StorageColumnType::Array(inner) => {
            contains_internal_storage_type(inner)
        }
        StorageColumnType::Tuple(members) => members.iter().any(contains_internal_storage_type),
        StorageColumnType::Record(descriptor) => descriptor
            .fields()
            .iter()
            .any(|field| contains_internal_storage_type(&field.value_type)),
        StorageColumnType::Enum(schema) => schema.cases.iter().any(|case| {
            case.payload
                .fields()
                .iter()
                .any(|field| contains_internal_storage_type(&field.value_type))
        }),
        _ => false,
    }
}

fn large_value_kind_for_type(column_type: &StorageColumnType) -> LargeValueSemanticKind {
    match column_type {
        StorageColumnType::String => LargeValueSemanticKind::String,
        StorageColumnType::Bytes => LargeValueSemanticKind::Bytes,
        StorageColumnType::Nullable(inner) => large_value_kind_for_type(inner),
        _ => LargeValueSemanticKind::NotLarge,
    }
}

/// Storage descriptors are schema-derived. JSON remains string-shaped to
/// callers and operators, but its internal cell codec is distinct so a large
/// JSON descriptor cannot be mistaken for text.
pub fn storage_column_type(column: &ColumnSchema) -> StorageColumnType {
    match column.large_value_kind {
        LargeValueSemanticKind::Json => jazz_types::large_values::physical_storage_value_type(
            jazz_types::large_values::LargeValueKind::Json,
        ),
        _ => column.column_type.clone(),
    }
}

impl From<jazz_types::column_schema::ColumnSchema> for ColumnSchema {
    fn from(column: jazz_types::column_schema::ColumnSchema) -> Self {
        Self {
            name: column.name,
            large_value_kind: large_value_kind_for_type(&column.column_type),
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

    /// Whether this table declares any write authorization clause.
    pub fn has_any(&self) -> bool {
        self.insert_check.is_some()
            || self.update_using.is_some()
            || self.update_check.is_some()
            || self.delete_using.is_some()
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
    pub branch_by: Vec<String>,
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
    /// Ordered application columns indexed together on current-row storage.
    #[serde(default)]
    pub composite_indexes: BTreeSet<Vec<String>>,
    /// Per-column merge strategy. Columns omitted here use [`MergeStrategy::Lww`].
    #[serde(default)]
    pub merge_strategies: BTreeMap<String, MergeStrategy>,
}

impl TableSchema {
    /// Construct a public/read-anyone table.
    pub fn new(
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
            composite_indexes: BTreeSet::new(),
            merge_strategies: BTreeMap::new(),
        }
    }

    /// Whether this table declares any explicit authorization grants.
    /// Absence is not an authorization bypass: ordinary user operations deny.
    pub fn has_any_policy(&self) -> bool {
        self.read_policy.is_some() || self.write_policies.has_any()
    }

    /// Add an ordinary application column to this table's branch key.
    pub fn with_branch_column(mut self, column: impl Into<String>) -> Self {
        self.branch_by.push(column.into());
        self
    }

    /// Mark a user column as referencing another Jazz table.
    #[cfg(test)]
    pub fn with_reference(
        mut self,
        column: impl Into<String>,
        target_table: impl Into<String>,
    ) -> Self {
        self.references.insert(column.into(), target_table.into());
        self
    }

    /// Set a user column's merge strategy.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_column_merge_strategy(
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
    #[cfg(any(test, feature = "testing"))]
    pub fn with_read_policy(mut self, read_policy: impl Into<Option<Query>>) -> Self {
        self.read_policy = read_policy.into();
        self
    }

    /// Set the table write policy.
    #[cfg(test)]
    pub fn with_write_policy(mut self, write_policy: impl Into<Option<Query>>) -> Self {
        self.write_policies = WritePolicies::legacy(write_policy.into());
        self
    }

    /// Set operation-specific write policies.
    #[cfg(any(test, feature = "testing"))]
    pub fn with_write_policies(mut self, write_policies: WritePolicies) -> Self {
        self.write_policies = write_policies;
        self
    }

    /// Return the storage table for rejected versions of this application table.
    pub fn rejected_versions_storage_table(&self) -> StorageTableSchema {
        let mut columns = vec![
            column("tx_time", StorageColumnType::U64),
            column("tx_node_id", StorageColumnType::U64),
            column("row_uuid", StorageColumnType::Uuid),
            column("_deletion", deletion_column().nullable()),
        ];
        columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        StorageTableSchema::new(format!("jazz_{}_rejected_versions", self.name), columns)
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
                PrimaryKeyColumn::uuid("row_uuid"),
            ]))
    }

    /// Return the storage history table for this application table.
    pub fn history_storage_table(&self) -> StorageTableSchema {
        self.history_storage_table_named(format!("jazz_{}_history", self.name))
    }

    /// Alias kept at the codec boundary to make the physical interpretation
    /// explicit at call sites.
    pub fn authored_history_storage_table(&self) -> StorageTableSchema {
        self.history_storage_table()
    }

    fn history_storage_table_named(&self, name: String) -> StorageTableSchema {
        let mut columns = vec![
            column("branch_key", StorageColumnType::Bytes),
            column("row_uuid", StorageColumnType::Uuid),
            column("tx_time", StorageColumnType::U64),
            column("tx_node_id", StorageColumnType::U64),
            column("schema_version", StorageColumnType::U64),
            column("created_by", interned_author_type()),
            column("created_at", StorageColumnType::U64),
            column("updated_by", interned_author_type()),
            column("updated_at", StorageColumnType::U64),
            // Deletion is a cell of the row image: null until deleted or restored.
            column("_deletion", deletion_column().nullable()),
        ];
        columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        // Absent on legacy records. When present, this is a strictly ordered
        // set of node-local physical column ids explicitly authored by this
        // version. Logical names remain a wire/schema-boundary concern.
        columns.push(column(
            "authored_columns",
            StorageColumnType::U64.array_of().nullable(),
        ));
        // Sign bits of a pending patch's counter ops, one per counter column
        // in schema order (SPEC 4, "Counter ops"). Empty on a settled image.
        columns.push(column(COUNTER_SIGNS_FIELD, StorageColumnType::Bytes));
        // The write's accepted seq; 0 while it has no accepted fate (SPEC 4
        // §4.6, "Durable layout").
        columns.push(column(SEQ_FIELD, StorageColumnType::U64));
        // The write's base: the settled seq its writer's image rested on and
        // the writer's own pending predecessor to this row.
        columns.push(column(BASE_SEQ_FIELD, StorageColumnType::U64.nullable()));
        columns.push(column(
            BASE_PENDING_FIELD,
            history_base_pending_storage_type(),
        ));

        // Accepted writes are keyed by seq, so "the row at seq S" is a point
        // read and "the row's writes after S" a range read. A write without
        // an accepted fate has seq 0, and its transaction identity keeps it
        // apart from the row's other pending writes.
        StorageTableSchema::new(name, columns).with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
            PrimaryKeyColumn::integer(SEQ_FIELD, IntegerKeyType::U64),
            PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
        ]))
        // No secondary index: the rows one transaction wrote are listed in its
        // `jazz_tx_touched_rows` record, so "versions of transaction T" is a
        // point read per listed row rather than an index over every version.
    }

    /// Return the global-current table: one settled row image per row.
    pub fn global_current_storage_table(&self) -> StorageTableSchema {
        let indexed_columns = self.global_current_indexed_columns();
        let mut content_columns = vec![
            column("branch_key", StorageColumnType::Bytes),
            column("row_uuid", StorageColumnType::Uuid),
            column("tx_time", StorageColumnType::U64),
            column("tx_node_id", StorageColumnType::U64),
            column("schema_version", StorageColumnType::U64),
            column("created_by", interned_author_type()),
            column("created_at", StorageColumnType::U64),
            column("updated_by", interned_author_type()),
            column("updated_at", StorageColumnType::U64),
            column("global_time", StorageColumnType::U64.nullable()),
            column("_deletion", deletion_column().nullable()),
        ];
        // Carry every user column (not only indexed ones) so the global-current
        // table is a self-sufficient current-row index: whole-table current
        // reads and subscriptions resolve cells here in O(current rows) without
        // joining the full history table. Secondary indexes are still built only
        // on the indexed subset below.
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        content_columns.push(column(
            "authored_columns",
            StorageColumnType::U64.array_of().nullable(),
        ));
        let mut content_table = StorageTableSchema::new(
            format!("jazz_{}_global_current", self.name),
            content_columns,
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
        ]));
        for indexed in &indexed_columns {
            content_table = content_table.with_index(StorageIndexSchema::new(
                global_current_index_name(indexed),
                ["branch_key".to_owned(), app_storage_column_name(indexed)],
            ));
        }
        for columns in &self.composite_indexes {
            content_table = content_table.with_index(StorageIndexSchema::new(
                global_current_composite_index_name(columns),
                std::iter::once("branch_key".to_owned())
                    .chain(columns.iter().map(|column| app_storage_column_name(column))),
            ));
        }
        // `by_seq`: rows ordered by the seq of their latest accepted change.
        // "Rows changed since W" is a range read here; there is no separate
        // change log.
        content_table.with_index(StorageIndexSchema::new(
            GLOBAL_CURRENT_BY_SEQ_INDEX,
            ["branch_key", "global_time", "row_uuid"],
        ))
    }

    /// Return the ahead-of-global candidate table (local pending rows).
    pub fn ahead_current_storage_table(&self) -> StorageTableSchema {
        let mut content_columns = vec![
            column("branch_key", StorageColumnType::Bytes),
            column("row_uuid", StorageColumnType::Uuid),
            column("tx_time", StorageColumnType::U64),
            column("tx_node_id", StorageColumnType::U64),
            column("schema_version", StorageColumnType::U64),
            column("created_by", interned_author_type()),
            column("created_at", StorageColumnType::U64),
            column("updated_by", interned_author_type()),
            column("updated_at", StorageColumnType::U64),
            column("global_time", StorageColumnType::U64.nullable()),
            column("_deletion", deletion_column().nullable()),
        ];
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                app_storage_column_name(&user_column.name),
                storage_column_type(user_column).nullable(),
            )
        }));
        content_columns.push(column(
            "authored_columns",
            StorageColumnType::U64.array_of().nullable(),
        ));
        StorageTableSchema::new(format!("jazz_{}_ahead_current", self.name), content_columns)
            // One overlay row per row: the newest pending local image.
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("branch_key"),
                PrimaryKeyColumn::uuid("row_uuid"),
            ]))
        // No transaction index: an overlay row is found through the history
        // rows its pending transaction lists in `jazz_tx_touched_rows`.
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
    /// `row_uuid`, provenance, `_deletion`, and nullable user cells.
    /// Receiver-local currentness and authority-state columns are deliberately
    /// excluded. Schema changes change this descriptor; v1 requires identical
    /// descriptors at sender and receiver. JSON cells retain their schema-derived
    /// stored-scalar kind so indirect JSON is never encoded as ordinary text.
    pub fn wire_record_descriptor(&self) -> RecordDescriptor {
        RecordDescriptor::new(
            [
                ("row_uuid".to_owned(), ValueType::Uuid),
                ("created_by".to_owned(), crate::ids::RowAuthor::value_type()),
                ("created_at".to_owned(), ValueType::U64),
                ("updated_by".to_owned(), crate::ids::RowAuthor::value_type()),
                ("updated_at".to_owned(), ValueType::U64),
                (
                    "_deletion".to_owned(),
                    ValueType::Nullable(Box::new(deletion_column().clone())),
                ),
            ]
            .into_iter()
            .chain(self.columns.iter().map(|column| {
                (
                    app_storage_column_name(&column.name),
                    ValueType::Nullable(Box::new(storage_column_type(column))),
                )
            })),
        )
    }
}

/// Value of a subscription watermark record v1 (format byte `1`).
#[doc(hidden)]
pub fn subscription_watermark_value_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("format_v1", ValueType::U8),
        ("settled_through", ValueType::U64),
        ("supporting_revision", ValueType::Bytes),
    ])
}

/// Policy-shape constructors.
#[cfg(test)]
pub struct Policy;

#[cfg(test)]
impl Policy {
    /// Owner-only policy equivalent to `column == claim("user")`.
    pub fn owner_only(table: impl Into<String>, column: impl Into<String>) -> Option<Query> {
        Some(Query::from(table).filter(eq(col(column), claim("user"))))
    }
}

fn storage_enum(name: &str, variants: &[&str]) -> StorageColumnType {
    StorageColumnType::EnumTag(
        ScalarEnumSchema::new(name, variants.iter().copied()).expect("valid enum schema"),
    )
}

fn tx_kind_column() -> StorageColumnType {
    storage_enum("jazz_tx_kind", &["mergeable", "exclusive"])
}

fn fate_column() -> StorageColumnType {
    storage_enum("jazz_fate", &["pending", "accepted", "rejected"])
}

fn deletion_column() -> StorageColumnType {
    StorageColumnType::EnumTag(
        ScalarEnumSchema::new("jazz_deletion", ["deleted", "restored"])
            .expect("valid deletion enum")
            .with_system_registry(SystemVariantRegistry::deletion_state()),
    )
}

fn rejection_reason_column() -> StorageColumnType {
    crate::tx::RejectionReason::storage_value_type()
}

fn durability_column() -> StorageColumnType {
    storage_enum("jazz_durability", &["none", "local", "global"])
}

/// History field holding a write's accepted seq (0 while unfated).
#[doc(hidden)]
pub const SEQ_FIELD: &str = "seq";

/// History field holding the settled seq of a write's base (SPEC 4 §4.6).
#[doc(hidden)]
pub const BASE_SEQ_FIELD: &str = "base_seq";

/// History field holding the pending predecessor of a write's base.
#[doc(hidden)]
pub const BASE_PENDING_FIELD: &str = "base_pending";

/// Physical type of the history `base_pending` field: a nullable record of the
/// predecessor's `(time, node)`. A record is variable-width, so the common
/// null costs a null byte and an offset instead of a reserved 25-byte tuple.
#[doc(hidden)]
pub fn history_base_pending_storage_type() -> StorageColumnType {
    StorageColumnType::Record(Box::new(RecordDescriptor::new([
        ("time", StorageColumnType::U64),
        ("node", StorageColumnType::Uuid),
    ])))
    .nullable()
}

/// History field carrying the sign bits of a patch's counter ops: bit `i`
/// (least significant first) belongs to the table's `i`-th counter column in
/// schema order. Empty when no op is negative, and on every settled image.
#[doc(hidden)]
pub const COUNTER_SIGNS_FIELD: &str = "counter_signs";

fn column(
    name: impl Into<String>,
    column_type: StorageColumnType,
) -> jazz_types::column_schema::ColumnSchema {
    jazz_types::column_schema::ColumnSchema::new(name, column_type)
}

/// Frozen carrier namespace used by schema-owned stored/current/wire fields.
/// Application names are encoded by this constructor, never parsed as identity.
pub const APP_COLUMN_PREFIX: &str = "_app_";

#[doc(hidden)]
pub fn app_storage_column_name(column: &str) -> String {
    format!("{APP_COLUMN_PREFIX}{column}")
}

/// Index on global current ordered by each row's latest accepted seq.
#[doc(hidden)]
pub const GLOBAL_CURRENT_BY_SEQ_INDEX: &str = "by_seq";

#[doc(hidden)]
pub fn global_current_index_name(column: &str) -> String {
    format!("by_app_{column}")
}

/// Logical name of a composite global-current index.
///
/// Single-column indexes are `by_app_<column>` for an arbitrary column name,
/// so a composite name must not start with `by_app_`: a column literally named
/// `composite_5_owner_4_rank` would otherwise collide with the `(owner, rank)`
/// index. `by_composite_` is a prefix no single-column name can produce, and
/// the `<len>_<name>` segments keep distinct column lists distinct.
pub fn global_current_composite_index_name(columns: &[String]) -> String {
    format!(
        "by_composite_{}",
        columns
            .iter()
            .map(|column| format!("{}_{}", column.len(), column))
            .collect::<Vec<_>>()
            .join("_")
    )
}

/// `jazz_transactions` (`jazz.tx-record.v2`): the permanent record of one
/// transaction, keyed `(time, node_id)`. It holds only what every node needs
/// for as long as it keeps the transaction: its identity, kind, author,
/// cardinality, fate, seq and durability. Everything else lives beside it
/// (SPEC 2 §2.8):
///
/// - fields needed only until the transaction has a seq or a rejection
///   (`permission_subject` and the serializable read evidence) in
///   `jazz_pending_transactions`, deleted by the batch that settles it;
/// - calculated-merge and branch-write provenance, which grows with the number
///   of writes, in `jazz_tx_provenance`, so status reads never copy it;
/// - the rejection reason in one nullable record that costs a byte on every
///   transaction that was not rejected.
fn transactions_table() -> StorageTableSchema {
    StorageTableSchema::new(
        "jazz_transactions",
        [
            column("time", StorageColumnType::U64),
            column("node_id", StorageColumnType::U64),
            column("kind", tx_kind_column()),
            // Bit 0: `n_total_writes` is only the view-redacted cardinality.
            // Bit 1: a `jazz_tx_provenance` record exists.
            // Bit 2: the permission subject is the author itself. Other bits
            // are reserved and refused on read.
            column("flags", StorageColumnType::U8),
            column("n_total_writes", StorageColumnType::U32),
            column("made_by", interned_author_type()),
            // upstream-decided: written only by fate/state application.
            column("fate", fate_column()),
            // upstream-decided: written only by fate/state application.
            column("global_time", StorageColumnType::U64.nullable()),
            // node-local-derived: updated when the node learns stronger durability.
            column("durability", durability_column()),
            column("user_metadata", StorageColumnType::String.nullable()),
            // upstream-decided: set exactly when `fate` is rejected.
            column("rejection", rejection_reason_column().nullable()),
            // The identity the writes were authorized as when it is not the
            // author (flag bit 2 covers that case). Replayed units carry it,
            // so it is kept for the transaction's whole life.
            column(
                "permission_subject",
                crate::ids::AuthorSubject::value_type().nullable(),
            ),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
    ]))
    .with_index(StorageIndexSchema::new("by_global_time", ["global_time"]))
}

const TX_PROVENANCE_TABLE: &str = "jazz_tx_provenance";

fn contribution_component_column() -> StorageColumnType {
    StorageColumnType::Enum(Box::new(
        EnumSchema::new(
            "jazz_contribution_component",
            [
                EnumCase::new(
                    "column",
                    // The public/wire contribution coordinate names a logical
                    // column.  Its durable payload is instead this node's
                    // stable physical-column identity; names are resolved at
                    // the storage boundary through the retained catalogue.
                    RecordDescriptor::new([("physical_column_id", ValueType::U64)]),
                ),
                EnumCase::new(
                    "operation",
                    RecordDescriptor::new([
                        ("physical_column_id", ValueType::U64),
                        ("identity", ValueType::Bytes),
                    ]),
                ),
                EnumCase::new(
                    "register",
                    RecordDescriptor::new(std::iter::empty::<(String, ValueType)>()),
                ),
            ],
        )
        .expect("valid contribution component enum"),
    ))
}

fn contribution_coordinate_column() -> StorageColumnType {
    StorageColumnType::Record(Box::new(RecordDescriptor::new([
        ("branch_key", ValueType::Bytes),
        ("physical_table_id", ValueType::U64),
        ("row_uuid", ValueType::Uuid),
        (
            "layer",
            storage_enum("jazz_contribution_layer", &["content", "deletion"]),
        ),
        ("component", contribution_component_column()),
    ])))
}

fn contribution_dot_column() -> StorageColumnType {
    StorageColumnType::Record(Box::new(RecordDescriptor::new([
        ("tx_time", ValueType::U64),
        ("tx_node", ValueType::Uuid),
        ("coordinate", contribution_coordinate_column()),
    ])))
}

fn contribution_merge_column() -> StorageColumnType {
    StorageColumnType::Record(Box::new(RecordDescriptor::new([
        ("source", ValueType::Bytes),
        ("target", ValueType::Bytes),
        (
            "substitutions",
            StorageColumnType::Record(Box::new(RecordDescriptor::new([
                ("target", contribution_coordinate_column()),
                ("sources", contribution_dot_column().array_of()),
            ])))
            .array_of(),
        ),
        // Versioned non-causal authorization evidence for a first head
        // overlay. This is a normal record, not an opaque postcard
        // payload: every field remains inspectable by authority admission.
        (
            "branch_view_copy_v1",
            StorageColumnType::Record(Box::new(RecordDescriptor::new([
                ("version", ValueType::U8),
                ("head", ValueType::Bytes),
                (
                    "base",
                    StorageColumnType::Enum(Box::new(
                        EnumSchema::new(
                            "jazz_branch_view_copy_base_v1",
                            [
                                EnumCase::new(
                                    "current",
                                    RecordDescriptor::new([("branch", ValueType::Bytes)]),
                                ),
                                EnumCase::new(
                                    "snapshot",
                                    RecordDescriptor::new([
                                        ("branch", ValueType::Bytes),
                                        ("owner", ValueType::Uuid),
                                        ("global_base", ValueType::U64),
                                        ("local_base", ValueType::U64),
                                        (
                                            "dots",
                                            ValueType::Record(Box::new(RecordDescriptor::new([
                                                ("time", ValueType::U64),
                                                ("node", ValueType::Uuid),
                                            ])))
                                            .array_of(),
                                        ),
                                    ]),
                                ),
                            ],
                        )
                        .expect("valid branch-view copy base enum"),
                    )),
                ),
                ("table", ValueType::String),
                ("row_uuid", ValueType::Uuid),
                ("source_time", ValueType::U64),
                ("source_node", ValueType::Uuid),
            ])))
            .array_of(),
        ),
        // Every non-root mergeable branch write carries one canonical intent.
        // Its table identity is physical; the authored schema disambiguates
        // historical logical names when admission resolves it.
        (
            "branch_write_intent_v1",
            StorageColumnType::Record(Box::new(RecordDescriptor::new([
                ("version", ValueType::U8),
                ("physical_table_id", ValueType::U64),
                ("authored_schema", ValueType::Uuid),
                ("row_uuid", ValueType::Uuid),
                ("head", ValueType::Bytes),
                (
                    "operation",
                    StorageColumnType::Enum(
                        Box::new(
                            EnumSchema::new(
                                "jazz_branch_write_operation_v1",
                                [
                                    EnumCase::new(
                                        "exact_head_insert",
                                        RecordDescriptor::new(std::iter::empty::<(
                                            String,
                                            ValueType,
                                        )>(
                                        )),
                                    ),
                                    EnumCase::new(
                                        "exact_head_update",
                                        RecordDescriptor::new(std::iter::empty::<(
                                            String,
                                            ValueType,
                                        )>(
                                        )),
                                    ),
                                    EnumCase::new(
                                        "view_update_copy",
                                        RecordDescriptor::new([("evidence_index", ValueType::U32)]),
                                    ),
                                ],
                            )
                            .expect("valid branch write operation enum"),
                        ),
                    ),
                ),
            ])))
            .array_of(),
        ),
    ])))
}

/// Bound physical type of `jazz_transactions.rejection`, with the registry
/// paths of the real column.
pub fn transaction_rejection_storage_type() -> StorageColumnType {
    transactions_table()
        .columns
        .into_iter()
        .find(|column| column.name == "rejection")
        .expect("transaction rejection storage column")
        .column_type
}

/// Bound physical type used by the transaction codec. Constructing the
/// single-column table applies the same durable registry paths as the real
/// `jazz_tx_provenance.contribution_merge` column.
pub fn contribution_merge_storage_type() -> StorageColumnType {
    StorageTableSchema::new(
        TX_PROVENANCE_TABLE,
        [column("contribution_merge", contribution_merge_column())],
    )
    .columns
    .into_iter()
    .next()
    .expect("contribution merge storage column")
    .column_type
}

/// Domain tag of the frozen schema-id encoding. Every schema without a
/// composite index is addressed by exactly these bytes, so its
/// [`SchemaVersionId`] never changes.
const SCHEMA_ID_DOMAIN_V1: &str = "jazz-schema-v1-large-value-kinds";
/// Domain tag used exactly when some table declares a composite index. The
/// body is the v1 body with, after each table's `branch_by`, that table's
/// composite indexes: a `u64` count, then per index (in canonical order,
/// lexicographic over UTF-8 column-name bytes) a `u64` column count and each
/// length-prefixed column name. A distinct domain tag rather than an optional
/// trailing section keeps the encoding injective.
const SCHEMA_ID_DOMAIN_V2_COMPOSITE_INDEXES: &str = "jazz-schema-v2-composite-indexes";

fn canonical_schema_bytes(schema: &RuntimeSchema) -> Vec<u8> {
    let with_composite_indexes = schema
        .tables
        .iter()
        .any(|table| !table.composite_indexes.is_empty());
    let mut bytes = Vec::new();
    put_str(
        &mut bytes,
        if with_composite_indexes {
            SCHEMA_ID_DOMAIN_V2_COMPOSITE_INDEXES
        } else {
            SCHEMA_ID_DOMAIN_V1
        },
    );
    let mut tables = schema.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    put_u64(&mut bytes, tables.len() as u64);
    for table in tables {
        put_str(&mut bytes, &table.name);
        put_u64(&mut bytes, table.columns.len() as u64);
        for column in &table.columns {
            put_str(&mut bytes, &column.name);
            put_column_type(&mut bytes, &column.column_type);
            bytes.push(match column.large_value_kind {
                LargeValueSemanticKind::NotLarge => 0,
                LargeValueSemanticKind::Bytes => 1,
                LargeValueSemanticKind::String => 2,
                LargeValueSemanticKind::Json => 3,
            });
            put_merge_strategy(&mut bytes, table.merge_strategy(&column.name));
        }
        put_u64(&mut bytes, table.references.len() as u64);
        for (column, target) in &table.references {
            put_str(&mut bytes, column);
            put_str(&mut bytes, target);
        }
        let mut branch_by = table.branch_by.iter().collect::<Vec<_>>();
        branch_by.sort();
        put_u64(&mut bytes, branch_by.len() as u64);
        for column in branch_by {
            put_str(&mut bytes, column);
        }
        if with_composite_indexes {
            // `BTreeSet<Vec<String>>` iterates in canonical order.
            put_u64(&mut bytes, table.composite_indexes.len() as u64);
            for columns in &table.composite_indexes {
                put_u64(&mut bytes, columns.len() as u64);
                for column in columns {
                    put_str(&mut bytes, column);
                }
            }
        }
    }
    bytes
}

fn put_merge_strategy(bytes: &mut Vec<u8>, strategy: MergeStrategy) {
    bytes.push(match strategy {
        MergeStrategy::Lww => 0,
        MergeStrategy::Counter => 1,
        MergeStrategy::GSet => 2,
        MergeStrategy::Text => 3,
        MergeStrategy::Markdown => 4,
        MergeStrategy::Html => 5,
    });
}

fn put_column_type(bytes: &mut Vec<u8>, column_type: &StorageColumnType) {
    match column_type {
        StorageColumnType::U8 => bytes.push(0),
        StorageColumnType::U16 => bytes.push(1),
        StorageColumnType::U32 => bytes.push(2),
        StorageColumnType::U64 => bytes.push(3),
        StorageColumnType::I32 => bytes.push(4),
        StorageColumnType::I64 => bytes.push(5),
        StorageColumnType::F64 => bytes.push(6),
        StorageColumnType::Bool => bytes.push(7),
        StorageColumnType::String => bytes.push(8),
        StorageColumnType::Bytes => bytes.push(9),
        StorageColumnType::Uuid => bytes.push(10),
        StorageColumnType::EnumTag(schema) => {
            bytes.push(11);
            put_str(bytes, &schema.name);
            put_u64(bytes, schema.variants.len() as u64);
            for variant in &schema.variants {
                put_str(bytes, variant);
            }
        }
        StorageColumnType::Tuple(members) => {
            bytes.push(12);
            put_u64(bytes, members.len() as u64);
            for member in members {
                put_column_type(bytes, member);
            }
        }
        StorageColumnType::Array(member) => {
            bytes.push(13);
            put_column_type(bytes, member);
        }
        StorageColumnType::Nullable(member) => {
            bytes.push(14);
            put_column_type(bytes, member);
        }
        StorageColumnType::Record(descriptor) => {
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
        StorageColumnType::Enum(schema) => {
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
        _ => {
            panic!("raw stored-scalar backing types cannot appear in a Jazz schema")
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

/// Whether a stored column's physical encoding stays readable under another
/// table schema's column: same merge strategy and a value type that only
/// appends enum variants or cases.
pub fn physical_column_epoch_is_compatible(
    source_table: &TableSchema,
    source_column_name: &str,
    target_table: &TableSchema,
    target_column_name: &str,
) -> bool {
    let Some(source_column) = source_table
        .columns
        .iter()
        .find(|column| column.name == source_column_name)
    else {
        return false;
    };
    let Some(target_column) = target_table
        .columns
        .iter()
        .find(|column| column.name == target_column_name)
    else {
        return false;
    };

    physical_value_epoch_is_compatible(&source_column.column_type, &target_column.column_type)
        && source_table.merge_strategy(source_column_name)
            == target_table.merge_strategy(target_column_name)
}

#[doc(hidden)]
pub fn physical_value_epoch_is_compatible(
    source: &jazz_types::records::ValueType,
    target: &jazz_types::records::ValueType,
) -> bool {
    use jazz_types::records::ValueType;
    match (source, target) {
        (ValueType::EnumTag(left), ValueType::EnumTag(right)) => {
            right.variants.starts_with(&left.variants)
        }
        (ValueType::Enum(left), ValueType::Enum(right)) => {
            right.cases.len() >= left.cases.len()
                && left.cases.iter().zip(&right.cases).all(|(a, b)| {
                    a.name == b.name && physical_record_epoch_is_compatible(&a.payload, &b.payload)
                })
        }
        (ValueType::Tuple(left), ValueType::Tuple(right)) => {
            left.len() == right.len()
                && left
                    .iter()
                    .zip(right)
                    .all(|(a, b)| physical_value_epoch_is_compatible(a, b))
        }
        (ValueType::Array(left), ValueType::Array(right))
        | (ValueType::Nullable(left), ValueType::Nullable(right)) => {
            physical_value_epoch_is_compatible(left, right)
        }
        (ValueType::Record(left), ValueType::Record(right)) => {
            physical_record_epoch_is_compatible(left, right)
        }
        _ => source == target,
    }
}

fn physical_record_epoch_is_compatible(
    source: &jazz_types::records::RecordDescriptor,
    target: &jazz_types::records::RecordDescriptor,
) -> bool {
    source.fields().len() == target.fields().len()
        && source.fields().iter().zip(target.fields()).all(|(a, b)| {
            a.name == b.name && physical_value_epoch_is_compatible(&a.value_type, &b.value_type)
        })
}

/// Record layout of a row's local-availability receipt column.
pub fn local_availability_record_descriptor() -> jazz_types::records::RecordDescriptor {
    jazz_types::records::RecordDescriptor::new([
        ("format_v1", jazz_types::records::ValueType::U8),
        ("unavailable", jazz_types::records::ValueType::Bool),
        ("core", jazz_types::records::ValueType::Uuid),
        ("core_epoch", jazz_types::records::ValueType::U64),
        ("claims_revision", jazz_types::records::ValueType::U64),
        ("policy_epoch", jazz_types::records::ValueType::U64),
        ("settled_through", jazz_types::records::ValueType::U64),
        ("evaluation_seq", jazz_types::records::ValueType::U64),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;
    use jazz_types::column_schema::ColumnType;

    #[test]
    fn string_branch_columns_are_accepted() {
        let _ = JazzSchema::new_with_branch_columns([TableSchema::new(
            "todos",
            [ColumnSchema::new("branch", ColumnType::String)],
        )
        .with_branch_column("branch")]);
    }

    #[test]
    fn branch_selector_projection_uses_the_declared_enum_codec() {
        let phase = ScalarEnumSchema::new("phase", ["draft", "ready"]).unwrap();
        let schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new(
                "phase",
                ColumnType::EnumTag(phase.clone()),
            )],
        )
        .with_branch_column("phase")]);
        let table = &schema.tables[0];

        let (key, cells) = schema
            .project_branch_selector(
                table,
                &BranchSelector::new([("phase", Value::String("ready".to_owned()))]),
            )
            .unwrap();

        assert_eq!(key.values[0].1.0, [1, 8, 1]);
        assert_eq!(cells["phase"], Value::String("ready".to_owned()));
        assert_eq!(
            schema.validate_authored_branch_key(table, &key).unwrap()["phase"],
            Value::EnumTag(1)
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
        let registry = |table: StorageTableSchema, name: &str| match &table
            .columns
            .iter()
            .find(|column| column.name == name)
            .unwrap()
            .column_type
        {
            StorageColumnType::EnumTag(schema) => schema.registry_id(),
            StorageColumnType::Nullable(inner) => match inner.as_ref() {
                StorageColumnType::EnumTag(schema) => schema.registry_id(),
                other => panic!("expected enum field, got {other:?}"),
            },
            other => panic!("expected enum field, got {other:?}"),
        };

        assert_eq!(
            registry(left.history_storage_table(), "_deletion"),
            registry(right.history_storage_table(), "_deletion")
        );
        assert_ne!(
            registry(left.history_storage_table(), "_app_state"),
            registry(right.history_storage_table(), "_app_state"),
            "structurally identical user enums must retain independent registries"
        );
    }

    #[test]
    fn deserializing_a_schema_rejects_internal_storage_types_and_kind_spoofing() {
        let internal = jazz_types::large_values::physical_storage_value_type(
            jazz_types::large_values::LargeValueKind::Json,
        );
        let internal_column = serde_json::json!({
            "name": "body",
            "column_type": internal,
            "large_value_kind": "Json",
            "default": null,
        });
        assert!(
            serde_json::from_value::<ColumnSchema>(internal_column).is_err(),
            "serde must not bypass the public schema constructor with an internal type"
        );

        let spoofed_kind = serde_json::json!({
            "name": "body",
            "column_type": StorageColumnType::Bytes,
            "large_value_kind": "Json",
            "default": null,
        });
        assert!(
            serde_json::from_value::<ColumnSchema>(spoofed_kind).is_err(),
            "semantic kind must be structurally compatible with its public column type"
        );
    }

    #[test]
    fn persisted_branch_keys_require_the_owning_table_type_and_enum_domain() {
        let uuid_schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("branch", ColumnType::Uuid)],
        )
        .with_branch_column("branch")]);
        let uuid_table = &uuid_schema.tables[0];
        let (valid, _) = uuid_schema
            .project_branch_selector(
                uuid_table,
                &BranchSelector::new([("branch", Value::Uuid(uuid::Uuid::from_bytes([0x42; 16])))]),
            )
            .unwrap();
        assert_eq!(
            RuntimeSchema::decode_persisted_branch_key(uuid_table, &valid.canonical_bytes())
                .unwrap(),
            valid
        );

        let wrong_scalar = BranchKey {
            values: vec![(
                "branch".to_owned(),
                BranchColumnValue::from(Value::String("not-a-uuid".to_owned())),
            )],
        };
        assert!(
            RuntimeSchema::decode_persisted_branch_key(uuid_table, &wrong_scalar.canonical_bytes())
                .is_err()
        );

        let phase = ScalarEnumSchema::new("phase", ["draft", "ready"]).unwrap();
        let enum_schema = RuntimeSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("phase", ColumnType::EnumTag(phase))],
        )
        .with_branch_column("phase")]);
        let invalid_enum = BranchKey {
            values: vec![("phase".to_owned(), BranchColumnValue(vec![1, 8, u8::MAX]))],
        };
        assert!(
            RuntimeSchema::decode_persisted_branch_key(
                &enum_schema.tables[0],
                &invalid_enum.canonical_bytes()
            )
            .is_err()
        );
    }
}
