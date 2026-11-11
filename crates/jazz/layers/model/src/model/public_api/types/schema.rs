use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};

use internment::Intern;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use super::*;

/// Interned name identifying a table in the schema.
/// Pointer-sized (8 bytes), Copy, fast equality via pointer comparison.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TableName(pub Intern<String>);

impl Serialize for TableName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for TableName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(TableName::new(s))
    }
}

impl TableName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Intern::new(name.into()))
    }

    /// Get the underlying string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for TableName {
    fn from(s: T) -> Self {
        Self(Intern::new(s.into()))
    }
}

impl std::fmt::Display for TableName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq<str> for TableName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for TableName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for TableName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

impl PartialOrd for TableName {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TableName {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

/// Column data type.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", deny_unknown_fields)]
pub enum ColumnType {
    /// 4-byte signed integer (i32), like PostgreSQL INTEGER.
    Integer,
    /// 8-byte signed integer (i64), like PostgreSQL BIGINT.
    BigInt,
    /// 1-byte boolean.
    Boolean,
    /// Variable-length UTF-8 text.
    Text,
    /// Enumerated text constrained to a closed set of variants.
    Enum { variants: Vec<String> },
    /// Catalogue-native scalar enum whose declaration order assigns durable
    /// compact tags. This is retained for core schema-evolution fixtures and
    /// catalogue source round-tripping; ordinary application enums use `Enum`.
    #[doc(hidden)]
    ScalarEnum { name: String, variants: Vec<String> },
    /// Catalogue-native payload enum with an explicit compiled type name.
    /// Ordinary application payload enums use `EnumPayload`.
    #[doc(hidden)]
    CatalogueEnumPayload {
        name: String,
        cases: Vec<EnumCaseDescriptor>,
    },
    /// Discriminated enum whose selected case carries a named record payload.
    /// This is a column type, never a top-level Jazz row union.
    EnumPayload { cases: Vec<EnumCaseDescriptor> },
    /// 8-byte unsigned timestamp (Unix milliseconds).
    Timestamp,
    /// 8-byte IEEE 754 double-precision float (f64).
    Double,
    /// 16-byte UUID (ObjectId).
    Uuid,
    /// 16-byte batch/version identity.
    TransactionId,
    /// Variable-length binary payload.
    Bytea,
    /// JSON payload stored as UTF-8 text, optionally constrained by JSON Schema.
    Json {
        #[serde(skip_serializing_if = "Option::is_none")]
        schema: Option<serde_json::Value>,
    },
    /// Homogeneous array of values.
    Array { element: Box<ColumnType> },
    /// Heterogeneous row/tuple of values with a known schema.
    /// Used for nested rows (e.g., array of rows from subquery).
    Row { columns: Box<RowDescriptor> },
}

impl ColumnType {
    /// Returns the element type if this is an array, None otherwise.
    pub fn element_type(&self) -> Option<&ColumnType> {
        match self {
            ColumnType::Array { element } => Some(element),
            _ => None,
        }
    }

    /// Returns the row descriptor if this is a Row type, None otherwise.
    pub fn row_descriptor(&self) -> Option<&RowDescriptor> {
        match self {
            ColumnType::Row { columns } => Some(columns),
            _ => None,
        }
    }
}

/// One named payload case of a column-local discriminated enum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnumCaseDescriptor {
    pub name: String,
    pub fields: Vec<ColumnDescriptor>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ColumnMergeStrategy {
    Counter,
    GSet,
    /// Concurrent edits to a plain-text string are merged three-way.
    Text,
    /// As [`Self::Text`], never splitting markdown syntax.
    Markdown,
    /// As [`Self::Text`], never splitting HTML tags or entities.
    Html,
}

impl ColumnMergeStrategy {
    const NAMES: [(&'static str, Self); 5] = [
        ("Counter", Self::Counter),
        ("GSet", Self::GSet),
        ("Text", Self::Text),
        ("Markdown", Self::Markdown),
        ("Html", Self::Html),
    ];

    /// Whether this strategy merges the text of a string column.
    pub fn is_text(self) -> bool {
        matches!(self, Self::Text | Self::Markdown | Self::Html)
    }
}

/// A strategy this version does not know (a schema written by a newer
/// Jazz) is refused by name rather than as an unknown serde variant.
/// Binary encodings keep the derived variant-index form.
impl<'de> Deserialize<'de> for ColumnMergeStrategy {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        #[serde(rename = "ColumnMergeStrategy")]
        enum Indexed {
            Counter,
            GSet,
            Text,
            Markdown,
            Html,
        }
        if !deserializer.is_human_readable() {
            return Ok(match Indexed::deserialize(deserializer)? {
                Indexed::Counter => Self::Counter,
                Indexed::GSet => Self::GSet,
                Indexed::Text => Self::Text,
                Indexed::Markdown => Self::Markdown,
                Indexed::Html => Self::Html,
            });
        }
        let name = String::deserialize(deserializer)?;
        Self::NAMES
            .iter()
            .find(|(known, _)| *known == name)
            .map(|(_, strategy)| *strategy)
            .ok_or_else(|| {
                serde::de::Error::custom(format!(
                    "merge strategy `{name}` is not supported by this version of Jazz"
                ))
            })
    }
}

/// Interned column name type.
/// Pointer-sized (8 bytes), Copy, fast equality.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnName(pub Intern<String>);

impl Serialize for ColumnName {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.as_str().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ColumnName {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(ColumnName::new(s))
    }
}

impl ColumnName {
    pub fn new(name: impl Into<String>) -> Self {
        Self(Intern::new(name.into()))
    }

    /// Get the underlying string reference.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl<T: Into<String>> From<T> for ColumnName {
    fn from(s: T) -> Self {
        Self(Intern::new(s.into()))
    }
}

impl std::fmt::Display for ColumnName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl PartialEq<str> for ColumnName {
    fn eq(&self, other: &str) -> bool {
        self.as_str() == other
    }
}

impl PartialEq<&str> for ColumnName {
    fn eq(&self, other: &&str) -> bool {
        self.as_str() == *other
    }
}

impl PartialEq<String> for ColumnName {
    fn eq(&self, other: &String) -> bool {
        self.as_str() == other
    }
}

/// Descriptor for a single column in a row.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ColumnDescriptor {
    pub name: ColumnName,
    pub column_type: ColumnType,
    pub nullable: bool,
    /// Optional foreign key reference to another table.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub references: Option<TableName>,
    /// Optional schema-level default used for omitted values on insert.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Value>,
    /// Optional per-column merge strategy. Absence means MRCA-relative LWW.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub merge_strategy: Option<ColumnMergeStrategy>,
}

impl ColumnDescriptor {
    pub fn new(name: impl Into<ColumnName>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            nullable: false,
            references: None,
            default: None,
            merge_strategy: None,
        }
    }

    /// Get the column name as a string slice.
    pub fn name_str(&self) -> &str {
        self.name.as_str()
    }

    pub fn nullable(mut self) -> Self {
        self.nullable = true;
        self
    }

    pub fn references(mut self, table: impl Into<TableName>) -> Self {
        self.references = Some(table.into());
        self
    }

    pub fn default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }

    pub fn merge_strategy(mut self, strategy: ColumnMergeStrategy) -> Self {
        self.merge_strategy = Some(strategy);
        self
    }

    pub fn validate_merge_strategy(&self) -> Result<(), String> {
        match self.merge_strategy {
            None => Ok(()),
            Some(ColumnMergeStrategy::Counter) => {
                if self.nullable
                    || !matches!(self.column_type, ColumnType::Integer | ColumnType::BigInt)
                {
                    Err(
                        "Counter merge strategy is only supported on non-nullable INTEGER or BIGINT columns"
                            .to_owned(),
                    )
                } else {
                    Ok(())
                }
            }
            Some(
                strategy @ (ColumnMergeStrategy::Text
                | ColumnMergeStrategy::Markdown
                | ColumnMergeStrategy::Html),
            ) => {
                if !matches!(self.column_type, ColumnType::Text) {
                    Err(format!(
                        "{strategy:?} merge strategy is only supported on TEXT columns, got {} ({:?})",
                        self.name_str(),
                        self.column_type,
                    ))
                } else {
                    Ok(())
                }
            }
            Some(ColumnMergeStrategy::GSet) => {
                if self.nullable || !matches!(self.column_type, ColumnType::Array { .. }) {
                    Err(format!(
                        "g-set merge strategy is only supported on non-nullable ARRAY columns, got {} ({:?}, nullable={})",
                        self.name_str(),
                        self.column_type,
                        self.nullable
                    ))
                } else {
                    Ok(())
                }
            }
        }
    }
}

/// Descriptor for a row's schema, defining column order and types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct RowDescriptor {
    pub columns: Vec<ColumnDescriptor>,
}

impl From<Vec<ColumnDescriptor>> for RowDescriptor {
    fn from(columns: Vec<ColumnDescriptor>) -> Self {
        Self::new(columns)
    }
}

impl RowDescriptor {
    pub fn new(columns: Vec<ColumnDescriptor>) -> Self {
        Self { columns }
    }

    /// Find column index by name.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.as_str() == name)
    }

    /// Get column descriptor by name.
    pub fn column(&self, name: &str) -> Option<&ColumnDescriptor> {
        self.columns.iter().find(|c| c.name.as_str() == name)
    }

    /// Combine multiple descriptors into one (for join outputs).
    /// Column names from later descriptors are preserved as-is.
    /// Use with table-qualified names to avoid ambiguity.
    pub fn combine(descriptors: &[RowDescriptor]) -> Self {
        let columns: Vec<ColumnDescriptor> =
            descriptors.iter().flat_map(|d| d.columns.clone()).collect();
        Self::new(columns)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Row structure definition.
    pub columns: RowDescriptor,
    /// User columns that should have secondary indexes.
    ///
    /// `None` preserves historical behavior and indexes every declared user
    /// column. `Some(columns)` opts into indexing only that explicit subset.
    /// Internal `_id` and `_id_deleted` indexes are always maintained.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexed_columns: Option<Vec<ColumnName>>,
    /// Ordered multi-column indexes. The first column can be constrained by an
    /// equality while the following column supplies the query's sort order.
    ///
    /// Column order inside one index is significant. The set of indexes is
    /// not: the builder and deserializer keep it in canonical order
    /// (lexicographic over the columns' UTF-8 bytes), and equality,
    /// serialization, [`SchemaHash`], and schema compilation all apply that
    /// order themselves, so a hand-built, unsorted `Vec` is equivalent to the
    /// sorted one everywhere schemas are compared, hashed, or encoded.
    #[serde(
        default,
        skip_serializing_if = "Vec::is_empty",
        serialize_with = "serialize_composite_indexes_canonically",
        deserialize_with = "deserialize_composite_indexes_canonically"
    )]
    pub composite_indexes: Vec<Vec<ColumnName>>,
    /// Access control policies.
    #[serde(default, skip_serializing_if = "table_policies_are_default")]
    pub policies: TablePolicies,
    /// Ordinary immutable columns that form this table's branch key.
    #[serde(default, rename = "branchBy", skip_serializing_if = "Vec::is_empty")]
    pub branch_by: Vec<ColumnName>,
    /// How much of each row's history nodes keep (SPEC 4 §4.9). `Full`
    /// is omitted, so a table that keeps full history serializes and
    /// hashes as it did before retention existed.
    #[serde(default, skip_serializing_if = "HistoryRetention::is_full")]
    pub history: HistoryRetention,
}

/// How much of a table's row history nodes keep (SPEC 4 §4.9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum HistoryRetention {
    /// Every accepted version, for history reads, snapshots and branches.
    #[default]
    Full,
    /// Only each row's newest accepted version (and any pending ones
    /// above it). Reads of older versions are refused.
    Latest,
}

impl HistoryRetention {
    pub fn is_full(&self) -> bool {
        *self == Self::Full
    }
}

impl PartialEq for TableSchema {
    fn eq(&self, other: &Self) -> bool {
        self.columns == other.columns
            && self.indexed_columns == other.indexed_columns
            && canonical_composite_index_order(&self.composite_indexes)
                == canonical_composite_index_order(&other.composite_indexes)
            && self.policies == other.policies
            && self.branch_by == other.branch_by
            && self.history == other.history
    }
}

/// Canonical order of a table's composite indexes: lexicographic over each
/// index's column names compared as UTF-8 bytes (equivalently, Unicode code
/// points). TypeScript's `structuralSchemaHash` must use the same comparator.
pub fn canonical_composite_index_order(indexes: &[Vec<ColumnName>]) -> Vec<Vec<&str>> {
    let mut sorted = indexes
        .iter()
        .map(|columns| {
            columns
                .iter()
                .map(|column| column.as_str())
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>();
    sorted.sort_unstable();
    sorted
}

/// Sort a table's composite indexes into [`canonical_composite_index_order`],
/// so structural equality of two schemas agrees with their [`SchemaHash`].
fn canonicalize_composite_indexes(mut indexes: Vec<Vec<ColumnName>>) -> Vec<Vec<ColumnName>> {
    indexes.sort_unstable_by(|left, right| {
        left.iter()
            .map(ColumnName::as_str)
            .cmp(right.iter().map(ColumnName::as_str))
    });
    indexes
}

fn deserialize_composite_indexes_canonically<'de, D>(
    deserializer: D,
) -> Result<Vec<Vec<ColumnName>>, D::Error>
where
    D: Deserializer<'de>,
{
    Vec::<Vec<ColumnName>>::deserialize(deserializer).map(canonicalize_composite_indexes)
}

fn serialize_composite_indexes_canonically<S>(
    indexes: &[Vec<ColumnName>],
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    canonical_composite_index_order(indexes).serialize(serializer)
}

fn table_policies_are_default(policies: &TablePolicies) -> bool {
    *policies == TablePolicies::default()
}

impl TableSchema {
    /// Create a new table schema with no explicit policies.
    ///
    /// Serving authorities deny user reads and writes without an explicit grant.
    /// Optimistic local staging does not imply authority acceptance.
    pub fn new(columns: RowDescriptor) -> Self {
        Self {
            columns,
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: TablePolicies::default(),
            branch_by: Vec::new(),
            history: HistoryRetention::Full,
        }
    }

    /// Create a table schema with policies.
    pub fn with_policies(columns: RowDescriptor, policies: TablePolicies) -> Self {
        Self {
            columns,
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies,
            branch_by: Vec::new(),
            history: HistoryRetention::Full,
        }
    }

    /// Start building a new table schema.
    pub fn builder(name: &str) -> TableSchemaBuilder {
        TableSchemaBuilder::new(name)
    }

    /// Return true when the given user column has a maintained secondary index.
    ///
    /// The implicit object-id indexes are always available and are handled here
    /// too so query planning can use one predicate path.
    pub fn is_indexed_column(&self, column: &str) -> bool {
        if column == "_id" || column == "_id_deleted" {
            return true;
        }
        // Blob columns carry no index (see should_index_column), so the
        // planner must not consult one.
        if self
            .columns
            .column(column)
            .is_some_and(|descriptor| matches!(descriptor.column_type, ColumnType::Bytea))
        {
            return false;
        }
        self.indexed_columns
            .as_ref()
            .is_none_or(|columns| columns.iter().any(|name| name.as_str() == column))
    }
}

impl From<RowDescriptor> for TableSchema {
    fn from(columns: RowDescriptor) -> Self {
        Self::new(columns)
    }
}

/// Builder for creating TableSchema with a fluent API.
#[derive(Debug, Clone)]
pub struct TableSchemaBuilder {
    name: String,
    columns: Vec<ColumnDescriptor>,
    indexed_columns: Option<Vec<ColumnName>>,
    composite_indexes: Vec<Vec<ColumnName>>,
    policies: TablePolicies,
    branch_by: Vec<ColumnName>,
    history: HistoryRetention,
}

impl TableSchemaBuilder {
    /// Create a new builder for a table with the given name.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            columns: Vec::new(),
            indexed_columns: None,
            composite_indexes: Vec::new(),
            policies: TablePolicies::default(),
            branch_by: Vec::new(),
            history: HistoryRetention::Full,
        }
    }

    /// Add a column to the table.
    pub fn column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns.push(ColumnDescriptor::new(name, column_type));
        self
    }

    /// Add a column with a schema-level default to the table.
    pub fn column_with_default(
        mut self,
        name: &str,
        column_type: ColumnType,
        default: Value,
    ) -> Self {
        self.columns
            .push(ColumnDescriptor::new(name, column_type).default(default));
        self
    }

    /// Add a nullable column to the table.
    pub fn nullable_column(mut self, name: &str, column_type: ColumnType) -> Self {
        self.columns
            .push(ColumnDescriptor::new(name, column_type).nullable());
        self
    }

    /// Add a foreign key column.
    pub fn fk_column(mut self, name: &str, references: &str) -> Self {
        self.columns
            .push(ColumnDescriptor::new(name, ColumnType::Uuid).references(references));
        self
    }

    /// Add a nullable foreign key column.
    pub fn nullable_fk_column(mut self, name: &str, references: &str) -> Self {
        self.columns.push(
            ColumnDescriptor::new(name, ColumnType::Uuid)
                .nullable()
                .references(references),
        );
        self
    }

    /// Add an array foreign key column.
    pub fn array_fk_column(mut self, name: &str, references: &str) -> Self {
        self.columns.push(
            ColumnDescriptor::new(
                name,
                ColumnType::Array {
                    element: Box::new(ColumnType::Uuid),
                },
            )
            .references(references),
        );
        self
    }

    /// Set policies for the table.
    pub fn policies(mut self, policies: TablePolicies) -> Self {
        self.policies = policies;
        self
    }

    /// Add an ordinary application column to this table's branch key.
    pub fn branch_by(mut self, column: impl Into<ColumnName>) -> Self {
        self.branch_by.push(column.into());
        self
    }

    /// Keep only each row's latest version (SPEC 4 §4.9).
    pub fn latest_only(mut self) -> Self {
        self.history = HistoryRetention::Latest;
        self
    }

    /// Index only this explicit user-column subset.
    ///
    /// Internal `_id` and `_id_deleted` indexes are always maintained.
    pub fn index_only<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<ColumnName>,
    {
        self.indexed_columns = Some(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Add an ordered multi-column index, such as `(owner_id, updated_at)`.
    pub fn composite_index<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<ColumnName>,
    {
        self.composite_indexes
            .push(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Get the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Build the TableSchema (returns just the schema, not the name).
    pub fn build(self) -> TableSchema {
        TableSchema {
            columns: RowDescriptor::new(self.columns),
            indexed_columns: self.indexed_columns,
            composite_indexes: canonicalize_composite_indexes(self.composite_indexes),
            policies: self.policies,
            branch_by: self.branch_by,
            history: self.history,
        }
    }

    /// Build and return both name and schema (for inserting into Schema map).
    pub fn build_named(self) -> (TableName, TableSchema) {
        let name = TableName::new(&self.name);
        let schema = TableSchema {
            columns: RowDescriptor::new(self.columns),
            indexed_columns: self.indexed_columns,
            composite_indexes: canonicalize_composite_indexes(self.composite_indexes),
            policies: self.policies,
            branch_by: self.branch_by,
            history: self.history,
        };
        (name, schema)
    }
}

/// Builder for creating a complete Schema with multiple tables.
#[derive(Debug, Clone, Default)]
pub struct SchemaBuilder {
    tables: Vec<TableSchemaBuilder>,
}

#[cfg(any(test, feature = "testing"))]
impl crate::model::test_support::AllowAll for SchemaBuilder {
    /// Grant all operations on tables already added to this builder.
    /// Call after adding the tables; replaces their existing policies.
    fn allow_all(mut self) -> Self {
        self.tables = self
            .tables
            .into_iter()
            .map(|table| table.allow_all())
            .collect();
        self
    }
}

impl SchemaBuilder {
    /// Create a new empty schema builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a table builder to the schema.
    pub fn table(mut self, builder: TableSchemaBuilder) -> Self {
        self.tables.push(builder);
        self
    }

    /// Build the complete schema.
    pub fn build(self) -> Schema {
        Schema {
            tables: self.tables.into_iter().map(|t| t.build_named()).collect(),
        }
    }

    /// Compute the schema hash.
    pub fn hash(&self) -> SchemaHash {
        let schema = self.clone().build();
        SchemaHash::compute(&schema)
    }
}

/// Developer-authored schema persisted in the catalogue.
///
/// Tables are stored in name order so serialization and content hashing are
/// deterministic. The compiled runtime representation is derived from this value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Schema {
    tables: BTreeMap<TableName, TableSchema>,
}

impl Schema {
    /// Create an empty schema.
    pub fn new() -> Self {
        Self::default()
    }
}

impl Deref for Schema {
    type Target = BTreeMap<TableName, TableSchema>;

    fn deref(&self) -> &Self::Target {
        &self.tables
    }
}

impl DerefMut for Schema {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.tables
    }
}

impl FromIterator<(TableName, TableSchema)> for Schema {
    fn from_iter<T: IntoIterator<Item = (TableName, TableSchema)>>(iter: T) -> Self {
        Self {
            tables: iter.into_iter().collect(),
        }
    }
}

impl<const N: usize> From<[(TableName, TableSchema); N]> for Schema {
    fn from(tables: [(TableName, TableSchema); N]) -> Self {
        tables.into_iter().collect()
    }
}

impl IntoIterator for Schema {
    type Item = (TableName, TableSchema);
    type IntoIter = std::collections::btree_map::IntoIter<TableName, TableSchema>;

    fn into_iter(self) -> Self::IntoIter {
        self.tables.into_iter()
    }
}

impl<'a> IntoIterator for &'a Schema {
    type Item = (&'a TableName, &'a TableSchema);
    type IntoIter = std::collections::btree_map::Iter<'a, TableName, TableSchema>;

    fn into_iter(self) -> Self::IntoIter {
        self.tables.iter()
    }
}

impl<'a> IntoIterator for &'a mut Schema {
    type Item = (&'a TableName, &'a mut TableSchema);
    type IntoIter = std::collections::btree_map::IterMut<'a, TableName, TableSchema>;

    fn into_iter(self) -> Self::IntoIter {
        self.tables.iter_mut()
    }
}
