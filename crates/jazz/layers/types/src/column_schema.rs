//! Table, column, primary-key, foreign-key and index declarations shared by
//! the schema model and storage. Columns and record fields share one
//! [`ValueType`], but this module does not encode rows itself; binary layout
//! lives in [`crate::records`].

use crate::records::{RecordDescriptor, ValueType, VariantRegistry, variant_registry_id_for_path};

/// Schema-facing name for the one logical type space used by both columns and
/// record values. This is an alias, not a conversion boundary.
pub use crate::records::ValueType as ColumnType;

/// SQL-ish table definition used for encoding and index maintenance.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::{
///     ColumnSchema, ColumnType, IndexSchema, IntegerKeyType, PrimaryKey, TableSchema,
/// };
///
/// let albums = TableSchema::new(
///     "albums",
///     [
///         ColumnSchema::new("id", ColumnType::U64),
///         ColumnSchema::new("title", ColumnType::String),
///     ],
/// )
/// .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
/// .with_index(IndexSchema::new("albums_by_title", ["title"]));
///
/// assert_eq!(albums.columns.len(), 2);
/// assert_eq!(albums.indices[0].name, "albums_by_title");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct TableSchema {
    pub name: String,
    /// Durable identity of the hidden whole-row variant registry.
    #[serde(default)]
    pub variant_registry_id: u64,
    /// Independently persisted registries referenced by nested column and
    /// payload descriptors. Keys are durable physical occurrence identities.
    #[serde(default)]
    pub value_variant_registries: std::collections::BTreeMap<u64, VariantRegistry>,
    /// Stable table-wide field catalogue. Schema versions select ordered row
    /// layouts from these fields without redefining their types.
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Option<PrimaryKey>,
    /// Explicit secondary indices.
    pub indices: Vec<IndexSchema>,
    pub foreign_keys: Vec<ForeignKey>,
    /// Top-level enum cases selected by the leading canonical varint stored in
    /// each row. A case owns one immutable dense payload descriptor.
    /// An empty registry denotes one homogeneous layout in catalogue order.
    /// Value-based writes use the reserved discriminator `0`; callers may bind
    /// the same layout to another discriminator when it is useful metadata.
    #[serde(default)]
    pub variants: Vec<TableVariant>,
}

#[cfg(test)]
thread_local! {
    pub(crate) static VARIANT_DESCRIPTOR_BUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: impl IntoIterator<Item = ColumnSchema>) -> Self {
        Self::new_inner(name, columns, false)
    }

    /// Construct a physical table whose column types were already rebound to
    /// durable registry identities by an owning catalogue.
    #[doc(hidden)]
    pub fn new_with_bound_registries(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = ColumnSchema>,
    ) -> Self {
        Self::new_inner(name, columns, true)
    }

    fn new_inner(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = ColumnSchema>,
        preserve_bound_registries: bool,
    ) -> Self {
        let name = name.into();
        let columns: Vec<ColumnSchema> = columns
            .into_iter()
            .map(|mut column| {
                let path = format!("table/{name}/column/{}", column.name);
                column.column_type = if preserve_bound_registries {
                    column.column_type.stamp_variant_registries(&path)
                } else {
                    column.column_type.rebind_variant_registries(&path)
                };
                column
            })
            .collect();
        let mut value_variant_registries = std::collections::BTreeMap::new();
        for column in &columns {
            let mut registries = std::collections::BTreeMap::new();
            column
                .column_type
                .collect_variant_registries(&mut registries);
            assert_eq!(
                registries.len(),
                column.column_type.variant_registry_occurrence_count(),
                "variant registry identity collision within column {}",
                column.name
            );
            for (id, registry) in registries {
                assert!(
                    value_variant_registries.insert(id, registry).is_none(),
                    "variant registry identity collision across columns"
                );
            }
        }
        Self {
            variant_registry_id: variant_registry_id_for_path(&format!("table/{name}/row")),
            value_variant_registries,
            name,
            columns,
            primary_key: None,
            indices: Vec::new(),
            foreign_keys: Vec::new(),
            variants: Vec::new(),
        }
    }

    pub fn with_primary_key(mut self, primary_key: PrimaryKey) -> Self {
        self.primary_key = Some(primary_key);
        self
    }

    pub fn with_index(mut self, index: IndexSchema) -> Self {
        self.indices.push(index);
        self
    }

    pub fn with_foreign_key(mut self, foreign_key: ForeignKey) -> Self {
        self.foreign_keys.push(foreign_key);
        self
    }

    /// Register one generic top-level enum case.
    ///
    /// The case tag is table-local and has no schema-version meaning to
    /// storage. Jazz allocates cases for physical schema layouts.
    pub fn with_variant(
        mut self,
        tag: u32,
        fields: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.variants.push(TableVariant::new(tag, fields));
        self
    }

    /// Register a case with a case-local payload descriptor. Fields only
    /// participate in table-wide keys/indices when they explicitly name a
    /// shared column identity.
    pub fn with_variant_payload(
        mut self,
        tag: u32,
        fields: impl IntoIterator<Item = TableVariantField>,
    ) -> Self {
        let fields = fields.into_iter().map(|mut field| {
            let path = format!(
                "{}/case/{tag}/field/{}",
                self.variant_registry_id, field.name
            );
            field.value_type = if field.shared_column.is_some() {
                field.value_type.stamp_variant_registries(&path)
            } else {
                field.value_type.rebind_variant_registries(&path)
            };
            field
        });
        let fields = fields.collect::<Vec<_>>();
        for field in &fields {
            let mut registries = std::collections::BTreeMap::new();
            field.value_type.collect_variant_registries(&mut registries);
            assert_eq!(
                registries.len(),
                field.value_type.variant_registry_occurrence_count(),
                "variant registry identity collision within payload field {}",
                field.name
            );
            for (id, registry) in registries {
                if let Some(existing) = self.value_variant_registries.get(&id) {
                    assert_eq!(existing, &registry, "variant registry definition collision");
                } else {
                    self.value_variant_registries.insert(id, registry);
                }
            }
        }
        self.variants.push(TableVariant::with_payload(tag, fields));
        self
    }

    pub fn has_variants(&self) -> bool {
        !self.variants.is_empty()
    }

    pub fn variant(&self, tag: u32) -> Option<&TableVariant> {
        self.variants.iter().find(|variant| variant.tag == tag)
    }

    /// Whether any column or variant payload field of this table is interned.
    pub fn has_interned_columns(&self) -> bool {
        self.columns
            .iter()
            .any(|column| column.column_type.contains_interned())
            || self.variants.iter().any(|variant| {
                variant
                    .payload_fields
                    .iter()
                    .any(|field| field.value_type.contains_interned())
            })
    }

    /// The descriptor of one whole-row enum case. It is both the stored
    /// layout and what reads and writes see: an interned field keeps its
    /// [`ValueType::interned`] type and holds its dictionary code, which is
    /// resolved to its value only on demand.
    pub fn record_schema_for_variant(&self, tag: u32) -> Option<RecordDescriptor> {
        #[cfg(test)]
        VARIANT_DESCRIPTOR_BUILDS.with(|count| count.set(count.get() + 1));
        if self.variants.is_empty() {
            return Some(self.record_schema());
        }
        let variant = self.variant(tag)?;
        let fields = if variant.payload_fields.is_empty() {
            variant
                .fields
                .iter()
                .map(|field_name| {
                    let column = self
                        .columns
                        .iter()
                        .find(|column| column.name == *field_name)?;
                    Some((column.name.clone(), column.column_type.clone()))
                })
                .collect::<Option<Vec<_>>>()?
        } else {
            variant
                .payload_fields
                .iter()
                .map(|field| (field.name.clone(), field.value_type.clone()))
                .collect()
        };
        Some(RecordDescriptor::new(fields))
    }

    /// The descriptor of this table's full column catalogue, stored and seen
    /// alike.
    pub fn record_schema(&self) -> RecordDescriptor {
        RecordDescriptor::new(
            self.columns
                .iter()
                .map(|column| (column.name.clone(), column.column_type.clone())),
        )
    }
}

/// One top-level table enum case and its ordered dense payload layout.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct TableVariant {
    pub tag: u32,
    pub fields: Vec<String>,
    #[serde(default)]
    pub payload_fields: Vec<TableVariantField>,
}

impl TableVariant {
    pub fn new(tag: u32, fields: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            tag,
            fields: fields.into_iter().map(Into::into).collect(),
            payload_fields: Vec::new(),
        }
    }

    pub fn with_payload(tag: u32, fields: impl IntoIterator<Item = TableVariantField>) -> Self {
        Self {
            tag,
            fields: Vec::new(),
            payload_fields: fields.into_iter().collect(),
        }
    }

    pub fn payload_name_for_shared(&self, shared: &str) -> Option<&str> {
        if self.payload_fields.is_empty() {
            return self
                .fields
                .iter()
                .find(|field| field.as_str() == shared)
                .map(String::as_str);
        }
        self.payload_fields
            .iter()
            .find(|field| field.shared_column.as_deref() == Some(shared))
            .map(|field| field.name.as_str())
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct TableVariantField {
    pub name: String,
    pub value_type: ValueType,
    /// Optional table-wide physical identity used for keys and indices.
    pub shared_column: Option<String>,
}

impl TableVariantField {
    pub fn local(name: impl Into<String>, value_type: ValueType) -> Self {
        Self {
            name: name.into(),
            value_type,
            shared_column: None,
        }
    }

    pub fn shared(
        name: impl Into<String>,
        value_type: ValueType,
        shared_column: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value_type,
            shared_column: Some(shared_column.into()),
        }
    }
}

/// Column name and type in declaration order.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::{ColumnSchema, ColumnType};
///
/// let title = ColumnSchema::new("title", ColumnType::String);
///
/// assert_eq!(title.name, "title");
/// assert_eq!(title.column_type, ColumnType::String);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct ColumnSchema {
    pub name: String,
    pub column_type: ColumnType,
}

impl ColumnSchema {
    pub fn new(name: impl Into<String>, column_type: ColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
        }
    }
}

/// Primary-key metadata for a table.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::{IntegerKeyType, PrimaryKey};
///
/// let primary_key = PrimaryKey::new("id", IntegerKeyType::U64);
///
/// assert!(primary_key.generated);
/// assert_eq!(primary_key.columns[0].column, "id");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct PrimaryKey {
    pub columns: Vec<PrimaryKeyColumn>,
    /// Records whether the key is intended to be generated by the database.
    /// Generation is metadata-only for now; callers still provide key values.
    pub generated: bool,
}

impl PrimaryKey {
    pub fn new(column: impl Into<String>, integer_type: IntegerKeyType) -> Self {
        Self {
            columns: vec![PrimaryKeyColumn::integer(column, integer_type)],
            generated: true,
        }
    }

    /// Defines a primary key from one or more typed key parts.
    pub fn composite(columns: impl IntoIterator<Item = PrimaryKeyColumn>) -> Self {
        Self {
            columns: columns.into_iter().collect(),
            generated: false,
        }
    }

    pub fn user_supplied(mut self) -> Self {
        self.generated = false;
        self
    }
}

/// A typed column that participates in a composite primary key.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::{ColumnType, IntegerKeyType, PrimaryKeyColumn};
///
/// let key_column = PrimaryKeyColumn::integer("tenant_id", IntegerKeyType::U64);
///
/// assert_eq!(key_column.column, "tenant_id");
/// assert_eq!(key_column.key_type.column_type(), ColumnType::U64);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct PrimaryKeyColumn {
    pub column: String,
    pub key_type: PrimaryKeyType,
}

impl PrimaryKeyColumn {
    pub fn new(column: impl Into<String>, key_type: PrimaryKeyType) -> Self {
        Self {
            column: column.into(),
            key_type,
        }
    }

    pub fn integer(column: impl Into<String>, integer_type: IntegerKeyType) -> Self {
        Self::new(column, PrimaryKeyType::Integer(integer_type))
    }

    pub fn bytes(column: impl Into<String>) -> Self {
        Self::new(column, PrimaryKeyType::Bytes)
    }

    pub fn uuid(column: impl Into<String>) -> Self {
        Self::new(column, PrimaryKeyType::Uuid)
    }
}

/// Type metadata for a primary-key column.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::{ColumnType, IntegerKeyType, PrimaryKeyType};
///
/// let key_type = PrimaryKeyType::Integer(IntegerKeyType::U64);
///
/// assert_eq!(key_type.column_type(), ColumnType::U64);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum PrimaryKeyType {
    Integer(IntegerKeyType),
    Bool,
    String,
    Bytes,
    Uuid,
}

impl PrimaryKeyType {
    pub fn column_type(&self) -> ColumnType {
        match self {
            Self::Integer(integer_type) => integer_type.column_type(),
            Self::Bool => ColumnType::Bool,
            Self::String => ColumnType::String,
            Self::Bytes => ColumnType::Bytes,
            Self::Uuid => ColumnType::Uuid,
        }
    }
}

/// Integer widths supported by generated primary keys.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::{ColumnType, IntegerKeyType};
///
/// assert_eq!(IntegerKeyType::U64.column_type(), ColumnType::U64);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum IntegerKeyType {
    U8,
    U16,
    U32,
    U64,
}

impl IntegerKeyType {
    pub fn column_type(&self) -> ColumnType {
        match self {
            Self::U8 => ColumnType::U8,
            Self::U16 => ColumnType::U16,
            Self::U32 => ColumnType::U32,
            Self::U64 => ColumnType::U64,
        }
    }
}

/// Explicit secondary index metadata.
///
/// # Examples
///
/// ```
/// use jazz_types::column_schema::IndexSchema;
///
/// let index = IndexSchema::new("albums_by_title", ["title"]);
///
/// assert_eq!(index.columns, ["title"]);
/// assert!(!index.unique);
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct IndexSchema {
    pub name: String,
    /// Column names, not descriptor positions. Runtime lowering resolves these
    /// after record layout canonicalization.
    pub columns: Vec<String>,
    pub unique: bool,
}

impl IndexSchema {
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            unique: false,
        }
    }

    pub fn unique(mut self) -> Self {
        self.unique = true;
        self
    }
}

/// Foreign-key metadata retained for future validation/planning.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct ForeignKey {
    pub name: String,
    pub columns: Vec<String>,
    pub referenced_table: String,
    pub referenced_columns: Vec<String>,
}

impl ForeignKey {
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<String>>,
        referenced_table: impl Into<String>,
        referenced_columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            referenced_table: referenced_table.into(),
            referenced_columns: referenced_columns.into_iter().map(Into::into).collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn column_types_map_nested_nullables_and_arrays_to_record_value_types() {
        assert_eq!(
            ColumnType::U16.nullable().array_of().clone(),
            ValueType::Array(Box::new(ValueType::Nullable(Box::new(ValueType::U16))))
        );
        assert_eq!(
            ColumnType::String.array_of().nullable().clone(),
            ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::String))))
        );
    }

    #[test]
    fn builders_preserve_key_index_and_foreign_key_metadata() {
        let table = TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("artist_id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64).user_supplied())
        .with_index(IndexSchema::new("albums_by_artist_title", ["artist_id", "title"]).unique())
        .with_foreign_key(ForeignKey::new(
            "albums_artist_fk",
            ["artist_id"],
            "artists",
            ["id"],
        ));

        let primary_key = table.primary_key.as_ref().unwrap();
        assert_eq!(primary_key.columns[0].column, "id");
        assert_eq!(
            primary_key.columns[0].key_type.column_type(),
            ColumnType::U64
        );
        assert!(!primary_key.generated);
        assert_eq!(table.indices[0].columns, ["artist_id", "title"]);
        assert!(table.indices[0].unique);
        assert_eq!(table.foreign_keys[0].columns, ["artist_id"]);
        assert_eq!(table.foreign_keys[0].referenced_table, "artists");
        assert_eq!(table.foreign_keys[0].referenced_columns, ["id"]);
    }

    #[test]
    fn primary_keys_can_cover_multiple_columns() {
        let primary_key = PrimaryKey::composite([
            PrimaryKeyColumn::bytes("row_uuid"),
            PrimaryKeyColumn::integer("tx_local_epoch", IntegerKeyType::U64),
        ]);

        assert!(!primary_key.generated);
        assert_eq!(primary_key.columns[0].column, "row_uuid");
        assert_eq!(
            primary_key.columns[0].key_type.column_type(),
            ColumnType::Bytes
        );
        assert_eq!(primary_key.columns[1].column, "tx_local_epoch");
        assert_eq!(
            primary_key.columns[1].key_type.column_type(),
            ColumnType::U64
        );
    }

    #[test]
    fn table_schema_maps_columns_to_record_schema() {
        let table = TableSchema::new(
            "albums",
            [
                ColumnSchema::new("id", ColumnType::U64),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("rating", ColumnType::F64.nullable()),
            ],
        )
        .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
        .with_index(IndexSchema::new("albums_by_title", ["title"]))
        .with_foreign_key(ForeignKey::new(
            "albums_artist_fk",
            ["artist_id"],
            "artists",
            ["id"],
        ));

        let descriptor = table.record_schema();
        let types = descriptor
            .fields()
            .iter()
            .map(|field| field.value_type.clone())
            .collect::<Vec<_>>();

        assert_eq!(
            types,
            [
                ValueType::U64,
                ValueType::String,
                ValueType::Nullable(Box::new(ValueType::F64)),
            ]
        );
        assert_eq!(table.primary_key.unwrap().columns[0].column, "id");
        assert_eq!(table.indices[0].columns, ["title"]);
        assert_eq!(table.foreign_keys[0].referenced_table, "artists");
    }
}
