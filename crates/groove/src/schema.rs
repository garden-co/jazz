//! SQL DDL-ish schema metadata for record layout and durable indices.
//!
//! This module owns database, table, column, primary-key, foreign-key, and index
//! declarations. It maps declared column types to [`RecordDescriptor`] value
//! types, but it does not encode rows itself; binary layout lives in
//! [`crate::records`]. It also does not plan or maintain indices; the database
//! facade and IVM runtime consume this metadata to create storage keys and
//! durable graph nodes.

use crate::records::{EnumSchema, RecordDescriptor, ValueType};

/// Collection of table and directly exposed record-store schemas known to a database.
///
/// # Examples
///
/// ```
/// use groove::records::{RecordDescriptor, ValueType};
/// use groove::schema::{
///     ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey,
///     DirectRecordStoreSchema, TableSchema,
/// };
///
/// let albums = TableSchema::new(
///     "albums",
///     [
///         ColumnSchema::new("id", ColumnType::U64),
///         ColumnSchema::new("title", ColumnType::String),
///     ],
/// )
/// .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
///
/// let schema =
///     DatabaseSchema::new([albums]).with_direct_record_store(DirectRecordStoreSchema::new(
///         "album_art",
///         RecordDescriptor::new([("album_id", ValueType::U64), ("side", ValueType::String)]),
///         RecordDescriptor::new([("bytes", ValueType::Bytes)]),
///     ));
///
/// assert_eq!(schema.table("albums").unwrap().name, "albums");
/// assert_eq!(
///     schema.direct_record_store("album_art").unwrap().name,
///     "album_art",
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct DatabaseSchema {
    /// Tables are kept in declaration order so future DDL output can be stable.
    pub tables: Vec<TableSchema>,
    /// Direct record stores are opened with tables but are not indexed,
    /// planned, or maintained by the IVM runtime.
    #[serde(default)]
    pub direct_record_stores: Vec<DirectRecordStoreSchema>,
}

impl DatabaseSchema {
    pub fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
            direct_record_stores: Vec::new(),
        }
    }

    /// Add a directly exposed typed record store column family to the schema.
    ///
    /// ```
    /// use groove::records::{RecordDescriptor, ValueType};
    /// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
    ///
    /// let schema = DatabaseSchema::new([]).with_direct_record_store(
    ///     DirectRecordStoreSchema::new(
    ///         "album_art",
    ///         RecordDescriptor::new([("album_id", ValueType::U64)]),
    ///         RecordDescriptor::new([("bytes", ValueType::Bytes)]),
    ///     ),
    /// );
    ///
    /// assert_eq!(schema.column_families(), vec!["album_art"]);
    /// ```
    pub fn with_direct_record_store(mut self, store: DirectRecordStoreSchema) -> Self {
        self.direct_record_stores.push(store);
        self
    }

    pub fn table(&self, name: &str) -> Option<&TableSchema> {
        self.tables.iter().find(|table| table.name == name)
    }

    pub fn direct_record_store(&self, name: &str) -> Option<&DirectRecordStoreSchema> {
        self.direct_record_stores
            .iter()
            .find(|store| store.name == name)
    }

    pub fn column_families(&self) -> Vec<&str> {
        let has_indices = self.tables.iter().any(|table| !table.indices.is_empty());
        let index_family = has_indices.then_some("indices");
        self.tables
            .iter()
            .map(|table| table.name.as_str())
            .chain(
                self.direct_record_stores
                    .iter()
                    .map(|store| store.name.as_str()),
            )
            .chain(index_family)
            .collect()
    }
}

/// Directly exposed typed record-store column family.
///
/// A direct record store has key and value descriptors but no table, secondary
/// index, foreign key, query, or IVM semantics. Callers use it directly through
/// the database facade for ordered typed storage.
///
/// ```
/// use groove::records::{RecordDescriptor, ValueType};
/// use groove::schema::DirectRecordStoreSchema;
///
/// let store = DirectRecordStoreSchema::new(
///     "album_art",
///     RecordDescriptor::new([("album_id", ValueType::U64)]),
///     RecordDescriptor::new([("bytes", ValueType::Bytes)]),
/// );
///
/// assert_eq!(store.name, "album_art");
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct DirectRecordStoreSchema {
    pub name: String,
    pub key: Vec<(String, ValueType)>,
    pub value: Vec<(String, ValueType)>,
}

impl DirectRecordStoreSchema {
    /// Declare a directly exposed typed record-store column family.
    ///
    /// ```
    /// use groove::records::{RecordDescriptor, ValueType};
    /// use groove::schema::{DatabaseSchema, DirectRecordStoreSchema};
    ///
    /// let schema = DatabaseSchema::new([]).with_direct_record_store(
    ///     DirectRecordStoreSchema::new(
    ///         "album_art",
    ///         RecordDescriptor::new([("album_id", ValueType::U64)]),
    ///         RecordDescriptor::new([("bytes", ValueType::Bytes)]),
    ///     ),
    /// );
    ///
    /// assert!(schema.direct_record_store("album_art").is_some());
    /// ```
    pub fn new(name: impl Into<String>, key: RecordDescriptor, value: RecordDescriptor) -> Self {
        Self {
            name: name.into(),
            key: descriptor_fields(&key),
            value: descriptor_fields(&value),
        }
    }

    pub fn key_descriptor(&self) -> RecordDescriptor {
        RecordDescriptor::new(self.key.iter().cloned())
    }

    pub fn value_descriptor(&self) -> RecordDescriptor {
        RecordDescriptor::new(self.value.iter().cloned())
    }
}

fn descriptor_fields(descriptor: &RecordDescriptor) -> Vec<(String, ValueType)> {
    descriptor
        .fields()
        .iter()
        .map(|field| {
            (
                field.name.clone().expect("direct store fields are named"),
                field.value_type.clone(),
            )
        })
        .collect()
}

/// SQL-ish table definition used for encoding and index maintenance.
///
/// # Examples
///
/// ```
/// use groove::schema::{
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
    /// Public write APIs accept values in this declaration order.
    /// [`TableSchema::record_schema`] may reorder fields for compact storage.
    pub columns: Vec<ColumnSchema>,
    pub primary_key: Option<PrimaryKey>,
    /// Explicit secondary indices to maintain as durable IVM nodes.
    pub indices: Vec<IndexSchema>,
    pub foreign_keys: Vec<ForeignKey>,
}

impl TableSchema {
    pub fn new(name: impl Into<String>, columns: impl IntoIterator<Item = ColumnSchema>) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().collect(),
            primary_key: None,
            indices: Vec::new(),
            foreign_keys: Vec::new(),
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

    pub fn record_schema(&self) -> RecordDescriptor {
        RecordDescriptor::new(
            self.columns
                .iter()
                .map(|column| (column.name.clone(), column.column_type.value_type())),
        )
    }
}

/// Column name and type in declaration order.
///
/// # Examples
///
/// ```
/// use groove::schema::{ColumnSchema, ColumnType};
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

/// Type metadata for a declared column.
///
/// # Examples
///
/// ```
/// use groove::records::ValueType;
/// use groove::schema::ColumnType;
///
/// let tags = ColumnType::String.array_of().nullable();
///
/// assert_eq!(
///     tags.value_type(),
///     ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::String))))
/// );
/// ```
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum ColumnType {
    U8,
    U16,
    U32,
    U64,
    F64,
    Bool,
    String,
    Bytes,
    Uuid,
    Enum(EnumSchema),
    /// Fixed-width composite column. All members must be fixed-width; variable
    /// tuple members are reserved for a future extension.
    Tuple(Vec<ColumnType>),
    Array(Box<ColumnType>),
    Nullable(Box<ColumnType>),
}

impl ColumnType {
    pub fn nullable(self) -> Self {
        Self::Nullable(Box::new(self))
    }

    pub fn array_of(self) -> Self {
        Self::Array(Box::new(self))
    }

    pub fn value_type(&self) -> ValueType {
        match self {
            Self::U8 => ValueType::U8,
            Self::U16 => ValueType::U16,
            Self::U32 => ValueType::U32,
            Self::U64 => ValueType::U64,
            Self::F64 => ValueType::F64,
            Self::Bool => ValueType::Bool,
            Self::String => ValueType::String,
            Self::Bytes => ValueType::Bytes,
            Self::Uuid => ValueType::Uuid,
            Self::Enum(schema) => ValueType::Enum(schema.clone()),
            Self::Tuple(members) => {
                ValueType::Tuple(members.iter().map(ColumnType::value_type).collect())
            }
            Self::Array(value_type) => ValueType::Array(Box::new(value_type.value_type())),
            Self::Nullable(value_type) => ValueType::Nullable(Box::new(value_type.value_type())),
        }
    }
}

/// Primary-key metadata for a table.
///
/// # Examples
///
/// ```
/// use groove::schema::{IntegerKeyType, PrimaryKey};
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
/// use groove::schema::{ColumnType, IntegerKeyType, PrimaryKeyColumn};
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
/// use groove::schema::{ColumnType, IntegerKeyType, PrimaryKeyType};
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
/// use groove::schema::{ColumnType, IntegerKeyType};
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
/// use groove::schema::IndexSchema;
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
    fn database_schema_finds_tables_by_name() {
        let schema = DatabaseSchema::new([
            TableSchema::new("albums", [ColumnSchema::new("id", ColumnType::U64)]),
            TableSchema::new("artists", [ColumnSchema::new("id", ColumnType::U64)]),
        ]);

        assert_eq!(schema.table("artists").unwrap().name, "artists");
        assert!(schema.table("tracks").is_none());
    }

    #[test]
    fn column_families_include_indices_family_when_any_table_declares_index() {
        let without_index = DatabaseSchema::new([TableSchema::new(
            "albums",
            [ColumnSchema::new("id", ColumnType::U64)],
        )])
        .with_direct_record_store(DirectRecordStoreSchema::new(
            "streams",
            RecordDescriptor::new([("id", ValueType::String)]),
            RecordDescriptor::new([("bytes", ValueType::Bytes)]),
        ));
        assert_eq!(without_index.column_families(), ["albums", "streams"]);

        let with_index = DatabaseSchema::new([
            TableSchema::new(
                "albums",
                [
                    ColumnSchema::new("id", ColumnType::U64),
                    ColumnSchema::new("title", ColumnType::String),
                ],
            )
            .with_index(IndexSchema::new("albums_by_title", ["title"])),
            TableSchema::new("artists", [ColumnSchema::new("id", ColumnType::U64)]),
        ])
        .with_direct_record_store(DirectRecordStoreSchema::new(
            "streams",
            RecordDescriptor::new([("id", ValueType::String)]),
            RecordDescriptor::new([("bytes", ValueType::Bytes)]),
        ));
        assert_eq!(
            with_index.column_families(),
            ["albums", "artists", "streams", "indices"]
        );
    }

    #[test]
    fn column_types_map_nested_nullables_and_arrays_to_record_value_types() {
        assert_eq!(
            ColumnType::U16.nullable().array_of().value_type(),
            ValueType::Array(Box::new(ValueType::Nullable(Box::new(ValueType::U16))))
        );
        assert_eq!(
            ColumnType::String.array_of().nullable().value_type(),
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
