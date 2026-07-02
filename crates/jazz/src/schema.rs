//! Jazz schema metadata and lowering to groove storage schemas. This module
//! owns table/column declarations, merge-strategy declarations, policy metadata,
//! storage-table naming, and migration-lens schema surfaces from
//! `jazz/SPEC/10_lenses_migrations.md`;
//! policy evaluation lives in [`crate::node::policy`], query shapes in
//! [`crate::query`], and runtime catalogue ingestion in [`crate::node::ingest`].
//! In the layer map it is the schema bridge from Jazz concepts to groove tables.

use std::collections::{BTreeMap, BTreeSet};

use groove::records::{EnumSchema, RecordDescriptor, ValueType};
use groove::schema::{
    ColumnType as GrooveColumnType, DatabaseSchema as GrooveDatabaseSchema,
    DirectRecordStoreSchema, IndexSchema as GrooveIndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, TableSchema as GrooveTableSchema,
};

use crate::ids::{BranchId, SchemaVersionId};
use crate::query::{Query, claim, col, eq};

/// Namespace used for schema-version UUIDv5 ids.
pub const SCHEMA_VERSION_NAMESPACE: uuid::Uuid =
    uuid::uuid!("61b9ef21-3195-50e8-87fc-2aa83a6f74e3");

/// Direct groove record store used for append-only large-value byte extents.
pub const CONTENT_EXTENTS_STORE: &str = "jazz_content_extents";
/// Direct groove record store used for large-value stream tails.
pub const CONTENT_META_STORE: &str = "jazz_content_meta";
/// Direct groove record store used for local large-value checkpoints.
pub const CONTENT_CHECKPOINTS_STORE: &str = "jazz_content_checkpoints";
/// Direct groove record store used for persisted fast known-state facts.
pub const KNOWN_STATE_FACTS_STORE: &str = "jazz_known_state_facts";

/// Complete logical Jazz schema.
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct JazzSchema {
    /// Application tables in the schema.
    pub tables: Vec<TableSchema>,
    /// Read policy for branch metadata rows. `None` means branch metadata is
    /// public for reads.
    #[serde(default)]
    pub branch_read_policy: Option<Query>,
    /// Write policy for branch metadata rows. `None` means branch metadata is
    /// public for writes.
    #[serde(default)]
    pub branch_write_policy: Option<Query>,
}

impl JazzSchema {
    /// Construct a schema from application tables.
    pub fn new(tables: impl IntoIterator<Item = TableSchema>) -> Self {
        Self {
            tables: tables.into_iter().collect(),
            branch_read_policy: None,
            branch_write_policy: None,
        }
        .validated()
    }

    /// Set the read policy for branch metadata rows.
    pub fn with_branch_read_policy(mut self, read_policy: impl Into<Option<Query>>) -> Self {
        self.branch_read_policy = read_policy.into();
        self.validated()
    }

    /// Set the write policy for branch metadata rows.
    pub fn with_branch_write_policy(mut self, write_policy: impl Into<Option<Query>>) -> Self {
        self.branch_write_policy = write_policy.into();
        self.validated()
    }

    fn validated(self) -> Self {
        for table in &self.tables {
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
                            column.large_value.is_none(),
                            "counter merge strategy cannot be used with a large-value column: {}.{}",
                            table.name,
                            column.name
                        );
                        assert!(
                            is_counter_column_type(&column.column_type),
                            "counter merge strategy requires a non-nullable integer column: {}.{}",
                            table.name,
                            column.name
                        );
                    }
                }
            }
            if let Some(policy) = &table.read_policy {
                assert_eq!(policy.table, table.name, "read policy table must match");
                policy.validate(&self).expect("valid read policy shape");
            }
            for (label, policy) in table.write_policies.iter() {
                assert_eq!(
                    policy.table, table.name,
                    "{label} write policy table must match"
                );
                policy
                    .validate(&self)
                    .unwrap_or_else(|_| panic!("valid {label} write policy shape"));
            }
        }
        if let Some(policy) = &self.branch_read_policy {
            assert_eq!(
                policy.table, "jazz_branches",
                "branch read policy table must be jazz_branches"
            );
            policy
                .validate(&self)
                .expect("valid branch read policy shape");
        }
        if let Some(policy) = &self.branch_write_policy {
            assert_eq!(
                policy.table, "jazz_branches",
                "branch write policy table must be jazz_branches"
            );
            policy
                .validate(&self)
                .expect("valid branch write policy shape");
        }
        self
    }

    /// Lower the Jazz schema into groove storage tables.
    pub fn lower_to_groove(&self) -> GrooveDatabaseSchema {
        self.with_jazz_direct_record_stores(GrooveDatabaseSchema::new(self.storage_tables()))
    }

    /// Lower the schema plus registered schema-version partitions.
    pub fn lower_to_groove_with_partitions(
        &self,
        catalogue_schemas: &BTreeMap<SchemaVersionId, crate::protocol::SchemaVersion>,
        partitions: &std::collections::BTreeSet<(String, SchemaVersionId)>,
        branch_partitions: &std::collections::BTreeSet<(String, SchemaVersionId, BranchId)>,
    ) -> GrooveDatabaseSchema {
        let mut tables = self.storage_tables();
        let base_id = self.version_id();
        for (logical_table, schema_version) in partitions {
            if *schema_version == base_id {
                continue;
            }
            let Some(schema) = catalogue_schemas.get(schema_version) else {
                continue;
            };
            let Some(table) = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == *logical_table)
            else {
                continue;
            };
            tables.push(table.history_partition_storage_table(*schema_version));
            tables.push(table.register_partition_storage_table(*schema_version));
        }
        for (logical_table, schema_version, branch_id) in branch_partitions {
            let Some(schema) = catalogue_schemas.get(schema_version) else {
                continue;
            };
            let Some(table) = schema
                .schema
                .tables
                .iter()
                .find(|table| table.name == *logical_table)
            else {
                continue;
            };
            tables.push(table.branch_history_partition_storage_table(*schema_version, *branch_id));
            tables.push(table.branch_register_partition_storage_table(*schema_version, *branch_id));
        }
        self.with_jazz_direct_record_stores(GrooveDatabaseSchema::new(tables))
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
        lowered
            .column_families()
            .into_iter()
            .chain(std::iter::once("indices"))
            .map(str::to_owned)
            .collect()
    }

    /// Return all storage tables used by Jazz.
    pub fn storage_tables(&self) -> Vec<GrooveTableSchema> {
        let mut tables = vec![
            nodes_table(),
            schema_versions_table(),
            catalogue_table(),
            catalogue_pointer_table(),
            partitions_table(),
            branch_partitions_table(),
            branches_table(),
            transactions_table(),
            rejected_transactions_table(),
            pending_edges_table(),
        ];
        tables.extend(
            self.tables
                .iter()
                .map(TableSchema::rejected_versions_storage_table),
        );
        tables.extend(self.tables.iter().map(TableSchema::history_storage_table));
        tables.extend(self.tables.iter().map(TableSchema::register_storage_table));
        tables.extend(
            self.tables
                .iter()
                .flat_map(TableSchema::global_current_storage_tables),
        );
        tables.extend(
            self.tables
                .iter()
                .flat_map(TableSchema::ahead_current_storage_tables),
        );
        tables.push(global_changes_table());
        tables
    }

    /// Return the version-independent metadata tables available before partitions are known.
    pub fn catalogue_meta_storage_tables(&self) -> Vec<GrooveTableSchema> {
        vec![
            nodes_table(),
            schema_versions_table(),
            catalogue_table(),
            catalogue_pointer_table(),
            partitions_table(),
            branch_partitions_table(),
            branches_table(),
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
                CONTENT_EXTENTS_STORE,
                RecordDescriptor::new([
                    ("writer", ValueType::Uuid),
                    ("row", ValueType::Uuid),
                    ("column", ValueType::String),
                    ("offset", ValueType::U64),
                ]),
                RecordDescriptor::new([("bytes", ValueType::Bytes)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                CONTENT_META_STORE,
                RecordDescriptor::new([
                    ("writer", ValueType::Uuid),
                    ("row", ValueType::Uuid),
                    ("column", ValueType::String),
                ]),
                RecordDescriptor::new([("offset", ValueType::U64)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                CONTENT_CHECKPOINTS_STORE,
                RecordDescriptor::new([
                    ("table", ValueType::String),
                    ("row", ValueType::Uuid),
                    ("column", ValueType::String),
                    ("version_time", ValueType::U64),
                    ("version_node", ValueType::Uuid),
                ]),
                RecordDescriptor::new([("bytes", ValueType::Bytes)]),
            ))
            .with_direct_record_store(DirectRecordStoreSchema::new(
                KNOWN_STATE_FACTS_STORE,
                RecordDescriptor::new([
                    ("shape_id", ValueType::Uuid),
                    ("binding_id", ValueType::Uuid),
                    ("read_view_id", ValueType::Uuid),
                ]),
                RecordDescriptor::new([("settled_through", ValueType::U64)]),
            ))
    }
}

pub(crate) fn branch_metadata_table_schema() -> TableSchema {
    TableSchema::new(
        "jazz_branches",
        [
            ColumnSchema::new("branch_id", GrooveColumnType::Uuid),
            ColumnSchema::new("parent", GrooveColumnType::Uuid.nullable()),
            ColumnSchema::new("base_global", GrooveColumnType::U64.nullable()),
            ColumnSchema::new("state", GrooveColumnType::String),
        ],
    )
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
}

fn is_counter_column_type(column_type: &GrooveColumnType) -> bool {
    matches!(
        column_type,
        GrooveColumnType::U8
            | GrooveColumnType::U16
            | GrooveColumnType::U32
            | GrooveColumnType::U64
    )
}

/// Jazz-level large-value column kind.
///
/// Groove stores these as opaque [`GrooveColumnType::Bytes`]. Jazz owns the
/// large-value semantics; this slice stores and merges the whole byte payload.
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Deserialize, serde::Serialize)]
pub enum LargeValueKind {
    /// Editable text stored as opaque bytes in groove for this slice.
    Text,
    /// Binary large object stored as opaque bytes in groove.
    Blob,
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
    /// Jazz-level large-value marker for opaque text/blob columns.
    #[serde(default)]
    pub large_value: Option<LargeValueKind>,
}

impl ColumnSchema {
    /// Construct an ordinary column from a groove storage type.
    pub fn new(name: impl Into<String>, column_type: GrooveColumnType) -> Self {
        Self {
            name: name.into(),
            column_type,
            large_value: None,
        }
    }

    /// Construct a Jazz text column stored in groove as opaque bytes.
    pub fn text(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: GrooveColumnType::Bytes,
            large_value: Some(LargeValueKind::Text),
        }
    }

    /// Construct a Jazz blob column stored in groove as opaque bytes.
    pub fn blob(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            column_type: GrooveColumnType::Bytes,
            large_value: Some(LargeValueKind::Blob),
        }
    }
}

impl From<groove::schema::ColumnSchema> for ColumnSchema {
    fn from(column: groove::schema::ColumnSchema) -> Self {
        Self {
            name: column.name,
            column_type: column.column_type,
            large_value: None,
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
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub struct TableSchema {
    /// Logical table name.
    pub name: String,
    /// User columns.
    pub columns: Vec<ColumnSchema>,
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
    pub fn new(
        name: impl Into<String>,
        columns: impl IntoIterator<Item = impl Into<ColumnSchema>>,
    ) -> Self {
        Self {
            name: name.into(),
            columns: columns.into_iter().map(Into::into).collect(),
            references: BTreeMap::new(),
            read_policy: None,
            write_policies: WritePolicies::default(),
            indexed_columns: BTreeSet::new(),
            merge_strategies: BTreeMap::new(),
        }
    }

    /// Mark a user column as referencing another Jazz table.
    pub fn with_reference(
        mut self,
        column: impl Into<String>,
        target_table: impl Into<String>,
    ) -> Self {
        self.references.insert(column.into(), target_table.into());
        self
    }

    /// Mark a user column as indexed on the global-current content table.
    pub fn with_indexed_column(mut self, column: impl Into<String>) -> Self {
        self.indexed_columns.insert(column.into());
        self
    }

    /// Mark user columns as indexed on the global-current content table.
    pub fn with_indexed_columns(
        mut self,
        columns: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        self.indexed_columns
            .extend(columns.into_iter().map(Into::into));
        self
    }

    /// Set a user column's merge strategy.
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
    pub fn with_read_policy(mut self, read_policy: impl Into<Option<Query>>) -> Self {
        self.read_policy = read_policy.into();
        self
    }

    /// Set the table write policy.
    pub fn with_write_policy(mut self, write_policy: impl Into<Option<Query>>) -> Self {
        self.write_policies = WritePolicies::legacy(write_policy.into());
        self
    }

    /// Set operation-specific write policies.
    pub fn with_write_policies(mut self, write_policies: WritePolicies) -> Self {
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

    /// Return a partitioned storage history table with an explicit physical name.
    pub fn history_partition_storage_table(
        &self,
        schema_version: SchemaVersionId,
    ) -> GrooveTableSchema {
        self.history_storage_table_named(partition_history_table_name(&self.name, schema_version))
    }

    /// Return a branch-overlay partitioned storage history table.
    pub fn branch_history_partition_storage_table(
        &self,
        schema_version: SchemaVersionId,
        branch_id: BranchId,
    ) -> GrooveTableSchema {
        self.history_storage_table_named(branch_partition_history_table_name(
            &self.name,
            schema_version,
            branch_id,
        ))
    }

    fn history_storage_table_named(&self, name: String) -> GrooveTableSchema {
        let mut columns = vec![
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

        GrooveTableSchema::new(name, columns)
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(GrooveIndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "row_uuid"],
            ))
    }

    /// Return the storage table for deletion-register versions.
    pub fn register_storage_table(&self) -> GrooveTableSchema {
        self.register_storage_table_named(format!("jazz_{}_register", self.name))
    }

    /// Return a partitioned deletion-register table with an explicit physical name.
    pub fn register_partition_storage_table(
        &self,
        schema_version: SchemaVersionId,
    ) -> GrooveTableSchema {
        self.register_storage_table_named(partition_register_table_name(&self.name, schema_version))
    }

    /// Return a branch-overlay partitioned deletion-register table.
    pub fn branch_register_partition_storage_table(
        &self,
        schema_version: SchemaVersionId,
        branch_id: BranchId,
    ) -> GrooveTableSchema {
        self.register_storage_table_named(branch_partition_register_table_name(
            &self.name,
            schema_version,
            branch_id,
        ))
    }

    fn register_storage_table_named(&self, name: String) -> GrooveTableSchema {
        GrooveTableSchema::new(
            name,
            [
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
            PrimaryKeyColumn::uuid("row_uuid"),
            PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
        ]))
        .with_index(GrooveIndexSchema::new(
            "by_tx",
            ["tx_time", "tx_node_id", "row_uuid"],
        ))
    }

    /// Return per-layer global-current tables for content and register winners.
    pub fn global_current_storage_tables(&self) -> Vec<GrooveTableSchema> {
        let indexed_columns = self.global_current_indexed_columns();
        let mut content_columns = vec![
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", GrooveColumnType::Uuid),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", GrooveColumnType::Uuid),
            column("updated_at", GrooveColumnType::U64),
            column("global_seq", GrooveColumnType::U64.nullable()),
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
        let mut content_table = GrooveTableSchema::new(
            format!("jazz_{}_global_current", self.name),
            content_columns,
        )
        .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")]));
        for indexed in &indexed_columns {
            content_table = content_table.with_index(GrooveIndexSchema::new(
                global_current_index_name(indexed),
                [format!("user_{indexed}")],
            ));
        }
        vec![
            content_table,
            GrooveTableSchema::new(
                format!("jazz_{}_register_global_current", self.name),
                [
                    column("row_uuid", GrooveColumnType::Uuid),
                    column("tx_time", GrooveColumnType::U64),
                    column("tx_node_id", GrooveColumnType::U64),
                    column("schema_version", GrooveColumnType::U64),
                    column("parents", tx_id_column().array_of()),
                    column("created_by", GrooveColumnType::Uuid),
                    column("created_at", GrooveColumnType::U64),
                    column("updated_by", GrooveColumnType::Uuid),
                    column("updated_at", GrooveColumnType::U64),
                    column("global_seq", GrooveColumnType::U64.nullable()),
                    column("_deletion", deletion_column()),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")])),
        ]
    }

    /// Return per-layer ahead-of-global candidate tables.
    pub fn ahead_current_storage_tables(&self) -> Vec<GrooveTableSchema> {
        let mut content_columns = vec![
            column("row_uuid", GrooveColumnType::Uuid),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("schema_version", GrooveColumnType::U64),
            column("parents", tx_id_column().array_of()),
            column("created_by", GrooveColumnType::Uuid),
            column("created_at", GrooveColumnType::U64),
            column("updated_by", GrooveColumnType::Uuid),
            column("updated_at", GrooveColumnType::U64),
            column("global_seq", GrooveColumnType::U64.nullable()),
        ];
        content_columns.extend(self.columns.iter().map(|user_column| {
            column(
                format!("user_{}", user_column.name),
                user_column.column_type.clone().nullable(),
            )
        }));
        vec![
            GrooveTableSchema::new(format!("jazz_{}_ahead_current", self.name), content_columns)
                .with_primary_key(PrimaryKey::composite([
                    PrimaryKeyColumn::uuid("row_uuid"),
                    PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                    PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
                ]))
                .with_index(GrooveIndexSchema::new(
                    "by_tx",
                    ["tx_time", "tx_node_id", "row_uuid"],
                )),
            GrooveTableSchema::new(
                format!("jazz_{}_register_ahead_current", self.name),
                [
                    column("row_uuid", GrooveColumnType::Uuid),
                    column("tx_time", GrooveColumnType::U64),
                    column("tx_node_id", GrooveColumnType::U64),
                    column("schema_version", GrooveColumnType::U64),
                    column("parents", tx_id_column().array_of()),
                    column("created_by", GrooveColumnType::Uuid),
                    column("created_at", GrooveColumnType::U64),
                    column("updated_by", GrooveColumnType::Uuid),
                    column("updated_at", GrooveColumnType::U64),
                    column("global_seq", GrooveColumnType::U64.nullable()),
                    column("_deletion", deletion_column()),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(GrooveIndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "row_uuid"],
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
                    ValueType::Array(Box::new(tx_id_column().value_type())),
                ),
                ("created_by".to_owned(), ValueType::Uuid),
                ("created_at".to_owned(), ValueType::U64),
                ("updated_by".to_owned(), ValueType::Uuid),
                ("updated_at".to_owned(), ValueType::U64),
                (
                    "_deletion".to_owned(),
                    ValueType::Nullable(Box::new(deletion_column().value_type())),
                ),
            ]
            .into_iter()
            .chain(self.columns.iter().map(|column| {
                (
                    format!("user_{}", column.name),
                    ValueType::Nullable(Box::new(column.column_type.clone().value_type())),
                )
            })),
        )
    }
}

pub(crate) fn partition_history_table_name(table: &str, schema_version: SchemaVersionId) -> String {
    format!("jazz_{table}_{}_history", schema_version.0.simple())
}

pub(crate) fn partition_register_table_name(
    table: &str,
    schema_version: SchemaVersionId,
) -> String {
    format!("jazz_{table}_{}_register", schema_version.0.simple())
}

pub(crate) fn branch_partition_history_table_name(
    table: &str,
    schema_version: SchemaVersionId,
    branch_id: BranchId,
) -> String {
    format!(
        "jazz_{table}_branch_{}_{}_history",
        branch_id.0.simple(),
        schema_version.0.simple()
    )
}

pub(crate) fn branch_partition_register_table_name(
    table: &str,
    schema_version: SchemaVersionId,
    branch_id: BranchId,
) -> String {
    format!(
        "jazz_{table}_branch_{}_{}_register",
        branch_id.0.simple(),
        schema_version.0.simple()
    )
}

fn schema_versions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_schema_versions",
        [
            // node-local-derived: allocated by schema-version alias interning.
            column("id", GrooveColumnType::U64),
            column("uuid", GrooveColumnType::Uuid),
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

fn partitions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_partitions",
        [
            column("table_name", GrooveColumnType::Bytes),
            column("schema_version", GrooveColumnType::Uuid),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("table_name"),
        PrimaryKeyColumn::uuid("schema_version"),
    ]))
}

fn branch_partitions_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_branch_partitions",
        [
            column("table_name", GrooveColumnType::Bytes),
            column("schema_version", GrooveColumnType::Uuid),
            column("branch_id", GrooveColumnType::Uuid),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("table_name"),
        PrimaryKeyColumn::uuid("schema_version"),
        PrimaryKeyColumn::uuid("branch_id"),
    ]))
}

fn branches_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_branches",
        [
            column("branch_id", GrooveColumnType::Uuid),
            column("parent", GrooveColumnType::Uuid.nullable()),
            column("base_global", GrooveColumnType::U64.nullable()),
            column(
                "state",
                storage_enum("jazz_branch_state", &["open", "merged", "discarded"]),
            ),
        ],
    )
    .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("branch_id")]))
}

fn global_changes_table() -> GrooveTableSchema {
    GrooveTableSchema::new(
        "jazz_global_changes",
        [
            column("table_name", GrooveColumnType::Bytes),
            column("row_uuid", GrooveColumnType::Uuid),
            column("layer", GrooveColumnType::Bytes),
            column("global_seq", GrooveColumnType::U64),
            column("tx_time", GrooveColumnType::U64),
            column("tx_node_id", GrooveColumnType::U64),
            column("_deletion", deletion_column().nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("table_name"),
        PrimaryKeyColumn::uuid("row_uuid"),
        PrimaryKeyColumn::bytes("layer"),
        PrimaryKeyColumn::integer("global_seq", IntegerKeyType::U64),
    ]))
    .with_index(GrooveIndexSchema::new(
        "by_global_seq",
        ["global_seq", "table_name", "row_uuid", "layer"],
    ))
    .with_index(GrooveIndexSchema::new(
        "by_table_global_seq",
        ["table_name", "global_seq", "row_uuid", "layer"],
    ))
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
pub struct Policy;

impl Policy {
    /// Public/no policy.
    pub fn public() -> Option<Query> {
        None
    }

    /// Owner-only policy equivalent to `column == claim("sub")`.
    pub fn owner_only(table: impl Into<String>, column: impl Into<String>) -> Option<Query> {
        Some(Query::from(table).filter(eq(col(column), claim("sub"))))
    }

    /// Use an explicit policy shape.
    pub fn shape(query: Query) -> Option<Query> {
        Some(query)
    }
}

fn storage_enum(name: &str, variants: &[&str]) -> GrooveColumnType {
    GrooveColumnType::Enum(
        EnumSchema::new(name, variants.iter().copied()).expect("valid enum schema"),
    )
}

fn tx_kind_column() -> GrooveColumnType {
    storage_enum("jazz_tx_kind", &["mergeable", "exclusive"])
}

fn fate_column() -> GrooveColumnType {
    storage_enum("jazz_fate", &["pending", "accepted", "rejected"])
}

fn deletion_column() -> GrooveColumnType {
    storage_enum("jazz_deletion", &["deleted", "restored"])
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
            column("source_branch", GrooveColumnType::Uuid.nullable()),
            column("permission_subject", GrooveColumnType::Uuid.nullable()),
            // upstream-decided: written only by fate/state application.
            column("fate", fate_column()),
            // upstream-decided: written only by fate/state application.
            column("global_seq", GrooveColumnType::U64.nullable()),
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
    .with_index(GrooveIndexSchema::new("by_global_seq", ["global_seq"]))
}

fn canonical_schema_bytes(schema: &JazzSchema) -> Vec<u8> {
    let mut bytes = Vec::new();
    put_str(&mut bytes, "jazz-schema-v0");
    let mut tables = schema.tables.iter().collect::<Vec<_>>();
    tables.sort_by(|left, right| left.name.cmp(&right.name));
    put_u64(&mut bytes, tables.len() as u64);
    for table in tables {
        put_str(&mut bytes, &table.name);
        put_u64(&mut bytes, table.columns.len() as u64);
        for column in &table.columns {
            put_str(&mut bytes, &column.name);
            put_column_type(&mut bytes, &column.column_type);
            if let Some(kind) = column.large_value {
                put_large_value_kind(&mut bytes, kind);
            }
            put_merge_strategy(&mut bytes, table.merge_strategy(&column.name));
        }
        put_u64(&mut bytes, table.references.len() as u64);
        for (column, target) in &table.references {
            put_str(&mut bytes, column);
            put_str(&mut bytes, target);
        }
    }
    bytes
}

fn put_merge_strategy(bytes: &mut Vec<u8>, strategy: MergeStrategy) {
    bytes.push(match strategy {
        MergeStrategy::Lww => 0,
        MergeStrategy::Counter => 1,
    });
}

fn put_large_value_kind(bytes: &mut Vec<u8>, kind: LargeValueKind) {
    put_str(bytes, "jazz-large-value-v0");
    bytes.push(match kind {
        LargeValueKind::Text => 1,
        LargeValueKind::Blob => 2,
    });
}

fn put_column_type(bytes: &mut Vec<u8>, column_type: &GrooveColumnType) {
    match column_type {
        GrooveColumnType::U8 => bytes.push(1),
        GrooveColumnType::U16 => bytes.push(2),
        GrooveColumnType::U32 => bytes.push(3),
        GrooveColumnType::U64 => bytes.push(4),
        GrooveColumnType::F64 => bytes.push(5),
        GrooveColumnType::Bool => bytes.push(6),
        GrooveColumnType::String => bytes.push(7),
        GrooveColumnType::Bytes => bytes.push(8),
        GrooveColumnType::Uuid => bytes.push(9),
        GrooveColumnType::Enum(schema) => {
            bytes.push(10);
            put_str(bytes, &schema.name);
            put_u64(bytes, schema.variants.len() as u64);
            for variant in &schema.variants {
                put_str(bytes, variant);
            }
        }
        GrooveColumnType::Tuple(members) => {
            bytes.push(11);
            put_u64(bytes, members.len() as u64);
            for member in members {
                put_column_type(bytes, member);
            }
        }
        GrooveColumnType::Array(member) => {
            bytes.push(12);
            put_column_type(bytes, member);
        }
        GrooveColumnType::Nullable(member) => {
            bytes.push(13);
            put_column_type(bytes, member);
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
    fn lowers_history_tables_with_composite_primary_keys() {
        let schema = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);

        let groove = schema.lower_to_groove();
        assert!(groove.table("jazz_nodes").is_some());
        assert!(groove.table("jazz_schema_versions").is_some());
        assert!(groove.table("jazz_transactions").is_some());
        let table = groove.table("jazz_todos_history").unwrap();
        let primary_key = table.primary_key.as_ref().unwrap();

        assert_eq!(primary_key.columns.len(), 3);
        assert_eq!(primary_key.columns[0].column, "row_uuid");
        assert_eq!(primary_key.columns[1].column, "tx_time");
        assert_eq!(primary_key.columns[2].column, "tx_node_id");
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
        let schema_a = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let schema_b = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let schema_c = JazzSchema::new([TableSchema::new(
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
        let lww = JazzSchema::new([TableSchema::new(
            "counters",
            [ColumnSchema::new("count", ColumnType::U64)],
        )]);
        let counter = JazzSchema::new([TableSchema::new(
            "counters",
            [ColumnSchema::new("count", ColumnType::U64)],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)]);

        assert_ne!(lww.version_id(), counter.version_id());
    }

    #[test]
    #[should_panic(expected = "counter merge strategy requires a non-nullable integer column")]
    fn counter_merge_strategy_rejects_string_columns() {
        JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_column_merge_strategy("title", MergeStrategy::Counter)]);
    }

    #[test]
    #[should_panic(expected = "merge strategy declared for unknown column todos.missing")]
    fn merge_strategy_rejects_unknown_user_column() {
        JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )
        .with_column_merge_strategy("missing", MergeStrategy::Lww)]);
    }

    #[test]
    #[should_panic(expected = "counter merge strategy requires a non-nullable integer column")]
    fn counter_merge_strategy_rejects_nullable_integer_columns() {
        JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("count", ColumnType::U64.nullable())],
        )
        .with_column_merge_strategy("count", MergeStrategy::Counter)]);
    }

    #[test]
    fn large_value_columns_lower_to_opaque_bytes() {
        let schema = JazzSchema::new([TableSchema::new(
            "notes",
            [ColumnSchema::text("body"), ColumnSchema::blob("attachment")],
        )]);
        let groove = schema.lower_to_groove();
        let history = groove.table("jazz_notes_history").unwrap();

        assert_eq!(
            history
                .columns
                .iter()
                .find(|column| column.name == "user_body")
                .unwrap()
                .column_type,
            ColumnType::Bytes.nullable()
        );
        assert_eq!(
            history
                .columns
                .iter()
                .find(|column| column.name == "user_attachment")
                .unwrap()
                .column_type,
            ColumnType::Bytes.nullable()
        );
    }

    #[test]
    fn large_value_kind_changes_schema_identity() {
        let plain_bytes = JazzSchema::new([TableSchema::new(
            "notes",
            [ColumnSchema::new("body", ColumnType::Bytes)],
        )]);
        let text = JazzSchema::new([TableSchema::new("notes", [ColumnSchema::text("body")])]);
        let blob = JazzSchema::new([TableSchema::new("notes", [ColumnSchema::blob("body")])]);

        assert_ne!(plain_bytes.version_id(), text.version_id());
        assert_ne!(text.version_id(), blob.version_id());
    }

    #[test]
    #[should_panic(expected = "counter merge strategy cannot be used with a large-value column")]
    fn counter_merge_strategy_rejects_large_value_columns() {
        JazzSchema::new([TableSchema::new("notes", [ColumnSchema::text("body")])
            .with_column_merge_strategy("body", MergeStrategy::Counter)]);
    }

    #[test]
    #[should_panic(expected = "read policy table must match")]
    fn read_policy_must_name_attached_table() {
        JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_read_policy(Policy::owner_only("other", "owner"))]);
    }

    #[test]
    #[should_panic(expected = "write policy table must match")]
    fn write_policy_must_name_attached_table() {
        JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("owner", ColumnType::Uuid)],
        )
        .with_write_policy(Policy::owner_only("other", "owner"))]);
    }

    #[test]
    #[should_panic(expected = "valid read policy shape")]
    fn read_policy_validates_against_complete_schema() {
        JazzSchema::new([TableSchema::new(
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
            vec!["table_name", "row_uuid", "layer", "global_seq"]
        );

        let index = table
            .indices
            .iter()
            .find(|index| index.name == "by_global_seq")
            .unwrap();
        assert_eq!(
            index.columns,
            vec!["global_seq", "table_name", "row_uuid", "layer"]
        );
        let table_index = table
            .indices
            .iter()
            .find(|index| index.name == "by_table_global_seq")
            .unwrap();
        assert_eq!(
            table_index.columns,
            vec!["table_name", "global_seq", "row_uuid", "layer"]
        );
    }

    #[test]
    fn storage_lowering_declares_system_columns_by_shape() {
        let schema = JazzSchema::new([TableSchema::new(
            "todos",
            [ColumnSchema::new("title", ColumnType::String)],
        )]);
        let tables = schema.storage_tables();
        let transactions = tables
            .iter()
            .find(|table| table.name == "jazz_transactions")
            .unwrap();
        let history = tables
            .iter()
            .find(|table| table.name == "jazz_todos_history")
            .unwrap();
        let register = tables
            .iter()
            .find(|table| table.name == "jazz_todos_register")
            .unwrap();
        let global_current = tables
            .iter()
            .find(|table| table.name == "jazz_todos_global_current")
            .unwrap();
        let register_global_current = tables
            .iter()
            .find(|table| table.name == "jazz_todos_register_global_current")
            .unwrap();

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
            vec!["row_uuid"]
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
            vec!["row_uuid"]
        );
    }
}
