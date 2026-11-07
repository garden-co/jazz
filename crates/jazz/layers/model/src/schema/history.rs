//! Virtual history relation descriptors. These descriptors are query-only: they
//! are not application tables, storage declarations, or permission grants.

use super::magic::MagicColumn;
use super::{ColumnSchema, TableSchema};

/// Reserved prefix for query-visible history relations.
pub const HISTORY_PREFIX: &str = "history.";
/// Whether a name occupies the reserved history namespace.
/// This does not imply that its source table exists or the relation is valid.
pub fn is_history_table(name: &str) -> bool {
    history_source_table(name).is_some()
}

/// Extract the source name from the history namespace; catalogue resolution
/// remains responsible for rejecting empty, nested, or unknown source names.
pub fn history_source_table(name: &str) -> Option<&str> {
    name.strip_prefix(HISTORY_PREFIX)
}

/// Portable transaction identity of a settled content version.
pub const TRANSACTION_ID_COLUMN: &str = "$transactionId";
/// Acceptance HLC of a settled content version.
pub const ACCEPTED_AT_COLUMN: &str = "$acceptedAt";

/// Metadata supplied for each accepted content version.
#[derive(Clone, Copy, Debug)]
pub enum HistoryMetadataColumn {
    /// Portable content transaction identity.
    TransactionId,
    /// Acceptance HLC, retaining physical and logical components.
    AcceptedAt,
}

impl HistoryMetadataColumn {
    /// Canonical metadata column order, shared by descriptors and row encoding.
    pub const ALL: [Self; 2] = [Self::TransactionId, Self::AcceptedAt];

    /// Query-visible column name.
    pub fn name(self) -> &'static str {
        match self {
            Self::TransactionId => TRANSACTION_ID_COLUMN,
            Self::AcceptedAt => ACCEPTED_AT_COLUMN,
        }
    }

    fn schema(self) -> ColumnSchema {
        let column = match self {
            Self::TransactionId => MagicColumn::TransactionId,
            Self::AcceptedAt => MagicColumn::AcceptedAt,
        };
        ColumnSchema::new(column.name(), column.value_type().clone())
    }

    /// Whether a name denotes history metadata.
    pub fn contains(name: &str) -> bool {
        Self::ALL.iter().any(|column| column.name() == name)
    }
}

/// Read-only, derived schema for a history query source.
///
/// The underlying table remains the authority for branch scope, schema lenses,
/// and current-row read policies. Its indexes and write policies are deliberately
/// not copied into a new writable application table. This descriptor alone does
/// not authorize or enable query execution.
#[derive(Clone, Debug)]
pub struct HistoryRelationSchema<'a> {
    name: String,
    source_table: &'a TableSchema,
    columns: Vec<ColumnSchema>,
}

impl<'a> HistoryRelationSchema<'a> {
    /// Derive a query-only history descriptor from an already resolved table.
    /// Name lookup and execution capability checks belong to query resolution.
    ///
    /// `$transactionId` uses the logical `(u64, UUID)` transaction tuple, never
    /// a node-local alias. Both metadata columns are non-null because history
    /// exposes only accepted, settled versions.
    pub fn new(source_table: &'a TableSchema) -> Self {
        let mut columns = source_table.columns.clone();
        columns.extend(HistoryMetadataColumn::ALL.map(HistoryMetadataColumn::schema));
        Self {
            name: format!("{HISTORY_PREFIX}{}", source_table.name),
            source_table,
            columns,
        }
    }

    /// Query-only table shape, retaining branch identity for version witnesses.
    pub fn table_schema(&self) -> TableSchema {
        let mut table = TableSchema::new(self.name.clone(), self.columns.iter().cloned());
        table.branch_by = self.source_table.branch_by.clone();
        table
    }

    /// Fully qualified virtual relation name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Application table whose current-row policies gate history access.
    pub fn source_table(&self) -> &TableSchema {
        self.source_table
    }

    /// Application and history metadata columns. As on current tables, `id`
    /// remains an implicit UUID column rather than an application declaration.
    pub fn columns(&self) -> &[ColumnSchema] {
        &self.columns
    }
}
