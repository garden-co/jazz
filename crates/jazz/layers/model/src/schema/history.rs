//! Virtual history relation resolution. These descriptors are query-only: they
//! are not application tables, storage declarations, or permission grants.

use super::{ColumnSchema, GrooveColumnType, RuntimeSchema, TableSchema};
use crate::query::QueryError;

/// Reserved prefix for query-visible history relations.
pub const HISTORY_PREFIX: &str = "history.";
/// Portable transaction identity of a settled content version.
pub const TRANSACTION_ID_COLUMN: &str = "$transactionId";
/// Accepted sequence of a settled content version.
pub const SEQUENCE_COLUMN: &str = "$seq";

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

impl HistoryRelationSchema<'_> {
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

impl RuntimeSchema {
    /// Resolve `history.<table>` without modifying the application catalogue.
    ///
    /// `$transactionId` uses the existing logical transaction tuple `(u64,
    /// UUID)`; bindings must encode the full originating node UUID, never a
    /// node-local alias. Both metadata columns are non-null because the relation
    /// will expose only accepted, settled versions.
    ///
    /// Schema admission reserves `history.` table names and `$` column names,
    /// so virtual relations cannot shadow application declarations. This is a
    /// descriptor lookup, not an execution capability check.
    pub fn resolve_history_relation(
        &self,
        name: &str,
    ) -> Result<HistoryRelationSchema<'_>, QueryError> {
        let table = name
            .strip_prefix(HISTORY_PREFIX)
            .filter(|name| !name.is_empty())
            .and_then(|table_name| self.tables.iter().find(|table| table.name == table_name))
            .ok_or_else(|| QueryError::UnknownTable(name.to_owned()))?;
        let mut columns = table.columns.clone();
        columns.extend([
            ColumnSchema::new(TRANSACTION_ID_COLUMN, super::tx_id_column()),
            ColumnSchema::new(SEQUENCE_COLUMN, GrooveColumnType::U64),
        ]);
        Ok(HistoryRelationSchema {
            name: name.to_owned(),
            source_table: table,
            columns,
        })
    }
}
