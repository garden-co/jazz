//! Cloudflare Durable Object SQLite's limits, applied to a native
//! connection so tests find SQL that would fail there. Measured in workerd
//! 2026-07-30 (`ctx.storage.sql`); production enforces the same values.
//!
//! What limits cannot emulate, and so stays out of this check: `exec` only
//! (no SAVEPOINT/BEGIN; nested `transactionSync` instead), no PRAGMAs
//! beyond a read-mostly allowlist, no TEMP objects, and integers read back
//! as JS numbers (exact only up to 2^53).

use crate::options::{Error, ErrorCode};
use crate::values::Schema;
use rusqlite::Connection;
use rusqlite::limits::Limit;

const LIMITS: [(Limit, i32); 12] = [
    // ?NNN placeholders, and their count.
    (Limit::SQLITE_LIMIT_VARIABLE_NUMBER, 100),
    // Statement text, in bytes.
    (Limit::SQLITE_LIMIT_SQL_LENGTH, 100_000),
    // A string, blob or row.
    (Limit::SQLITE_LIMIT_LENGTH, 2 * 1024 * 1024),
    // A left-deep AND/OR chain of n terms is n deep.
    (Limit::SQLITE_LIMIT_EXPR_DEPTH, 100),
    (Limit::SQLITE_LIMIT_COLUMN, 100),
    (Limit::SQLITE_LIMIT_COMPOUND_SELECT, 5),
    // Workerd's security profile also bounds compiled VM allocation, not
    // just SQL text/depth. SQLite reports an exceeded budget as SQLITE_NOMEM.
    (Limit::SQLITE_LIMIT_VDBE_OP, 25_000),
    (Limit::SQLITE_LIMIT_TRIGGER_DEPTH, 10),
    (Limit::SQLITE_LIMIT_ATTACHED, 0),
    (Limit::SQLITE_LIMIT_WORKER_THREADS, 0),
    (Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50),
    // workerd 2026-07-30 accepts 127, but the documented platform contract
    // is 32. Exercise the conservative limit until production is verified.
    (Limit::SQLITE_LIMIT_FUNCTION_ARG, 32),
];

/// Check the physical union, including columns retained from older schemas,
/// before changing it or issuing DDL. Current rows have eight structural
/// columns in addition to the stored application/provenance columns.
pub(crate) fn validate_schema(schema: &Schema, existing: Option<&Schema>) -> Result<(), Error> {
    for table in schema.tables.values() {
        let mut columns = std::collections::BTreeSet::new();
        for column in table.stored() {
            columns.insert(column.stored.as_str());
        }
        if let Some(physical) = existing.and_then(|schema| schema.by_stored(&table.stored)) {
            for column in physical.stored() {
                columns.insert(column.stored.as_str());
            }
        }
        let count = 8 + columns.len();
        if count > 100 {
            return Err(Error::new(
                ErrorCode::Schema,
                format!(
                    "Storing table {} with {count} physical columns is not supported in Durable Objects yet (limit 100, including engine and retained migration columns)",
                    table.name,
                ),
            ));
        }
    }
    Ok(())
}

pub(crate) fn apply(conn: &Connection) {
    for (limit, value) in LIMITS {
        conn.set_limit(limit, value).expect("set SQLite limit");
    }
}
