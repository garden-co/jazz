//! Cloudflare Durable Object SQLite's limits, applied to a native
//! connection so tests find SQL that would fail there. Measured in workerd
//! 2026-07-30 (`ctx.storage.sql`); production enforces the same values.
//!
//! What limits cannot emulate, and so stays out of this check: `exec` only
//! (no SAVEPOINT/BEGIN; nested `transactionSync` instead), no PRAGMAs
//! beyond a read-mostly allowlist, no TEMP objects, and integers read back
//! as JS numbers (exact only up to 2^53).

use rusqlite::Connection;
use rusqlite::limits::Limit;

const LIMITS: [(Limit, i32); 8] = [
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
    (Limit::SQLITE_LIMIT_LIKE_PATTERN_LENGTH, 50),
    (Limit::SQLITE_LIMIT_FUNCTION_ARG, 127),
];

pub(crate) fn apply(conn: &Connection) {
    for (limit, value) in LIMITS {
        conn.set_limit(limit, value).expect("set SQLite limit");
    }
}
