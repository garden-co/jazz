use crate::{Error, Result};
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn ensure_user(conn: &Connection, user_id: &str) -> Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_user (user_id) VALUES (?)",
        params![user_id],
    )?;
    user_num(conn, user_id)
}

pub(crate) fn user_num(conn: &Connection, user_id: &str) -> Result<i64> {
    conn.query_row(
        "SELECT user_num FROM jazz_user WHERE user_id = ?",
        params![user_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| Error::new(format!("unknown user {user_id}")))
}

pub(crate) fn user_id_expr(alias: &str, column: &str) -> String {
    format!("(SELECT user_id FROM jazz_user WHERE user_num = {alias}.{column})")
}
