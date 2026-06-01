use crate::Result;
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) fn ensure_row_id(conn: &Connection, _table: &str, row_id: &str) -> Result<i64> {
    Ok(ensure_row_id_with_status(conn, row_id)?.0)
}

pub(crate) fn ensure_row_id_with_status(conn: &Connection, row_id: &str) -> Result<(i64, bool)> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_row_id (row_id) VALUES (?)",
        params![row_id],
    )?;
    let created = conn.changes() > 0;
    Ok(conn.query_row(
        "SELECT row_num FROM jazz_row_id WHERE row_id = ?",
        params![row_id],
        |row| row.get(0),
    )?)
    .map(|row_num| (row_num, created))
}

pub(crate) fn existing_row_num(conn: &Connection, row_id: &str) -> Result<Option<i64>> {
    conn.query_row(
        "SELECT row_num FROM jazz_row_id WHERE row_id = ?",
        params![row_id],
        |row| row.get(0),
    )
    .optional()
    .map_err(Into::into)
}

pub(crate) fn row_num(conn: &Connection, row_id: &str) -> Result<i64> {
    existing_row_num(conn, row_id)?
        .ok_or_else(|| crate::Error::new(format!("unknown row {row_id}")))
}

pub(crate) fn public_row_id(conn: &Connection, row_num: i64) -> Result<String> {
    conn.query_row(
        "SELECT row_id FROM jazz_row_id WHERE row_num = ?",
        params![row_num],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| crate::Error::new(format!("unknown physical row {row_num}")))
}
