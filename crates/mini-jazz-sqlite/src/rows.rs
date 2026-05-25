use crate::Result;
use rusqlite::{params, Connection, OptionalExtension};

pub(crate) struct NewTodo<'a> {
    pub(crate) id: &'a str,
    pub(crate) title: &'a str,
    pub(crate) done: bool,
    pub(crate) project_id: &'a str,
    pub(crate) now: i64,
    pub(crate) principal: &'a str,
}

pub(crate) fn ensure_row_id(conn: &Connection, table: &str, row_id: &str) -> Result<i64> {
    conn.execute(
        "INSERT OR IGNORE INTO jazz_row_id (table_name, row_id) VALUES (?, ?)",
        params![table, row_id],
    )?;
    Ok(conn.query_row(
        "SELECT row_num FROM jazz_row_id WHERE row_id = ?",
        params![row_id],
        |row| row.get(0),
    )?)
}

pub(crate) fn row_num(conn: &Connection, row_id: &str) -> Result<i64> {
    conn.query_row(
        "SELECT row_num FROM jazz_row_id WHERE row_id = ?",
        params![row_id],
        |row| row.get(0),
    )
    .optional()?
    .ok_or_else(|| crate::Error::new(format!("unknown row {row_id}")))
}

pub(crate) fn insert_project(
    conn: &Connection,
    tx_num: i64,
    id: &str,
    title: &str,
    now: i64,
    principal: &str,
) -> Result<()> {
    let row_num = ensure_row_id(conn, "projects", id)?;
    conn.execute(
        "INSERT OR IGNORE INTO projects__schema_v1_history
         (row_num, tx_num, op, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
        params![row_num, tx_num, title, now, now, principal, principal],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO projects__schema_v1_current
         (row_num, visible_tx_num, is_deleted, title, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 0, ?, ?, ?, ?, ?)",
        params![row_num, tx_num, title, now, now, principal, principal],
    )?;
    Ok(())
}

pub(crate) fn insert_todo(conn: &Connection, tx_num: i64, todo: NewTodo<'_>) -> Result<()> {
    let row_num = ensure_row_id(conn, "todos", todo.id)?;
    let project_row_num = ensure_row_id(conn, "projects", todo.project_id)?;
    conn.execute(
        "INSERT OR IGNORE INTO todos__schema_v1_history
         (row_num, tx_num, op, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 1, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row_num,
            tx_num,
            todo.title,
            i64::from(todo.done),
            project_row_num,
            todo.now,
            todo.now,
            todo.principal,
            todo.principal
        ],
    )?;
    conn.execute(
        "INSERT OR REPLACE INTO todos__schema_v1_current
         (row_num, visible_tx_num, is_deleted, title, done, project_row_num, j_created_at, j_updated_at, j_created_by, j_updated_by)
         VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?)",
        params![
            row_num,
            tx_num,
            todo.title,
            i64::from(todo.done),
            project_row_num,
            todo.now,
            todo.now,
            todo.principal,
            todo.principal
        ],
    )?;
    Ok(())
}
